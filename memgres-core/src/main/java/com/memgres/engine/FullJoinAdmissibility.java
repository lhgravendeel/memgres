package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Whether PostgreSQL would refuse a FULL JOIN's condition, reproduced in the order PostgreSQL
 * decides it.
 *
 * <p>PostgreSQL has no nested-loop plan for a full join: it must merge or hash the two sides, and
 * both need an equality between one side's value and the other's, so a condition offering neither
 * is {@code 0A000 FULL JOIN is only supported with merge-joinable or hash-joinable join
 * conditions}. An application that runs against both engines has to see the same refusal, which is
 * why memgres reproduces a limitation it does not itself have.
 *
 * <p>The trap is that PostgreSQL does not read the condition as written. Two passes run first:
 *
 * <ul>
 *   <li><b>Normalisation.</b> {@code eval_const_expressions} folds constants, eliminates NOT by
 *       replacing an operator with its negator ({@code NOT (a &lt;&gt; b)} is {@code a = b}) and by
 *       de Morgan, simplifies {@code x = true} to {@code x}, drops a no-op cast, and picks the arm
 *       of a CASE whose condition is constant; {@code prepqual} then factors an OR whose arms share
 *       a clause, so {@code (A AND B) OR (A AND C)} becomes {@code A AND (B OR C)}. Only what is
 *       left of the condition after all of that is asked to be mergejoinable. A single-element
 *       {@code IN} never reaches the planner as one at all: the parser writes it as an equality.
 *   <li><b>Outer-join reduction.</b> {@code reduce_outer_joins} downgrades a full join to a left,
 *       right or inner one when a qual above it rejects the rows one side was padded with, and a
 *       downgraded join is never asked the question. The qual may come from WHERE, from a HAVING
 *       clause with no aggregate in it, from the ON condition of an inner join above, or from an
 *       enclosing query once the subquery, WITH query or view holding the join has been pulled up.
 *       Only a <em>strict</em> qual counts: {@code WHERE a.x IS NOT NULL} downgrades the join,
 *       {@code WHERE a.x IS NULL} and {@code WHERE coalesce(a.x,0) &gt;= 0} do not.
 * </ul>
 *
 * <p><b>The check errs towards accepting.</b> Refusing a query PostgreSQL answers is worse than
 * answering one PostgreSQL refuses — the rule exists only so the two engines cannot be told apart,
 * and memgres can compute any full join it is asked for. So a clause whose shape this cannot read,
 * a qual naming something it cannot place, and every position it cannot follow a qual through are
 * all reasons to accept, and the refusal is raised only when every remaining clause is one of the
 * shapes measured against PostgreSQL as not mergejoinable.
 */
final class FullJoinAdmissibility {

    private final FromResolver from;
    private final AstExecutor executor;

    FullJoinAdmissibility(FromResolver from) {
        this.from = from;
        this.executor = from.executor;
    }

    /** Stands for a clause that folded to true, and so is not a clause at all. */
    private static final Expression TRUE = new Literal(Literal.LiteralType.BOOLEAN, "true");
    /** Stands for a clause that folded to false or null: the join then has nothing to plan. */
    private static final Expression NOT_TRUE = new Literal(Literal.LiteralType.BOOLEAN, "false");

    private static final int SIDE_NONE = 0;
    private static final int SIDE_LEFT = 1;
    private static final int SIDE_RIGHT = 2;
    private static final int SIDE_BOTH = 3;

    /**
     * Functions with a side effect or an answer of their own each time. Judging a condition must
     * not draw a sequence value or take a lock, so a clause naming one is left unfolded.
     */
    private static final Set<String> VOLATILE_FUNCTIONS = new HashSet<>(Arrays.asList(
            "nextval", "setval", "currval", "lastval", "random", "random_normal", "gen_random_uuid",
            "uuid_generate_v1", "uuid_generate_v4", "gen_random_bytes", "clock_timestamp",
            "statement_timestamp", "timeofday", "txid_current", "pg_current_xact_id", "pg_sleep",
            "pg_sleep_for", "pg_sleep_until", "nextval_ref"));

    /** Functions that answer with a value of their own when an argument is null. */
    private static final Set<String> NON_STRICT = new HashSet<>(Arrays.asList(
            "coalesce", "greatest", "least", "nullif", "concat", "concat_ws", "nvl", "ifnull",
            "num_nonnulls", "num_nulls", "format", "json_build_object", "jsonb_build_object",
            "json_build_array", "jsonb_build_array", "to_json", "to_jsonb", "row_to_json"));

    // ---- Entry point ----

    void reject(Expression on,
                List<RowContext.TableBinding> leftShape,
                List<RowContext.TableBinding> rightShape) {
        // USING and NATURAL join on equality by construction, and a join whose sides could not be
        // described is not one this check can judge.
        if (on == null) return;
        if (leftShape == null || leftShape.isEmpty() || rightShape == null || rightShape.isEmpty()) return;
        // A view's body is analysed, not planned, when the view is defined: PostgreSQL stores the
        // definition and refuses only the reads of it.
        if (from.suppressFullJoinCheck) return;
        // A name that reads from both sides is reported as ambiguous when the condition is
        // resolved, which PostgreSQL does before it plans anything.
        if (hasAmbiguousBareName(on, leftShape, rightShape)) return;
        if (downgradedFromAbove(leftShape, rightShape)) return;

        Expression normalised = normalize(on, 0);
        // A condition that is constantly false or null leaves PostgreSQL a plan with no join in it.
        if (normalised == NOT_TRUE) return;

        List<Expression> conjuncts = new ArrayList<Expression>();
        flattenAnd(normalised, conjuncts);
        int judged = 0;
        for (Expression c : conjuncts) {
            if (c == TRUE) continue;
            if (isCrossSideEquality(c, leftShape, rightShape)) return;
            if (!recognisablyUnmergeable(c)) return;
            judged++;
        }
        if (judged == 0) return;
        throw PgErrors.notImplemented(
                "FULL JOIN is only supported with merge-joinable or hash-joinable join conditions");
    }

    // ---- Normalisation ----

    /** How deep the rewrites may nest before the condition is taken as it stands. */
    private static final int MAX_DEPTH = 24;

    private Expression normalize(Expression e, int depth) {
        if (e == null) return TRUE;
        if (depth > MAX_DEPTH) return e;
        e = strip(e);
        if (e == TRUE || e == NOT_TRUE) return e;
        if (isConstant(e)) {
            Object value;
            try {
                value = executor.evalExpr(e, null);
            } catch (RuntimeException ex) {
                return e;
            }
            return Boolean.TRUE.equals(value) ? TRUE : NOT_TRUE;
        }
        if (e instanceof UnaryExpr && ((UnaryExpr) e).op() == UnaryExpr.UnaryOp.NOT) {
            Expression pushed = negate(((UnaryExpr) e).operand());
            return pushed == null ? e : normalize(pushed, depth + 1);
        }
        if (e instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) e;
            if (bin.op() == BinaryExpr.BinOp.AND) return normalizeAnd(bin, depth);
            if (bin.op() == BinaryExpr.BinOp.OR) return normalizeOr(bin, depth);
            if (bin.op() == BinaryExpr.BinOp.EQUAL || bin.op() == BinaryExpr.BinOp.NOT_EQUAL) {
                Expression simplified = simplifyBooleanEquality(bin);
                if (simplified != null) return normalize(simplified, depth + 1);
            }
            return bin;
        }
        if (e instanceof InExpr) {
            Expression expanded = expandIn((InExpr) e);
            return expanded == null ? e : normalize(expanded, depth + 1);
        }
        if (e instanceof CaseExpr) {
            Expression folded = foldCase((CaseExpr) e);
            return folded == null ? e : normalize(folded, depth + 1);
        }
        return e;
    }

    private Expression normalizeAnd(Expression e, int depth) {
        List<Expression> parts = new ArrayList<Expression>();
        flattenAnd(e, parts);
        List<Expression> live = new ArrayList<Expression>();
        for (Expression p : parts) {
            Expression n = normalize(p, depth + 1);
            if (n == TRUE) continue;
            if (n == NOT_TRUE) return NOT_TRUE;
            live.add(n);
        }
        if (live.isEmpty()) return TRUE;
        return andOf(live);
    }

    private Expression normalizeOr(Expression e, int depth) {
        List<Expression> parts = new ArrayList<Expression>();
        flattenOr(e, parts);
        List<Expression> live = new ArrayList<Expression>();
        for (Expression p : parts) {
            Expression n = normalize(p, depth + 1);
            // A true alternative makes the whole clause true; anything else — false, null — can
            // never be the alternative that holds, and drops out of the condition.
            if (n == TRUE) return TRUE;
            if (n == NOT_TRUE) continue;
            live.add(n);
        }
        if (live.isEmpty()) return NOT_TRUE;
        if (live.size() == 1) return live.get(0);
        Expression factored = factorDuplicateOrs(live);
        if (factored != null) return normalize(factored, depth + 1);
        return orOf(live);
    }

    /**
     * PostgreSQL's {@code process_duplicate_ors}: a clause every arm of an OR carries can be
     * lifted out of it, and an arm left with nothing is implied by the clauses lifted, which
     * removes the OR altogether. {@code A OR (A AND B)} is {@code A}; {@code (A AND B) OR (A AND
     * C)} is {@code A AND (B OR C)}.
     */
    private Expression factorDuplicateOrs(List<Expression> arms) {
        List<List<Expression>> armClauses = new ArrayList<List<Expression>>();
        for (Expression arm : arms) {
            List<Expression> cs = new ArrayList<Expression>();
            flattenAnd(arm, cs);
            armClauses.add(cs);
        }
        List<Expression> common = new ArrayList<Expression>();
        for (Expression candidate : armClauses.get(0)) {
            if (contains(common, candidate)) continue;
            boolean inEvery = true;
            for (int i = 1; i < armClauses.size(); i++) {
                if (!contains(armClauses.get(i), candidate)) {
                    inEvery = false;
                    break;
                }
            }
            if (inEvery) common.add(candidate);
        }
        if (common.isEmpty()) return null;
        List<Expression> remainders = new ArrayList<Expression>();
        boolean armFullyCovered = false;
        for (List<Expression> cs : armClauses) {
            List<Expression> rest = new ArrayList<Expression>();
            for (Expression c : cs) {
                if (!contains(common, c)) rest.add(c);
            }
            if (rest.isEmpty()) {
                armFullyCovered = true;
                break;
            }
            remainders.add(andOf(rest));
        }
        List<Expression> result = new ArrayList<Expression>(common);
        if (!armFullyCovered) result.add(orOf(remainders));
        return andOf(result);
    }

    private static boolean contains(List<Expression> list, Expression e) {
        for (Expression c : list) {
            if (c == e || c.equals(e)) return true;
        }
        return false;
    }

    /** {@code x = true} is {@code x} and {@code x = false} is {@code NOT x}, as PostgreSQL folds them. */
    private Expression simplifyBooleanEquality(BinaryExpr bin) {
        Boolean left = booleanConstant(bin.left());
        Boolean right = booleanConstant(bin.right());
        Boolean constant;
        Expression other;
        if (left != null && right == null) {
            constant = left;
            other = bin.right();
        } else if (right != null && left == null) {
            constant = right;
            other = bin.left();
        } else {
            return null;
        }
        if (!isBooleanValued(other)) return null;
        boolean keep = (bin.op() == BinaryExpr.BinOp.EQUAL) == constant.booleanValue();
        return keep ? other : new UnaryExpr(UnaryExpr.UnaryOp.NOT, other);
    }

    /**
     * {@code a IN (b)} is written by the parser as {@code a = b}, and a list whose members are not
     * all constants as an OR of equalities; a list that is all constants becomes an array
     * comparison, which is never a join clause.
     */
    private Expression expandIn(InExpr in) {
        if (in.negated() || in.fromAny()) return null;
        List<Expression> values = in.values();
        if (values == null || values.isEmpty()) return null;
        boolean allConstant = true;
        for (Expression v : values) {
            if (v instanceof SubqueryExpr || v instanceof ArraySubqueryExpr) return null;
            if (mentionsRelation(v)) allConstant = false;
        }
        if (allConstant) return null;
        List<Expression> equalities = new ArrayList<Expression>();
        for (Expression v : values) {
            equalities.add(new BinaryExpr(in.expr(), BinaryExpr.BinOp.EQUAL, v));
        }
        return equalities.size() == 1 ? equalities.get(0) : orOf(equalities);
    }

    /** A CASE whose conditions are constant collapses to the arm that holds. */
    private Expression foldCase(CaseExpr c) {
        if (c.operand() != null) return null;
        if (c.whenClauses() == null) return null;
        for (CaseExpr.WhenClause w : c.whenClauses()) {
            Boolean condition = booleanConstant(w.condition());
            if (condition == null) return null;
            if (condition.booleanValue()) return w.result();
        }
        return c.elseExpr() != null ? c.elseExpr() : Literal.ofNull();
    }

    /**
     * The condition with the NOT pushed into it, or null when it cannot be pushed in — de Morgan
     * for AND and OR, the negator of an operator, the other half of a null or boolean test.
     */
    private Expression negate(Expression e) {
        e = strip(e);
        if (e instanceof UnaryExpr && ((UnaryExpr) e).op() == UnaryExpr.UnaryOp.NOT) {
            return ((UnaryExpr) e).operand();
        }
        if (e instanceof Literal && ((Literal) e).literalType() == Literal.LiteralType.BOOLEAN) {
            return Literal.ofBoolean(!"true".equalsIgnoreCase(((Literal) e).value()));
        }
        if (e instanceof IsNullExpr) {
            IsNullExpr n = (IsNullExpr) e;
            return new IsNullExpr(n.expr(), !n.negated());
        }
        if (e instanceof IsBooleanExpr) {
            IsBooleanExpr t = (IsBooleanExpr) e;
            IsBooleanExpr.BooleanTest opposite = oppositeTest(t.test());
            return opposite == null ? null : new IsBooleanExpr(t.expr(), opposite);
        }
        if (e instanceof BinaryExpr) {
            BinaryExpr b = (BinaryExpr) e;
            if (b.op() == BinaryExpr.BinOp.AND || b.op() == BinaryExpr.BinOp.OR) {
                Expression l = negate(b.left());
                if (l == null) l = new UnaryExpr(UnaryExpr.UnaryOp.NOT, b.left());
                Expression r = negate(b.right());
                if (r == null) r = new UnaryExpr(UnaryExpr.UnaryOp.NOT, b.right());
                return new BinaryExpr(l,
                        b.op() == BinaryExpr.BinOp.AND ? BinaryExpr.BinOp.OR : BinaryExpr.BinOp.AND, r);
            }
            BinaryExpr.BinOp negator = negatorOf(b.op());
            if (negator != null) return new BinaryExpr(b.left(), negator, b.right());
        }
        return null;
    }

    private static BinaryExpr.BinOp negatorOf(BinaryExpr.BinOp op) {
        switch (op) {
            case EQUAL: return BinaryExpr.BinOp.NOT_EQUAL;
            case NOT_EQUAL: return BinaryExpr.BinOp.EQUAL;
            case LESS_THAN: return BinaryExpr.BinOp.GREATER_EQUAL;
            case GREATER_EQUAL: return BinaryExpr.BinOp.LESS_THAN;
            case GREATER_THAN: return BinaryExpr.BinOp.LESS_EQUAL;
            case LESS_EQUAL: return BinaryExpr.BinOp.GREATER_THAN;
            case IS_DISTINCT_FROM: return BinaryExpr.BinOp.IS_NOT_DISTINCT_FROM;
            case IS_NOT_DISTINCT_FROM: return BinaryExpr.BinOp.IS_DISTINCT_FROM;
            default: return null;
        }
    }

    private static IsBooleanExpr.BooleanTest oppositeTest(IsBooleanExpr.BooleanTest test) {
        switch (test) {
            case IS_TRUE: return IsBooleanExpr.BooleanTest.IS_NOT_TRUE;
            case IS_NOT_TRUE: return IsBooleanExpr.BooleanTest.IS_TRUE;
            case IS_FALSE: return IsBooleanExpr.BooleanTest.IS_NOT_FALSE;
            case IS_NOT_FALSE: return IsBooleanExpr.BooleanTest.IS_FALSE;
            case IS_UNKNOWN: return IsBooleanExpr.BooleanTest.IS_NOT_UNKNOWN;
            case IS_NOT_UNKNOWN: return IsBooleanExpr.BooleanTest.IS_UNKNOWN;
            default: return null;
        }
    }

    /** A qualified operator spelling and a cast that changes nothing both disappear before planning. */
    private Expression strip(Expression e) {
        while (true) {
            if (e instanceof QualifiedOperatorExpr) {
                e = ((QualifiedOperatorExpr) e).inner();
                continue;
            }
            if (e instanceof CastExpr) {
                CastExpr cast = (CastExpr) e;
                if (isBooleanTypeName(cast.typeName()) && isBooleanValued(cast.expr())) {
                    e = cast.expr();
                    continue;
                }
            }
            return e;
        }
    }

    private static boolean isBooleanTypeName(String name) {
        if (name == null) return false;
        String n = name.trim().toLowerCase();
        int dot = n.lastIndexOf('.');
        if (dot >= 0) n = n.substring(dot + 1);
        return "bool".equals(n) || "boolean".equals(n);
    }

    /** Whether the expression is one of the shapes that can only answer with a boolean. */
    private boolean isBooleanValued(Expression e) {
        if (e instanceof QualifiedOperatorExpr) return isBooleanValued(((QualifiedOperatorExpr) e).inner());
        if (e instanceof CastExpr) {
            CastExpr cast = (CastExpr) e;
            return isBooleanTypeName(cast.typeName()) && isBooleanValued(cast.expr());
        }
        if (e instanceof Literal) return ((Literal) e).literalType() == Literal.LiteralType.BOOLEAN;
        if (e instanceof IsNullExpr || e instanceof IsBooleanExpr || e instanceof BetweenExpr
                || e instanceof LikeExpr || e instanceof ExistsExpr || e instanceof InExpr
                || e instanceof AnyAllArrayExpr || e instanceof AnyAllExpr) {
            return true;
        }
        if (e instanceof UnaryExpr) return ((UnaryExpr) e).op() == UnaryExpr.UnaryOp.NOT;
        if (e instanceof BinaryExpr) {
            switch (((BinaryExpr) e).op()) {
                case EQUAL: case NOT_EQUAL: case LESS_THAN: case GREATER_THAN:
                case LESS_EQUAL: case GREATER_EQUAL: case AND: case OR:
                case LIKE: case ILIKE: case SIMILAR_TO:
                case IS_DISTINCT_FROM: case IS_NOT_DISTINCT_FROM:
                case REGEX_MATCH: case REGEX_IMATCH:
                case NOT_REGEX_MATCH: case NOT_REGEX_IMATCH:
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }

    /** The expression's value when it stands on its own and answers with a boolean. */
    private Boolean booleanConstant(Expression e) {
        if (e == null) return null;
        e = strip(e);
        if (!isConstant(e)) return null;
        Object value;
        try {
            value = executor.evalExpr(e, null);
        } catch (RuntimeException ex) {
            return null;
        }
        return value instanceof Boolean ? (Boolean) value : null;
    }

    // ---- What is left of the condition ----

    /**
     * The shapes measured against PostgreSQL as never mergejoinable. Anything else — a user's own
     * operator, a placeholder, a node this does not know — is accepted rather than refused.
     */
    private boolean recognisablyUnmergeable(Expression c) {
        c = strip(c);
        if (c instanceof BinaryExpr) {
            switch (((BinaryExpr) c).op()) {
                case EQUAL: case NOT_EQUAL: case LESS_THAN: case GREATER_THAN:
                case LESS_EQUAL: case GREATER_EQUAL: case AND: case OR:
                case ADD: case SUBTRACT: case MULTIPLY: case DIVIDE: case MODULO: case POWER:
                case CONCAT: case LIKE: case ILIKE: case SIMILAR_TO:
                case IS_DISTINCT_FROM: case IS_NOT_DISTINCT_FROM:
                case REGEX_MATCH: case REGEX_IMATCH:
                case NOT_REGEX_MATCH: case NOT_REGEX_IMATCH:
                    return true;
                default:
                    return false;
            }
        }
        return c instanceof IsNullExpr || c instanceof IsBooleanExpr || c instanceof BetweenExpr
                || c instanceof LikeExpr || c instanceof ExistsExpr || c instanceof InExpr
                || c instanceof ColumnRef || c instanceof Literal || c instanceof CaseExpr
                || c instanceof AnyAllArrayExpr || c instanceof AnyAllExpr
                || c instanceof UnaryExpr || c instanceof FunctionCallExpr;
    }

    /** {@code x = y} with x reading only one side of the join and y only the other. */
    private boolean isCrossSideEquality(Expression c,
                                        List<RowContext.TableBinding> leftShape,
                                        List<RowContext.TableBinding> rightShape) {
        c = strip(c);
        if (!(c instanceof BinaryExpr)) return false;
        BinaryExpr bin = (BinaryExpr) c;
        if (bin.op() != BinaryExpr.BinOp.EQUAL) return false;
        int a = sideRead(bin.left(), leftShape, rightShape);
        int b = sideRead(bin.right(), leftShape, rightShape);
        return (a == SIDE_LEFT && b == SIDE_RIGHT) || (a == SIDE_RIGHT && b == SIDE_LEFT);
    }

    /** Which of the join's two sides an expression reads from, if exactly one of them. */
    private int sideRead(Expression e,
                         List<RowContext.TableBinding> leftShape,
                         List<RowContext.TableBinding> rightShape) {
        List<ColumnRef> refs = new ArrayList<ColumnRef>();
        collectVisibleRefs(e, refs);
        int seen = SIDE_NONE;
        for (ColumnRef ref : refs) {
            seen |= placeRef(ref, leftShape, rightShape);
        }
        return seen;
    }

    /**
     * The column references an expression makes to the rows being joined. A subquery with a FROM
     * clause of its own resolves its own names and contributes none; one without a FROM clause —
     * {@code (SELECT b.y)} — reads the row the join is building, and PostgreSQL keeps the clause
     * around it a join clause of that side.
     */
    private void collectVisibleRefs(Object node, List<ColumnRef> out) {
        if (node == null) return;
        if (node instanceof ColumnRef) {
            out.add((ColumnRef) node);
            return;
        }
        if (node instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) node;
            if (sel.from() != null && !sel.from().isEmpty()) return;
        }
        if (node instanceof SetOpStmt) return;
        final List<ColumnRef> sink = out;
        AstWalk.forEachChild(node, new java.util.function.Consumer<Object>() {
            @Override public void accept(Object child) { collectVisibleRefs(child, sink); }
        });
    }

    /** Which side of the join a name reads: one of them, both of them, or neither. */
    private int placeRef(ColumnRef ref,
                         List<RowContext.TableBinding> leftShape,
                         List<RowContext.TableBinding> rightShape) {
        if (ref.table() != null) {
            int side = SIDE_NONE;
            if (namedIn(leftShape, ref.table())) side |= SIDE_LEFT;
            if (namedIn(rightShape, ref.table())) side |= SIDE_RIGHT;
            return side;
        }
        int side = SIDE_NONE;
        if (hasColumn(leftShape, ref.column())) side |= SIDE_LEFT;
        if (hasColumn(rightShape, ref.column())) side |= SIDE_RIGHT;
        return side;
    }

    // ---- Quals above the join ----

    /**
     * Whether a qual above the join rejects the rows one side was padded with, which makes
     * PostgreSQL plan a left, right or inner join instead and ask nothing of the condition.
     */
    private boolean downgradedFromAbove(List<RowContext.TableBinding> leftShape,
                                        List<RowContext.TableBinding> rightShape) {
        if (from.inheritedQualsUnreadable) return true;
        List<Expression> quals = new ArrayList<Expression>();
        if (from.enclosingWhere != null) {
            List<Expression> parts = new ArrayList<Expression>();
            flattenAnd(from.enclosingWhere, parts);
            for (Expression p : parts) {
                Expression n = normalize(p, 0);
                // A WHERE that can never hold leaves PostgreSQL a plan with no join in it.
                if (n == NOT_TRUE) return true;
                if (n != TRUE) quals.add(n);
            }
        }
        // A HAVING clause with no aggregate in it is a WHERE clause PostgreSQL moves for you.
        if (from.enclosingHaving != null) {
            List<Expression> parts = new ArrayList<Expression>();
            flattenAnd(from.enclosingHaving, parts);
            for (Expression p : parts) {
                if (!StoredExprCheck.hasAggregate(p)) quals.add(normalize(p, 0));
            }
        }
        for (Expression q : from.joinQualsAbove) {
            quals.add(normalize(q, 0));
        }

        // A name this cannot place belongs to something outside the join — a sibling relation, an
        // enclosing query, or nothing at all, which PostgreSQL reports before it plans.
        for (Expression q : quals) {
            List<ColumnRef> refs = new ArrayList<ColumnRef>();
            collectVisibleRefs(q, refs);
            for (ColumnRef ref : refs) {
                if (placeRef(ref, leftShape, rightShape) == SIDE_NONE) return true;
            }
        }

        Set<ColumnRef> nonNullable = new HashSet<ColumnRef>();
        for (Expression q : quals) {
            nonNullableVars(q, nonNullable);
        }
        for (ColumnRef ref : nonNullable) {
            int side = placeRef(ref, leftShape, rightShape);
            if (side == SIDE_LEFT || side == SIDE_RIGHT) return true;
        }
        for (String name : from.inheritedNonNullNames) {
            if (hasColumn(leftShape, name) || hasColumn(rightShape, name)) return true;
        }
        return false;
    }

    /**
     * The columns a qual proves are not null, PostgreSQL's {@code find_nonnullable_vars}: the AND
     * of two quals proves what either proves, the OR only what both do, and a test for null
     * proves nothing.
     */
    void nonNullableVars(Expression e, Set<ColumnRef> out) {
        if (e == null) return;
        e = strip(e);
        if (e instanceof ColumnRef) {
            out.add((ColumnRef) e);
            return;
        }
        if (e instanceof BinaryExpr) {
            BinaryExpr b = (BinaryExpr) e;
            if (b.op() == BinaryExpr.BinOp.AND) {
                nonNullableVars(b.left(), out);
                nonNullableVars(b.right(), out);
                return;
            }
            if (b.op() == BinaryExpr.BinOp.OR) {
                List<Expression> arms = new ArrayList<Expression>();
                flattenOr(b, arms);
                Set<ColumnRef> shared = null;
                for (Expression arm : arms) {
                    Set<ColumnRef> armVars = new HashSet<ColumnRef>();
                    nonNullableVars(arm, armVars);
                    if (shared == null) shared = armVars;
                    else shared.retainAll(armVars);
                }
                if (shared != null) out.addAll(shared);
                return;
            }
            if (b.op() == BinaryExpr.BinOp.IS_DISTINCT_FROM
                    || b.op() == BinaryExpr.BinOp.IS_NOT_DISTINCT_FROM) {
                return;
            }
            strictVars(b.left(), out);
            strictVars(b.right(), out);
            return;
        }
        if (e instanceof IsNullExpr) {
            if (((IsNullExpr) e).negated()) strictVars(((IsNullExpr) e).expr(), out);
            return;
        }
        if (e instanceof IsBooleanExpr) {
            IsBooleanExpr t = (IsBooleanExpr) e;
            if (t.test() == IsBooleanExpr.BooleanTest.IS_TRUE
                    || t.test() == IsBooleanExpr.BooleanTest.IS_FALSE
                    || t.test() == IsBooleanExpr.BooleanTest.IS_NOT_UNKNOWN) {
                strictVars(t.expr(), out);
            }
            return;
        }
        if (e instanceof BetweenExpr) {
            BetweenExpr b = (BetweenExpr) e;
            strictVars(b.expr(), out);
            strictVars(b.low(), out);
            strictVars(b.high(), out);
            return;
        }
        if (e instanceof LikeExpr) {
            LikeExpr l = (LikeExpr) e;
            strictVars(l.left(), out);
            strictVars(l.pattern(), out);
            return;
        }
        if (e instanceof InExpr) {
            strictVars(((InExpr) e).expr(), out);
            return;
        }
        if (e instanceof AnyAllArrayExpr) {
            strictVars(((AnyAllArrayExpr) e).left(), out);
            return;
        }
        if (e instanceof CastExpr) {
            strictVars(e, out);
        }
    }

    /**
     * The columns an expression reads in a position where a null in makes a null out. Reading into
     * a function this does not know is deliberate: taking a function for strict can only make the
     * check accept a join, never refuse one.
     */
    private void strictVars(Expression e, Set<ColumnRef> out) {
        if (e == null) return;
        e = strip(e);
        if (e instanceof ColumnRef) {
            out.add((ColumnRef) e);
            return;
        }
        if (e instanceof CastExpr) {
            strictVars(((CastExpr) e).expr(), out);
            return;
        }
        if (e instanceof BinaryExpr) {
            BinaryExpr b = (BinaryExpr) e;
            if (b.op() == BinaryExpr.BinOp.AND || b.op() == BinaryExpr.BinOp.OR
                    || b.op() == BinaryExpr.BinOp.IS_DISTINCT_FROM
                    || b.op() == BinaryExpr.BinOp.IS_NOT_DISTINCT_FROM) {
                return;
            }
            strictVars(b.left(), out);
            strictVars(b.right(), out);
            return;
        }
        if (e instanceof UnaryExpr) {
            UnaryExpr u = (UnaryExpr) e;
            if (u.op() == UnaryExpr.UnaryOp.NOT) return;
            strictVars(u.operand(), out);
            return;
        }
        if (e instanceof LikeExpr) {
            strictVars(((LikeExpr) e).left(), out);
            strictVars(((LikeExpr) e).pattern(), out);
            return;
        }
        if (e instanceof FunctionCallExpr) {
            FunctionCallExpr f = (FunctionCallExpr) e;
            String name = f.name() == null ? "" : f.name().toLowerCase();
            int dot = name.lastIndexOf('.');
            if (dot >= 0) name = name.substring(dot + 1);
            if (NON_STRICT.contains(name)) return;
            if (f.args() != null) {
                for (Expression a : f.args()) strictVars(a, out);
            }
        }
    }

    // ---- Shared helpers ----

    /** Splits a condition into the clauses AND joins, which is how PostgreSQL reads one. */
    static void flattenAnd(Expression e, List<Expression> out) {
        if (e instanceof BinaryExpr && ((BinaryExpr) e).op() == BinaryExpr.BinOp.AND) {
            flattenAnd(((BinaryExpr) e).left(), out);
            flattenAnd(((BinaryExpr) e).right(), out);
            return;
        }
        out.add(e);
    }

    private static void flattenOr(Expression e, List<Expression> out) {
        if (e instanceof BinaryExpr && ((BinaryExpr) e).op() == BinaryExpr.BinOp.OR) {
            flattenOr(((BinaryExpr) e).left(), out);
            flattenOr(((BinaryExpr) e).right(), out);
            return;
        }
        out.add(e);
    }

    private static Expression andOf(List<Expression> parts) {
        Expression out = parts.get(0);
        for (int i = 1; i < parts.size(); i++) {
            out = new BinaryExpr(out, BinaryExpr.BinOp.AND, parts.get(i));
        }
        return out;
    }

    private static Expression orOf(List<Expression> parts) {
        Expression out = parts.get(0);
        for (int i = 1; i < parts.size(); i++) {
            out = new BinaryExpr(out, BinaryExpr.BinOp.OR, parts.get(i));
        }
        return out;
    }

    /** True when the condition writes a bare name both sides answer to. */
    private boolean hasAmbiguousBareName(Expression on,
                                         List<RowContext.TableBinding> leftShape,
                                         List<RowContext.TableBinding> rightShape) {
        List<ColumnRef> refs = new ArrayList<ColumnRef>();
        collectVisibleRefs(on, refs);
        for (ColumnRef ref : refs) {
            if (ref.table() == null && ref.column() != null
                    && hasColumn(leftShape, ref.column()) && hasColumn(rightShape, ref.column())) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the expression stands on its own and gives the same answer every time, which is
     * what PostgreSQL folds before it plans and what this may evaluate to find out.
     */
    private static boolean isConstant(Expression e) {
        return !AstWalk.anyMatch(e, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                if (n instanceof FunctionCallExpr) {
                    String name = ((FunctionCallExpr) n).name();
                    if (name == null) return true;
                    name = name.toLowerCase();
                    int dot = name.lastIndexOf('.');
                    if (dot >= 0) name = name.substring(dot + 1);
                    return VOLATILE_FUNCTIONS.contains(name);
                }
                return n instanceof ColumnRef || n instanceof WildcardExpr
                        || n instanceof CompositeStarExpr || n instanceof SelectStmt
                        || n instanceof SetOpStmt || n instanceof ParamRef;
            }
        });
    }

    /** True when the expression reads anything from a row rather than standing on its own. */
    private static boolean mentionsRelation(Expression e) {
        return AstWalk.anyMatch(e, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                return n instanceof ColumnRef || n instanceof WildcardExpr
                        || n instanceof CompositeStarExpr || n instanceof SelectStmt
                        || n instanceof SetOpStmt || n instanceof ParamRef;
            }
        });
    }

    private static boolean namedIn(List<RowContext.TableBinding> shape, String name) {
        if (shape == null) return false;
        for (RowContext.TableBinding b : shape) {
            if (b.alias() != null ? b.alias().equalsIgnoreCase(name)
                    : b.table().getName().equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasColumn(List<RowContext.TableBinding> shape, String column) {
        if (shape == null || column == null) return false;
        for (RowContext.TableBinding b : shape) {
            if (b.table().getColumnIndex(column) >= 0) return true;
        }
        return false;
    }
}
