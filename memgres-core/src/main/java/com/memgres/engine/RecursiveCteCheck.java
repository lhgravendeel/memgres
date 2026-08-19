package com.memgres.engine;

import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.CaseExpr;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.SetOpStmt;
import com.memgres.engine.parser.ast.Statement;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The shape PostgreSQL insists a {@code WITH RECURSIVE} item has before it will run it.
 *
 * <p>A recursive WITH item is only recursive because its body names itself, and PostgreSQL can
 * only evaluate that self-reference in one place: a single scan of the rows the previous
 * iteration produced, sitting at the top of the recursive term's FROM clause. Anything that would
 * need the whole result at once — a second scan of it, an aggregate over it, the null-extended
 * side of an outer join, a sub-select — has no meaning while the result is still being built, so
 * PostgreSQL refuses the query (42P19) rather than answer a different question. A set operation
 * counts only where an arm is subtracted or where duplicate counts matter: the right-hand side of
 * any EXCEPT, and either side of an INTERSECT ALL or EXCEPT ALL. A few clauses PostgreSQL simply
 * has not implemented for a recursive query at all — ORDER BY, LIMIT, OFFSET, FOR UPDATE — are
 * 0A000 and are read before the self-reference is looked for, and a recursive term whose column
 * type differs from the non-recursive term's is 42804.
 *
 * <p>Every rule here fires only on a WITH item that both declares RECURSIVE and actually names
 * itself; ordinary recursion — one self-reference in the FROM clause, joined to anything, under
 * WHERE, GROUP BY, DISTINCT, a window function, an aggregate in the <em>non</em>-recursive term,
 * a plain INTERSECT, or the left side of a plain EXCEPT — is left alone.
 */
public final class RecursiveCteCheck {

    private RecursiveCteCheck() { }

    /** Whether a WITH item was written as a statement that writes rows rather than as a query. */
    private static boolean writesRows(Statement stmt) {
        return stmt instanceof com.memgres.engine.parser.ast.InsertStmt
                || stmt instanceof com.memgres.engine.parser.ast.UpdateStmt
                || stmt instanceof com.memgres.engine.parser.ast.DeleteStmt
                || stmt instanceof com.memgres.engine.parser.ast.MergeStmt;
    }

    /** True when a WITH item's body names the item itself, so it really does recurse. */
    public static boolean selfReferencing(SelectStmt.CommonTableExpr cte) {
        return !selfReferences(cte.query(), cte.name()).isEmpty();
    }

    /**
     * Applies every structural rule to a WITH item that declares RECURSIVE and names itself.
     * Called before the fixed-point loop starts, so a refused query produces no rows at all.
     */
    static void validate(SelectExecutor select, SelectStmt.CommonTableExpr cte) {
        String name = cte.name();

        // A statement that writes cannot be iterated to a fixed point at all, so PostgreSQL says
        // that before it says anything about the shape the item was written in. Reporting only
        // the missing UNION described the shape and left the reason unsaid.
        if (writesRows(cte.query())) {
            throw new MemgresException("recursive query \"" + name
                    + "\" must not contain data-modifying statements", "42P19");
        }

        // The only shape PG can evaluate is "non-recursive term UNION [ALL] recursive term".
        if (!(cte.query() instanceof SetOpStmt)
                || ((SetOpStmt) cte.query()).op() != SetOpStmt.SetOpType.UNION) {
            throw new MemgresException("recursive query \"" + name
                    + "\" does not have the form non-recursive-term UNION [ALL] recursive-term",
                    "42P19");
        }
        SetOpStmt setOp = (SetOpStmt) cte.query();

        // Clauses PG has not implemented for a recursive query. These sit on the set operation
        // itself; the same words inside a parenthesised arm or a subquery belong to that arm and
        // are allowed, which is why only the top node is looked at. PostgreSQL reads them before
        // it looks for the self-reference at all, so a query that is wrong in both ways — a
        // second reference and a LIMIT — is refused for the LIMIT.
        if (setOp.orderBy() != null && !setOp.orderBy().isEmpty()) {
            throw new MemgresException("ORDER BY in a recursive query is not implemented", "0A000");
        }
        if (setOp.offset() != null) {
            throw new MemgresException("OFFSET in a recursive query is not implemented", "0A000");
        }
        if (setOp.limit() != null) {
            throw new MemgresException("LIMIT in a recursive query is not implemented", "0A000");
        }
        if (hasLockClause(setOp.left()) || hasLockClause(setOp.right())) {
            throw new MemgresException("FOR UPDATE/SHARE in a recursive query is not implemented",
                    "0A000");
        }

        // The seed has to be computable without the result it seeds.
        if (!selfReferences(setOp.left(), name).isEmpty()) {
            throw new MemgresException("recursive reference to query \"" + name
                    + "\" must not appear within its non-recursive term", "42P19");
        }

        List<String> refs = selfReferences(setOp.right(), name);
        if (refs.isEmpty()) return;

        // Where the reference sits is decided before how many there are: PostgreSQL names the
        // context of the offending reference — a sub-select, an outer join, EXCEPT, INTERSECT ALL
        // — and only says "more than once" when every one of them was in a place that allows one.
        for (String context : CONTEXT_PRIORITY) {
            if (refs.contains(context)) {
                throw new MemgresException("recursive reference to query \"" + name
                        + "\" must not appear within " + context, "42P19");
            }
        }
        if (refs.size() > 1) {
            throw new MemgresException("recursive reference to query \"" + name
                    + "\" must not appear more than once", "42P19");
        }

        // An aggregate reads the whole recursive term at once, which is exactly what the
        // iteration cannot supply. Only this query level counts: an aggregate inside a FROM
        // subquery or a scalar sub-select of the recursive term is over that query, not this one.
        if (recursiveTermHasAggregate(select, setOp.right())) {
            throw new MemgresException(
                    "aggregate functions are not allowed in a recursive query's recursive term",
                    "42P19");
        }
    }

    /**
     * The union's output type has to be the type the non-recursive term already had.
     *
     * <p>PostgreSQL resolves one output type per column from both arms and then insists the seed
     * carried it, because the seed's rows are the ones already in the result when the recursive
     * term first runs — there is nothing left to re-type them. So a recursive term that widens a
     * column (integer to bigint, integer to numeric, date to timestamp) is refused, while one
     * that narrows into the seed's own type is fine, and two arms with no common type at all are
     * refused earlier and differently.
     *
     * <p>The recursive term's types are read off the rows its first iteration actually produced;
     * an expression's declared type is what the engine computed for it, which is the same thing
     * the union would resolve against. A seed column written as a bare NULL has no type of its
     * own, so the recursive term's type is the union's and the mismatch is reported against the
     * text PostgreSQL falls back to.
     */
    static void checkColumnTypes(String cteName, List<Column> baseColumns, Statement baseTerm,
                                 Statement recTerm, List<Column> recursiveColumns) {
        int n = Math.min(baseColumns.size(), recursiveColumns.size());
        for (int i = 0; i < n; i++) {
            DataType recType = recursiveColumns.get(i).getType();
            boolean baseUntyped = isUntypedNull(baseTerm, i);
            DataType baseType = baseUntyped ? DataType.TEXT : baseColumns.get(i).getType();
            if (baseType == null || recType == null) continue;
            if (!baseUntyped) checkAcrossCategories(baseType, recType, baseTerm, recTerm, i);
            int baseFamily = baseUntyped ? family(recType) : family(baseType);
            int recFamily = family(recType);
            if (baseFamily == FAMILY_NONE || recFamily == FAMILY_NONE) continue;

            if (baseFamily == recFamily) {
                // The character types are not a widening ladder: PostgreSQL runs a varchar seed
                // with a text recursive term and answers in varchar, and the only complaints it
                // makes here are about length modifiers this engine does not carry. Nothing to
                // show, so nothing refused.
                if (recFamily == FAMILY_TEXT) continue;
                int baseRank = baseUntyped ? 0 : rank(baseType);
                int recRank = Math.max(rank(recType), castRankInValuePosition(recTerm, i, recFamily));
                if (recRank > baseRank) {
                    throw widened(cteName, i, baseType, widestOf(recFamily, recRank));
                }
                continue;
            }
            // Different families have no common type at all — but only say so where the text
            // side was cast to text outright. An unadorned string literal is of no type yet:
            // PostgreSQL reads it as the other arm's type, so 'x' against an integer column is
            // bad input rather than an unmatched union, and '5' is simply the number five.
            if (statedAsText(baseFamily == FAMILY_TEXT ? baseTerm : recTerm, i)) {
                throw cannotBeMatched(baseType, recType);
            }
        }
    }

    /**
     * The types a recursive term may not carry at all, whatever the widening rules say.
     *
     * <p>The families above are the ones that have a widening ladder to walk; a boolean, a jsonb,
     * an array or a network address has none, and against a number or a date there is no common
     * type either. PostgreSQL says so where both sides carry a type of their own — a bare string
     * literal does not, which is why a text side is only reported when the query wrote the cast.
     */
    private static void checkAcrossCategories(DataType baseType, DataType recType,
                                              Statement baseTerm, Statement recTerm, int i) {
        if (baseType == null || recType == null || baseType == recType) return;
        TypeCoercion.TypeCategory baseCat = TypeCoercion.categoryOf(baseType);
        TypeCoercion.TypeCategory recCat = TypeCoercion.categoryOf(recType);
        // An array and a scalar are not each other's type whatever their elements are, and this
        // engine files every array under the same category, so ask the element question first.
        boolean baseIsArray = DataType.elementOf(baseType) != null;
        boolean recIsArray = DataType.elementOf(recType) != null;
        if (baseIsArray != recIsArray) {
            TypeCoercion.TypeCategory scalarCat = baseIsArray ? recCat : baseCat;
            if (scalarCat == TypeCoercion.TypeCategory.UNKNOWN) return;
            if (scalarCat == TypeCoercion.TypeCategory.STRING
                    && !statedAsText(baseIsArray ? recTerm : baseTerm, i)) {
                return;
            }
            throw cannotBeMatched(baseType, recType);
        }
        if (baseCat == recCat) return;
        if (baseCat == TypeCoercion.TypeCategory.UNKNOWN
                || recCat == TypeCoercion.TypeCategory.UNKNOWN) return;
        boolean baseIsText = baseCat == TypeCoercion.TypeCategory.STRING;
        boolean recIsText = recCat == TypeCoercion.TypeCategory.STRING;
        if (baseIsText || recIsText) {
            if (statedAsText(baseIsText ? baseTerm : recTerm, i)) {
                throw cannotBeMatched(baseType, recType);
            }
            return;
        }
        throw cannotBeMatched(baseType, recType);
    }

    private static MemgresException cannotBeMatched(DataType baseType, DataType recType) {
        return new MemgresException("UNION types " + displayName(baseType) + " and "
                + displayName(recType) + " cannot be matched", "42804");
    }

    private static MemgresException widened(String cteName, int col, DataType base, DataType rec) {
        MemgresException ex = new MemgresException("recursive query \"" + cteName + "\" column "
                + (col + 1) + " has type " + displayName(base) + " in non-recursive term but type "
                + displayName(rec) + " overall", "42804");
        ex.setHint("Cast the output of the non-recursive term to the correct type.");
        return ex;
    }

    /**
     * The widest rank an explicit cast reaches in a value position of the i'th target.
     *
     * <p>The engine computes COALESCE and CASE as the type of the branch it took, so a wider
     * branch it did not take this iteration is invisible in the result — but PostgreSQL resolves
     * those from every branch at once. Only positions whose type reaches the result are read: a
     * CASE's WHEN condition and a comparison's operands decide nothing about the value's type,
     * and reading them would refuse queries PostgreSQL runs.
     */
    private static int castRankInValuePosition(Statement recTerm, int i, int wantedFamily) {
        return valueCastRank(targetOf(recTerm, i), wantedFamily);
    }

    private static int valueCastRank(Expression expr, int wantedFamily) {
        if (expr == null) return 0;
        if (expr instanceof CastExpr) {
            DataType cast = DataType.fromPgName(((CastExpr) expr).typeName());
            return family(cast) == wantedFamily ? rank(cast) : 0;
        }
        int widest = 0;
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            if (c.whenClauses() != null) {
                for (CaseExpr.WhenClause when : c.whenClauses()) {
                    widest = Math.max(widest, valueCastRank(when.result(), wantedFamily));
                }
            }
            widest = Math.max(widest, valueCastRank(c.elseExpr(), wantedFamily));
            return widest;
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr call = (FunctionCallExpr) expr;
            String fn = call.name() == null ? "" : call.name().toLowerCase();
            // Only the functions whose result is one of their arguments carry an argument's type.
            if (!fn.equals("coalesce") && !fn.equals("nullif") && !fn.equals("greatest")
                    && !fn.equals("least")) return 0;
            if (call.args() != null) {
                for (Expression arg : call.args()) {
                    widest = Math.max(widest, valueCastRank(arg, wantedFamily));
                }
            }
            return widest;
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            switch (bin.op()) {
                case ADD: case SUBTRACT: case MULTIPLY: case DIVIDE: case MODULO:
                    return Math.max(valueCastRank(bin.left(), wantedFamily),
                            valueCastRank(bin.right(), wantedFamily));
                default:
                    return 0;
            }
        }
        return 0;
    }

    /** The type at a given rank within a family, for the message. */
    private static DataType widestOf(int family, int rank) {
        if (family == FAMILY_NUMBER) {
            switch (rank) {
                case 1: return DataType.SMALLINT;
                case 2: return DataType.INTEGER;
                case 3: return DataType.BIGINT;
                case 4: return DataType.NUMERIC;
                case 5: return DataType.REAL;
                default: return DataType.DOUBLE_PRECISION;
            }
        }
        if (family == FAMILY_DATETIME) {
            return rank <= 1 ? DataType.DATE : rank == 2 ? DataType.TIMESTAMP : DataType.TIMESTAMPTZ;
        }
        return rank <= 1 ? DataType.CHAR : rank == 2 ? DataType.VARCHAR : DataType.TEXT;
    }

    /** The i'th target of a plain SELECT term, or null when the term is not one. */
    private static Expression targetOf(Statement term, int i) {
        if (!(term instanceof SelectStmt)) return null;
        List<SelectStmt.SelectTarget> targets = ((SelectStmt) term).targets();
        if (targets == null || i >= targets.size()) return null;
        return targets.get(i).expr();
    }

    /** True when the seed wrote a bare NULL, which carries no type of its own. */
    private static boolean isUntypedNull(Statement term, int i) {
        Expression expr = targetOf(term, i);
        return expr instanceof Literal && ((Literal) expr).literalType() == Literal.LiteralType.NULL;
    }

    /** True when a term said "text" outright — an explicit cast, not a literal of no type yet. */
    private static boolean statedAsText(Statement term, int i) {
        Expression expr = targetOf(term, i);
        return expr instanceof CastExpr
                && family(DataType.fromPgName(((CastExpr) expr).typeName())) == FAMILY_TEXT;
    }

    private static final int FAMILY_NONE = 0;
    private static final int FAMILY_NUMBER = 1;
    private static final int FAMILY_TEXT = 2;
    private static final int FAMILY_DATETIME = 3;

    private static int family(DataType t) {
        if (t == null) return FAMILY_NONE;
        switch (t) {
            case SMALLINT: case SMALLSERIAL: case INTEGER: case SERIAL:
            case BIGINT: case BIGSERIAL: case NUMERIC: case REAL: case DOUBLE_PRECISION:
                return FAMILY_NUMBER;
            case TEXT: case VARCHAR: case CHAR: case NAME:
                return FAMILY_TEXT;
            case DATE: case TIMESTAMP: case TIMESTAMPTZ:
                return FAMILY_DATETIME;
            default:
                return FAMILY_NONE;
        }
    }

    /** How wide a type is within its family; a wider recursive term than seed is the error. */
    private static int rank(DataType t) {
        if (t == null) return 0;
        switch (t) {
            case SMALLINT: case SMALLSERIAL: return 1;
            case INTEGER: case SERIAL: return 2;
            case BIGINT: case BIGSERIAL: return 3;
            case NUMERIC: return 4;
            case REAL: return 5;
            case DOUBLE_PRECISION: return 6;
            case DATE: return 1;
            case TIMESTAMP: return 2;
            case TIMESTAMPTZ: return 3;
            default: return 0;
        }
    }

    // ---- self reference discovery ----

    /** The context names, most specific first, so a query with two bad references names one. */
    private static final String[] CONTEXT_PRIORITY =
            {"a subquery", "an outer join", "EXCEPT", "INTERSECT"};

    /**
     * One entry per unqualified FROM reference to {@code name} inside {@code node}: the name of
     * the construct that makes the reference inadmissible, or null when the reference sits
     * somewhere the iteration can evaluate it. A schema-qualified name is never a WITH item, so it
     * is not counted.
     *
     * <p>Which constructs those are is what PostgreSQL will actually refuse, no more. A sub-select
     * of an expression (EXISTS, IN, a scalar subquery) needs the whole result at once, and so does
     * the null-extended side of an outer join. A set operation only needs it where the arm is
     * subtracted or where duplicate counts matter: the right-hand side of any EXCEPT, and either
     * side of an INTERSECT ALL or EXCEPT ALL. Plain {@code INTERSECT} and the left side of a plain
     * {@code EXCEPT} grow with the rows the previous round produced, so PostgreSQL runs them and
     * so does this.
     */
    private static List<String> selfReferences(Object node, String name) {
        List<String> found = new ArrayList<>();
        String lcName = name.toLowerCase();
        Deque<Object> nodes = new ArrayDeque<>();
        Deque<String> contexts = new ArrayDeque<>();
        Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        push(nodes, contexts, node, NONE);
        while (!nodes.isEmpty()) {
            Object cur = nodes.pop();
            String context = contexts.pop();
            if (NONE.equals(context)) context = null;
            if (cur == null || !seen.add(cur)) continue;
            if (cur instanceof SelectStmt.TableRef) {
                SelectStmt.TableRef ref = (SelectStmt.TableRef) cur;
                if (ref.schema() == null && ref.table() != null
                        && ref.table().toLowerCase().equals(lcName)) {
                    found.add(context);
                }
                continue;
            }
            if (cur instanceof String || cur instanceof Number || cur instanceof Boolean
                    || cur instanceof Character || cur instanceof Enum) continue;
            // A query that declares a WITH item of this name means that item everywhere below,
            // not the one being defined; nothing under it is a self-reference.
            if (cur instanceof SelectStmt && declaresWithItem((SelectStmt) cur, lcName)) continue;
            if (cur instanceof SetOpStmt) {
                SetOpStmt sop = (SetOpStmt) cur;
                push(nodes, contexts, sop.left(), armContext(sop, context, true));
                push(nodes, contexts, sop.right(), armContext(sop, context, false));
                push(nodes, contexts, sop.orderBy(), context);
                push(nodes, contexts, sop.limit(), context);
                push(nodes, contexts, sop.offset(), context);
                continue;
            }
            if (cur instanceof SelectStmt.JoinFrom) {
                SelectStmt.JoinFrom j = (SelectStmt.JoinFrom) cur;
                SelectStmt.JoinType type = j.joinType();
                boolean rightNullable = type == SelectStmt.JoinType.LEFT
                        || type == SelectStmt.JoinType.NATURAL_LEFT
                        || type == SelectStmt.JoinType.FULL
                        || type == SelectStmt.JoinType.NATURAL_FULL;
                boolean leftNullable = type == SelectStmt.JoinType.RIGHT
                        || type == SelectStmt.JoinType.NATURAL_RIGHT
                        || type == SelectStmt.JoinType.FULL
                        || type == SelectStmt.JoinType.NATURAL_FULL;
                // A join names the reference's context only where nothing nearer already has.
                push(nodes, contexts, j.left(), leftNullable && context == null
                        ? "an outer join" : context);
                push(nodes, contexts, j.right(), rightNullable && context == null
                        ? "an outer join" : context);
                push(nodes, contexts, j.on(), context);
                continue;
            }
            // Once inside an expression, everything below it is inside a sub-select of that
            // expression — the FROM-clause subqueries PG allows are reached through FromItems.
            String childContext = cur instanceof Expression ? "a subquery" : context;
            if (cur instanceof Collection) {
                for (Object item : (Collection<?>) cur) push(nodes, contexts, item, childContext);
                continue;
            }
            if (cur instanceof Map) {
                for (Object item : ((Map<?, ?>) cur).values()) push(nodes, contexts, item, childContext);
                continue;
            }
            Class<?> cls = cur.getClass();
            Package pkg = cls.getPackage();
            if (pkg == null || !pkg.getName().startsWith("com.memgres")) continue;
            for (java.lang.reflect.Field f : cls.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(f.getModifiers())) continue;
                try {
                    f.setAccessible(true);
                    push(nodes, contexts, f.get(cur), childContext);
                } catch (IllegalAccessException | RuntimeException ignored) { }
            }
        }
        return found;
    }

    /** ArrayDeque takes no nulls, so an absent context travels as this marker. */
    private static final String NONE = "";

    private static void push(Deque<Object> nodes, Deque<String> contexts, Object node,
                             String context) {
        if (node == null) return;
        nodes.push(node);
        contexts.push(context == null ? NONE : context);
    }

    /** The context an arm of a set operation puts its self-references in. */
    private static String armContext(SetOpStmt sop, String context, boolean leftArm) {
        switch (sop.op()) {
            case EXCEPT:
                return !leftArm || sop.all() ? "EXCEPT" : context;
            case INTERSECT:
                return sop.all() ? "INTERSECT" : context;
            default:
                return context;
        }
    }

    /** True when a query's own WITH clause claims the name, shadowing an enclosing one. */
    private static boolean declaresWithItem(SelectStmt stmt, String lcName) {
        if (stmt.withClauses() == null) return false;
        for (SelectStmt.CommonTableExpr item : stmt.withClauses()) {
            if (item.name() != null && item.name().toLowerCase().equals(lcName)) return true;
        }
        return false;
    }

    // ---- clause inspection ----

    private static boolean hasLockClause(Statement stmt) {
        return stmt instanceof SelectStmt && ((SelectStmt) stmt).lockClause() != null;
    }

    /** An aggregate written at the recursive term's own query level. */
    private static boolean recursiveTermHasAggregate(SelectExecutor select, Statement stmt) {
        if (!(stmt instanceof SelectStmt)) return false;
        SelectStmt sel = (SelectStmt) stmt;
        if (sel.targets() != null) {
            for (SelectStmt.SelectTarget target : sel.targets()) {
                if (target.expr() != null && select.containsAggregate(target.expr())) return true;
            }
        }
        if (sel.where() != null && select.containsAggregate(sel.where())) return true;
        if (sel.having() != null && select.containsAggregate(sel.having())) return true;
        if (sel.groupBy() != null) {
            for (Expression e : sel.groupBy()) {
                if (e != null && select.containsAggregate(e)) return true;
            }
        }
        return false;
    }

    /**
     * Refuse a column SEARCH or CYCLE adds under a name the WITH item's own query already answers
     * to.
     *
     * <p>The added column is in place while the recursive term is read, so the item's name answers
     * to that column name twice by then. PostgreSQL reads the term before it reads the clause that
     * added the column: where the term names the column without saying which relation it means,
     * that unsettleable reference is what the statement is refused for; where it does not, nothing
     * was ambiguous and the collision between the two columns is.
     */
    static void rejectAddedColumn(SelectStmt.CommonTableExpr cte, List<String> columns,
                                  String name, String what) {
        if (name == null || !holdsColumn(columns, name)) return;
        if (namedBareInRecursiveTerm(cte, name)) {
            throw new MemgresException("column reference \"" + name + "\" is ambiguous", "42702");
        }
        throw new MemgresException(
                what + " \"" + name + "\" already used in WITH query column list", "42601");
    }

    /** Whether the recursive term names this column without saying which relation it comes from. */
    private static boolean namedBareInRecursiveTerm(SelectStmt.CommonTableExpr cte, String name) {
        if (!(cte.query() instanceof SetOpStmt)) return false;
        return AstWalk.anyMatch(((SetOpStmt) cte.query()).right(),
                node -> node instanceof com.memgres.engine.parser.ast.ColumnRef
                        && ((com.memgres.engine.parser.ast.ColumnRef) node).table() == null
                        && ((com.memgres.engine.parser.ast.ColumnRef) node).column()
                                .equalsIgnoreCase(name));
    }

    private static boolean holdsColumn(List<String> columns, String name) {
        for (String column : columns) {
            if (column != null && column.equalsIgnoreCase(name)) return true;
        }
        return false;
    }

    /** The name PostgreSQL prints for a type in a UNION or recursive-query complaint. */
    private static String displayName(DataType type) {
        if (type == null) return "unknown";
        switch (type) {
            case SMALLINT: case SMALLSERIAL: return "smallint";
            case INTEGER: case SERIAL: return "integer";
            case BIGINT: case BIGSERIAL: return "bigint";
            case NUMERIC: return "numeric";
            case REAL: return "real";
            case DOUBLE_PRECISION: return "double precision";
            case TEXT: return "text";
            case VARCHAR: return "character varying";
            case CHAR: return "character";
            case NAME: return "name";
            case DATE: return "date";
            case TIMESTAMP: return "timestamp without time zone";
            case TIMESTAMPTZ: return "timestamp with time zone";
            case BOOLEAN: return "boolean";
            default: return type.toRegtypeDisplay();
        }
    }
}
