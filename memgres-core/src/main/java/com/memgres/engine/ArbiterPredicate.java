package com.memgres.engine;

import com.memgres.engine.parser.ast.BetweenExpr;
import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.InExpr;
import com.memgres.engine.parser.ast.IsNullExpr;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.SubqueryExpr;
import com.memgres.engine.parser.ast.UnaryExpr;
import com.memgres.engine.util.Cols;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.Collator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The predicate written beside an {@code ON CONFLICT} target, and which unique index it reaches.
 *
 * <p>PostgreSQL does not look for an index whose predicate is the one that was written: it asks
 * that the index's own predicate be <em>implied</em> by it. An index that is not partial has no
 * predicate to imply, so any predicate at all arbitrates on it — {@code ON CONFLICT (i) WHERE
 * i &gt; 0} is accepted against a plain primary key — while a partial index is reached only by a
 * predicate that entails its own: {@code WHERE i &gt; 5} reaches an index built {@code WHERE
 * i &gt; 0} and {@code WHERE i &gt; -5} does not, because a row the first admits need not be a row
 * the index holds. Nothing written at all reaches no partial index, which is why a partial index
 * has to be named by a predicate before it will arbitrate.
 *
 * <p>The proof is the one PostgreSQL's own theorem prover makes. It reads both predicates as the
 * planner leaves them — a NOT written into the comparison it negates, a test against a boolean
 * constant reduced to the operand it tests, BETWEEN and IN spelled out as the conjunction and the
 * disjunction they stand for — and then asks of every part of the index's predicate whether what
 * was written entails it: because the two are the same expression, because both compare the same
 * expression against a constant and the constants nest the right way round, or because a strict
 * operator or function answers nothing at all where its argument is null, so a row a clause built
 * from them holds for is a row that has a value there. A conjunction is entailed a part at a time,
 * and a disjunction is entailed only where every branch of it entails on its own. Anything the
 * proof cannot settle counts as unproved, so an index is left unreached rather than reached on a
 * guess.
 *
 * <p>Two constants are put in order the way their own type puts them: text by the collation, which
 * reads {@code 'a'} before {@code 'A'} and both before {@code 'B'} where the codepoints read none
 * of that, and a number by its value with the arithmetic over it worked out first, because the
 * planner has worked it out before the proof ever sees the predicate — {@code i > 1 - 1} and
 * {@code i > 0} are one predicate. The type each constant's spelling gives it is kept as well: a
 * constant wider than the column makes the column the side that is cast to reach it, so
 * {@code i > 0.5} over an integer column compares {@code i::numeric}, which is a different
 * expression from the {@code i} an index built {@code WHERE i > 0} compares, and neither of them
 * says anything about the other.
 */
final class ArbiterPredicate {

    private ArbiterPredicate() {}

    /** The btree strategies a comparison can carry, in the order PostgreSQL numbers them. */
    private static final int LT = 1;
    private static final int LE = 2;
    private static final int EQ = 3;
    private static final int GE = 4;
    private static final int GT = 5;
    private static final int NE = 6;

    /**
     * How to test the two constants, indexed by the strategy of the clause and then of the index's
     * predicate: knowing {@code x CLAUSE c1}, the predicate {@code x PRED c2} follows exactly when
     * {@code c2 TEST c1} holds. A zero says no test settles it and the predicate is not proved.
     */
    private static final int[][] PROOF = {
            /* clause <  */ {GE, GE, 0, 0, 0, GE},
            /* clause <= */ {GT, GE, 0, 0, 0, GT},
            /* clause =  */ {GT, GE, EQ, LE, LT, NE},
            /* clause >= */ {0, 0, 0, LE, LT, LT},
            /* clause >  */ {0, 0, 0, LE, LE, LE},
            /* clause <> */ {0, 0, 0, 0, 0, EQ},
    };

    /**
     * Whether an index carrying this predicate of its own is reached by the one written beside the
     * conflict target.
     *
     * @param table the relation both predicates read, whose column types settle which side of a
     *              comparison the cast between two types falls on
     */
    static boolean infers(Table table, Expression written, Expression indexPredicate) {
        if (indexPredicate == null) return true;
        if (written == null) return false;
        Expression clause = normalized(written);
        // PostgreSQL folds a constant away before it proves anything, and a false one takes the
        // whole conjunction with it: what is left says nothing about any index, not even about the
        // one the other half of the conjunction named.
        List<Expression> clauses = conjuncts(clause);
        for (int i = 0; i < clauses.size(); i++) {
            if (isFalse(clauses.get(i))) return false;
        }
        return implies(table, clause, normalized(indexPredicate));
    }

    /** Whether a unique index over these columns is the one the conflict target names. */
    static boolean sameColumns(List<String> indexColumns, List<String> target) {
        if (indexColumns == null || target == null || indexColumns.size() != target.size()) {
            return false;
        }
        for (String name : target) {
            boolean found = false;
            for (String column : indexColumns) {
                if (column.equalsIgnoreCase(name)) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    }

    /**
     * Resolve the predicate against the relation being written.
     *
     * <p>PostgreSQL analyses it as it analyses an index predicate, while it plans the statement, so
     * a name nothing answers to is reported whether or not any index would have arbitrated — and it
     * is reported before the arbiter is looked for, because there is no arbiter to look for until
     * the predicate has been read. A call naming no function is settled in the same reading, where
     * the call stands: which of two faults a predicate is refused for follows from which of them
     * was written first, and reading the names through and then the calls answered for the second.
     *
     * @param written the relation as the statement named it, which is the name a qualifier reaches
     * @param alias   the name the statement gave it instead, which hides the relation's own
     */
    static void checkNames(DdlExecutor ddl, Expression predicate, Table table, String written,
                           String alias) {
        checkNamesIn(ddl, predicate, table, written, alias);
    }

    private static void checkNamesIn(DdlExecutor ddl, Object node, Table table, String written,
                                     String alias) {
        if (node == null) return;
        if (node instanceof ColumnRef) {
            checkName((ColumnRef) node, table, written, alias);
            return;
        }
        if (node instanceof FunctionCallExpr) {
            // A call is looked for by the types of its arguments, so the arguments are read first
            // and a column that is not there is reported ahead of the call standing over it.
            AstWalk.forEachChild(node, child -> checkNamesIn(ddl, child, table, written, alias));
            StoredExprNames.readCall(ddl, (FunctionCallExpr) node, table);
            return;
        }
        AstWalk.forEachChild(node, child -> checkNamesIn(ddl, child, table, written, alias));
    }

    private static void checkName(ColumnRef ref, Table table, String written, String alias) {
        String qualifier = ref.table();
        String reachable = alias != null ? alias : relationName(written, table);
        if (qualifier != null && !qualifier.equalsIgnoreCase(reachable)) {
            // A relation the statement gave an alias is in the statement under that alias only, so
            // its own name reaches an entry that is there and cannot be spoken to — which is a
            // different complaint from a name that reaches nothing at all.
            if (alias != null && qualifier.equalsIgnoreCase(relationName(written, table))) {
                MemgresException hidden = new MemgresException(
                        "invalid reference to FROM-clause entry for table \"" + qualifier + "\"",
                        "42P01");
                hidden.setHint("Perhaps you meant to reference the table alias \"" + alias + "\".");
                throw hidden;
            }
            throw new MemgresException(
                    "missing FROM-clause entry for table \"" + qualifier + "\"", "42P01");
        }
        // Every relation carries the system columns whether or not anybody declared them, so ctid
        // resolves in an index predicate exactly as it does in a query.
        if (DdlDefinitionChecks.isSystemColumnName(ref.column())) return;
        if (table.getColumnIndex(ref.column()) >= 0) return;
        if (qualifier != null) {
            throw new MemgresException("column " + qualifier.toLowerCase(Locale.ROOT) + "."
                    + ref.column() + " does not exist", "42703");
        }
        MemgresException missing =
                new MemgresException("column \"" + ref.column() + "\" does not exist", "42703");
        missing.setPositionToken(ref.column());
        throw missing;
    }

    /** The bare name the statement wrote, which is what a qualifier in the predicate reaches. */
    private static String relationName(String written, Table table) {
        String bare = written == null ? null : RelationNamespace.bareName(written);
        return bare != null ? bare : table.getName();
    }

    /** Whether an expression is the constant false, which a conjunction is worth no more than. */
    private static boolean isFalse(Expression expr) {
        return expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.BOOLEAN
                && "false".equalsIgnoreCase(((Literal) expr).value());
    }

    /** The parts of a predicate that hold one beside another, which is what AND means. */
    private static List<Expression> conjuncts(Expression expr) {
        List<Expression> parts = new ArrayList<Expression>();
        collectConjuncts(expr, parts);
        return parts;
    }

    private static void collectConjuncts(Expression expr, List<Expression> into) {
        if (expr instanceof BinaryExpr && ((BinaryExpr) expr).op() == BinaryExpr.BinOp.AND) {
            collectConjuncts(((BinaryExpr) expr).left(), into);
            collectConjuncts(((BinaryExpr) expr).right(), into);
            return;
        }
        into.add(expr);
    }

    /** The branches of a predicate that stand in for one another, which is what OR means. */
    private static List<Expression> disjuncts(Expression expr) {
        List<Expression> parts = new ArrayList<Expression>();
        collectDisjuncts(expr, parts);
        return parts;
    }

    private static void collectDisjuncts(Expression expr, List<Expression> into) {
        if (expr instanceof BinaryExpr && ((BinaryExpr) expr).op() == BinaryExpr.BinOp.OR) {
            collectDisjuncts(((BinaryExpr) expr).left(), into);
            collectDisjuncts(((BinaryExpr) expr).right(), into);
            return;
        }
        into.add(expr);
    }

    /**
     * The predicate as the planner leaves it, which is the form PostgreSQL proves things about: a
     * NOT written into the comparison it negates, a test against a boolean constant reduced to the
     * operand it tests, a constant branch of a conjunction folded away, and BETWEEN and IN spelled
     * out as the conjunction and the disjunction the grammar writes them for. Two predicates that
     * say the same thing are then the same expression, which is most of what the proof goes on.
     */
    private static Expression normalized(Expression expr) {
        if (expr instanceof UnaryExpr && ((UnaryExpr) expr).op() == UnaryExpr.UnaryOp.NOT) {
            return negated(normalized(((UnaryExpr) expr).operand()));
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr binary = (BinaryExpr) expr;
            if (binary.op() == BinaryExpr.BinOp.AND || binary.op() == BinaryExpr.BinOp.OR) {
                return combined(normalized(binary.left()), binary.op(), normalized(binary.right()));
            }
            if (binary.op() == BinaryExpr.BinOp.EQUAL
                    || binary.op() == BinaryExpr.BinOp.NOT_EQUAL) {
                Boolean right = booleanConstant(binary.right());
                if (right != null) {
                    return asserted(normalized(binary.left()), binary.op(), right.booleanValue());
                }
                Boolean left = booleanConstant(binary.left());
                if (left != null) {
                    return asserted(normalized(binary.right()), binary.op(), left.booleanValue());
                }
            }
            return expr;
        }
        if (expr instanceof BetweenExpr) {
            BetweenExpr between = (BetweenExpr) expr;
            // SYMMETRIC stands for both orders of the bounds at once, which is a disjunction of two
            // conjunctions rather than the pair of comparisons the plain form is.
            if (between.symmetric()) return expr;
            Expression pair = new BinaryExpr(
                    new BinaryExpr(between.expr(), BinaryExpr.BinOp.GREATER_EQUAL, between.low()),
                    BinaryExpr.BinOp.AND,
                    new BinaryExpr(between.expr(), BinaryExpr.BinOp.LESS_EQUAL, between.high()));
            return between.negated() ? negated(pair) : pair;
        }
        if (expr instanceof InExpr) return expanded((InExpr) expr);
        return expr;
    }

    /**
     * What a list of values stands for: a row answers a membership test where it answers one of the
     * equalities and answers a negated one where it answers every inequality. A list that is a
     * query speaks about rows no proof here can see, and an ANY over an array of its own is not a
     * list of values at all, so both are left as they were written.
     */
    private static Expression expanded(InExpr in) {
        List<Expression> values = in.values();
        if (in.fromAny() || values == null || values.isEmpty()) return in;
        Expression written = null;
        for (Expression value : values) {
            if (value instanceof SubqueryExpr) return in;
            Expression one = new BinaryExpr(in.expr(),
                    in.negated() ? BinaryExpr.BinOp.NOT_EQUAL : BinaryExpr.BinOp.EQUAL, value);
            written = written == null ? one : new BinaryExpr(written,
                    in.negated() ? BinaryExpr.BinOp.AND : BinaryExpr.BinOp.OR, one);
        }
        return written;
    }

    /** A conjunction or a disjunction over a constant, which PostgreSQL folds away. */
    private static Expression combined(Expression left, BinaryExpr.BinOp op, Expression right) {
        boolean all = op == BinaryExpr.BinOp.AND;
        if (isTrue(left)) return all ? right : left;
        if (isTrue(right)) return all ? left : right;
        if (isFalse(left)) return all ? left : right;
        if (isFalse(right)) return all ? right : left;
        return new BinaryExpr(left, op, right);
    }

    /**
     * What a comparison against a boolean constant says, which is what its operand says or the
     * opposite of it: PostgreSQL is left with no operator at all once it has read one.
     */
    private static Expression asserted(Expression operand, BinaryExpr.BinOp op, boolean constant) {
        return (op == BinaryExpr.BinOp.EQUAL) == constant ? operand : negated(operand);
    }

    /**
     * The negation PostgreSQL writes in place of a NOT. Every comparison has an operator that says
     * the opposite of it and a conjunction is negated a branch at a time, so a NOT survives the
     * folding only where nothing underneath it could take it — and what it stands over then proves
     * nothing at all, which is why {@code NOT f} reaches no index that {@code f} reaches.
     */
    private static Expression negated(Expression expr) {
        if (expr instanceof UnaryExpr && ((UnaryExpr) expr).op() == UnaryExpr.UnaryOp.NOT) {
            return ((UnaryExpr) expr).operand();
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr binary = (BinaryExpr) expr;
            BinaryExpr.BinOp opposite = opposite(binary.op());
            if (opposite != null) {
                if (binary.op() == BinaryExpr.BinOp.AND || binary.op() == BinaryExpr.BinOp.OR) {
                    return new BinaryExpr(negated(binary.left()), opposite,
                            negated(binary.right()));
                }
                return new BinaryExpr(binary.left(), opposite, binary.right());
            }
        }
        if (expr instanceof IsNullExpr) {
            IsNullExpr test = (IsNullExpr) expr;
            return new IsNullExpr(test.expr(), !test.negated());
        }
        Boolean constant = booleanConstant(expr);
        if (constant != null) return Literal.ofBoolean(!constant.booleanValue());
        return new UnaryExpr(UnaryExpr.UnaryOp.NOT, expr);
    }

    /** The operator that says the opposite of this one, or null where none of them does. */
    private static BinaryExpr.BinOp opposite(BinaryExpr.BinOp op) {
        switch (op) {
            case AND: return BinaryExpr.BinOp.OR;
            case OR: return BinaryExpr.BinOp.AND;
            case EQUAL: return BinaryExpr.BinOp.NOT_EQUAL;
            case NOT_EQUAL: return BinaryExpr.BinOp.EQUAL;
            case LESS_THAN: return BinaryExpr.BinOp.GREATER_EQUAL;
            case LESS_EQUAL: return BinaryExpr.BinOp.GREATER_THAN;
            case GREATER_THAN: return BinaryExpr.BinOp.LESS_EQUAL;
            case GREATER_EQUAL: return BinaryExpr.BinOp.LESS_THAN;
            default: return null;
        }
    }

    /** The value of a boolean constant, or null where the expression is not one. */
    private static Boolean booleanConstant(Expression expr) {
        if (!(expr instanceof Literal)
                || ((Literal) expr).literalType() != Literal.LiteralType.BOOLEAN) {
            return null;
        }
        String value = ((Literal) expr).value();
        if ("true".equalsIgnoreCase(value)) return Boolean.TRUE;
        return "false".equalsIgnoreCase(value) ? Boolean.FALSE : null;
    }

    /** Whether an expression is the constant true, which a conjunction is no narrower for. */
    private static boolean isTrue(Expression expr) {
        return Boolean.TRUE.equals(booleanConstant(expr));
    }

    /** Whether every row the clause holds for is a row the predicate holds for. */
    private static boolean implies(Table table, Expression clause, Expression predicate) {
        List<Expression> either = disjuncts(clause);
        if (either.size() > 1) {
            // A row a disjunction holds for is a row one of its branches holds for, and which one
            // is nowhere written down: the predicate follows only where each branch says it alone.
            for (int i = 0; i < either.size(); i++) {
                if (!implies(table, either.get(i), predicate)) return false;
            }
            return true;
        }
        List<Expression> parts = conjuncts(predicate);
        if (parts.size() > 1) {
            for (int i = 0; i < parts.size(); i++) {
                if (!implies(table, clause, parts.get(i))) return false;
            }
            return true;
        }
        List<Expression> alternatives = disjuncts(predicate);
        if (alternatives.size() > 1) {
            for (int i = 0; i < alternatives.size(); i++) {
                if (implies(table, clause, alternatives.get(i))) return true;
            }
            return false;
        }
        List<Expression> given = conjuncts(clause);
        if (given.size() > 1) {
            for (int i = 0; i < given.size(); i++) {
                if (implies(table, given.get(i), predicate)) return true;
            }
            return false;
        }
        return proves(table, clause, predicate);
    }

    /** The names PostgreSQL declares strict, whose answer for a null argument is null. */
    private static final Set<String> STRICT_FUNCTIONS = Cols.setOf(
            "abs", "ascii", "btrim", "ceil", "ceiling", "char_length", "character_length", "chr",
            "date_part", "date_trunc", "floor", "initcap", "left", "length", "lower", "lpad",
            "ltrim", "md5", "octet_length", "repeat", "replace", "reverse", "right", "round",
            "rpad", "rtrim", "sign", "split_part", "sqrt", "strpos", "substr", "substring",
            "to_char", "translate", "trunc", "upper");

    /** The operators PostgreSQL declares strict, which are the ones a proof runs through. */
    private static boolean strictOperator(BinaryExpr.BinOp op) {
        switch (op) {
            case EQUAL: case NOT_EQUAL: case LESS_THAN: case LESS_EQUAL:
            case GREATER_THAN: case GREATER_EQUAL:
            case ADD: case SUBTRACT: case MULTIPLY: case DIVIDE: case MODULO:
            case CONCAT: case LIKE: case ILIKE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Whether a row this clause holds for is a row where {@code operand} has a value. A strict
     * operator or function answers null wherever an argument of it is null, and a null answer is no
     * answer at all to a predicate, so a clause built out of nothing but strict things holding for
     * a row is proof that everything underneath it had a value. COALESCE and CASE answer for a row
     * whose column is null, so a clause written over one of them proves nothing about it.
     */
    private static boolean strictFor(Expression clause, Expression operand) {
        if (clause == null || operand == null) return false;
        if (sameExpression(clause, operand)) return true;
        // A cast is a function like any other and each of them is strict, so a column written as
        // another type says as much about the column as the column itself does.
        if (clause instanceof CastExpr) return strictFor(((CastExpr) clause).expr(), operand);
        if (clause instanceof BinaryExpr) {
            BinaryExpr binary = (BinaryExpr) clause;
            return strictOperator(binary.op())
                    && (strictFor(binary.left(), operand) || strictFor(binary.right(), operand));
        }
        if (clause instanceof UnaryExpr) {
            UnaryExpr unary = (UnaryExpr) clause;
            return (unary.op() == UnaryExpr.UnaryOp.NEGATE
                    || unary.op() == UnaryExpr.UnaryOp.POSITIVE
                    || unary.op() == UnaryExpr.UnaryOp.ABS
                    || unary.op() == UnaryExpr.UnaryOp.SQRT
                    || unary.op() == UnaryExpr.UnaryOp.CBRT)
                    && strictFor(unary.operand(), operand);
        }
        if (clause instanceof FunctionCallExpr) {
            FunctionCallExpr call = (FunctionCallExpr) clause;
            if (call.name() == null || call.args() == null) return false;
            if (!STRICT_FUNCTIONS.contains(call.name().toLowerCase(Locale.ROOT))) return false;
            for (Expression arg : call.args()) {
                if (strictFor(arg, operand)) return true;
            }
        }
        return false;
    }

    /** Whether one clause on its own entails one part of the index's predicate. */
    private static boolean proves(Table table, Expression clause, Expression predicate) {
        if (sameExpression(clause, predicate)) return true;
        // A comparison answers nothing at all where its operand is null, so a row it holds for has
        // a value there: that is the whole proof of IS NOT NULL, which is the predicate a great
        // many partial indexes are built with. Nothing else proves one, which is why a predicate
        // asking for a null is reached by nothing at all.
        if (predicate instanceof IsNullExpr) {
            IsNullExpr test = (IsNullExpr) predicate;
            return test.negated() && strictFor(clause, test.expr());
        }
        if (!(clause instanceof BinaryExpr) || !(predicate instanceof BinaryExpr)) return false;
        Comparison left = Comparison.of((BinaryExpr) clause);
        Comparison right = Comparison.of((BinaryExpr) predicate);
        if (left == null || right == null || !sameExpression(left.subject, right.subject)
                || !overOneExpression(table, left, right)) {
            return false;
        }
        int test = PROOF[left.strategy - 1][right.strategy - 1];
        if (test == 0) return false;
        Integer order = compare(right.constant.value, left.constant.value);
        if (order == null) return false;
        switch (test) {
            case LT: return order < 0;
            case LE: return order <= 0;
            case EQ: return order == 0;
            case GE: return order >= 0;
            case GT: return order > 0;
            default: return order != 0;
        }
    }

    /** One expression compared against one constant, whichever side each was written on. */
    private static final class Comparison {
        private final Expression subject;
        private final Constant constant;
        private final int strategy;

        private Comparison(Expression subject, Constant constant, int strategy) {
            this.subject = subject;
            this.constant = constant;
            this.strategy = strategy;
        }

        static Comparison of(BinaryExpr expr) {
            int strategy = strategyOf(expr.op());
            if (strategy == 0) return null;
            Constant constant = constantOf(expr.right());
            if (constant != null && constantOf(expr.left()) == null) {
                return new Comparison(expr.left(), constant, strategy);
            }
            // Written the other way round it says the same thing about the same expression, which
            // is what the operator's commutator is for.
            constant = constantOf(expr.left());
            if (constant == null || constantOf(expr.right()) != null) return null;
            return new Comparison(expr.right(), constant, commuted(strategy));
        }
    }

    /** A value a comparison is written against, and how wide the type of its spelling is. */
    private static final class Constant {
        private final Object value;
        private final int width;

        private Constant(Object value, int width) {
            this.value = value;
            this.width = width;
        }
    }

    /**
     * How wide a type is among the numbers, which is what settles whether a comparison casts the
     * column or the constant. A constant PostgreSQL reads as unknown — a string nobody named a type
     * for — takes the type of whatever it is compared with and so is written below all of them, and
     * a type that is no number at all cannot be placed beside another.
     */
    private static final int UNPLACED_TYPE = -2;
    private static final int UNKNOWN_TYPE = -1;
    private static final int INT_TYPE = 0;
    private static final int NUMERIC_TYPE = 1;
    private static final int FLOAT_TYPE = 2;

    /**
     * Whether the two comparisons are written over one expression once their types are settled.
     * PostgreSQL reads a comparison with the operator its two sides resolve to, and where the
     * constant is of a wider type than the column it is the column that is cast to reach it: an
     * index built {@code WHERE i > 0} over an integer column compares {@code i}, a statement
     * writing {@code i > 0.5} compares {@code i::numeric}, and neither is a bound on the other's
     * expression. Where the column is the wider of the two the constant is what moves and both
     * comparisons are over the column itself, which is why every integer bound proves about a
     * numeric column and each of {@code smallint}, {@code integer} and {@code bigint} proves about
     * an integer one.
     */
    private static boolean overOneExpression(Table table, Comparison left, Comparison right) {
        if (left.constant.width == right.constant.width) return true;
        int column = columnWidth(table, left.subject);
        if (column == UNPLACED_TYPE) return false;
        return Math.max(column, left.constant.width) == Math.max(column, right.constant.width);
    }

    /** How wide the column a comparison is written over is, or unplaced where this cannot say. */
    private static int columnWidth(Table table, Expression subject) {
        if (table == null || !(subject instanceof ColumnRef)) return UNPLACED_TYPE;
        String name = ((ColumnRef) subject).column();
        for (Column column : table.getColumns()) {
            if (column.getName() != null && column.getName().equalsIgnoreCase(name)) {
                return typeWidth(column.getType());
            }
        }
        return UNPLACED_TYPE;
    }

    private static int typeWidth(DataType type) {
        if (type == null) return UNPLACED_TYPE;
        switch (type) {
            case SMALLINT: case INTEGER: case BIGINT:
            case SMALLSERIAL: case SERIAL: case BIGSERIAL:
                return INT_TYPE;
            case NUMERIC: return NUMERIC_TYPE;
            case REAL: case DOUBLE_PRECISION: return FLOAT_TYPE;
            default: return UNPLACED_TYPE;
        }
    }

    /**
     * How wide the type a cast names is. A cast to anything but a number is read as the unknown
     * width: it casts no numeric column, and two constants carrying it stand beside one another
     * exactly as two constants carrying none do.
     */
    private static int typeWidth(String typeName) {
        if (typeName == null) return UNKNOWN_TYPE;
        String name = typeName.trim().toLowerCase(Locale.ROOT);
        int paren = name.indexOf('(');
        if (paren >= 0) name = name.substring(0, paren).trim();
        if (name.equals("smallint") || name.equals("int2") || name.equals("int")
                || name.equals("integer") || name.equals("int4") || name.equals("bigint")
                || name.equals("int8")) {
            return INT_TYPE;
        }
        if (name.equals("numeric") || name.equals("decimal")) return NUMERIC_TYPE;
        if (name.equals("real") || name.equals("float4") || name.equals("float")
                || name.equals("double precision") || name.equals("float8")) {
            return FLOAT_TYPE;
        }
        return UNKNOWN_TYPE;
    }

    private static int strategyOf(BinaryExpr.BinOp op) {
        switch (op) {
            case LESS_THAN: return LT;
            case LESS_EQUAL: return LE;
            case EQUAL: return EQ;
            case GREATER_EQUAL: return GE;
            case GREATER_THAN: return GT;
            case NOT_EQUAL: return NE;
            default: return 0;
        }
    }

    private static int commuted(int strategy) {
        switch (strategy) {
            case LT: return GT;
            case LE: return GE;
            case GE: return LE;
            case GT: return LT;
            default: return strategy;
        }
    }

    /**
     * The value an operand stands for without a row to read, or null where it stands for something
     * only a row can settle. Arithmetic over constants is worked out here because the planner has
     * worked it out before the proof sees either predicate, and a cast is worked through because
     * PostgreSQL folds one over a constant away in the same pass — but the type the cast names is
     * kept, because that is what settles which side of the comparison a cast falls on.
     */
    private static Constant constantOf(Expression expr) {
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            Constant inner = constantOf(cast.expr());
            if (inner == null) return null;
            int width = typeWidth(cast.typeName());
            Object value = inner.value;
            // A number narrowed to an integer type is rounded away from zero, as PostgreSQL rounds
            // it, so the bound the comparison carries is the whole one it was narrowed to.
            if (width == INT_TYPE && value instanceof BigDecimal) {
                value = ((BigDecimal) value).setScale(0, RoundingMode.HALF_UP);
            }
            return new Constant(value, width);
        }
        if (expr instanceof UnaryExpr) {
            UnaryExpr unary = (UnaryExpr) expr;
            Constant operand = constantOf(unary.operand());
            if (operand == null || !(operand.value instanceof BigDecimal)) return null;
            if (unary.op() == UnaryExpr.UnaryOp.NEGATE) {
                return new Constant(((BigDecimal) operand.value).negate(), operand.width);
            }
            return unary.op() == UnaryExpr.UnaryOp.POSITIVE ? operand : null;
        }
        if (expr instanceof BinaryExpr) return folded((BinaryExpr) expr);
        if (!(expr instanceof Literal)) return null;
        Literal literal = (Literal) expr;
        if (literal.literalType() == Literal.LiteralType.INTEGER) {
            BigDecimal value = number(literal.value());
            return value == null ? null : new Constant(value, INT_TYPE);
        }
        if (literal.literalType() == Literal.LiteralType.FLOAT) {
            // A number written with a point or an exponent is numeric to PostgreSQL rather than a
            // float, and numeric is wider than an integer column: it casts one.
            BigDecimal value = number(literal.value());
            return value == null ? null : new Constant(value, NUMERIC_TYPE);
        }
        return literal.literalType() == Literal.LiteralType.STRING
                ? new Constant(literal.value(), UNKNOWN_TYPE) : null;
    }

    /**
     * The value an arithmetic expression over two constants stands for. Integers divide as integers
     * and throw away what is left over, which is the answer PostgreSQL folds; a division of wider
     * numbers that does not come out exactly is left unsettled rather than answered to a scale
     * PostgreSQL did not pick, and so is one by zero, which is no constant at all.
     */
    private static Constant folded(BinaryExpr expr) {
        Constant left = constantOf(expr.left());
        Constant right = constantOf(expr.right());
        if (left == null || right == null) return null;
        if (!(left.value instanceof BigDecimal) || !(right.value instanceof BigDecimal)) return null;
        if (left.width < INT_TYPE || right.width < INT_TYPE) return null;
        BigDecimal one = (BigDecimal) left.value;
        BigDecimal other = (BigDecimal) right.value;
        int width = Math.max(left.width, right.width);
        BigDecimal answer;
        try {
            switch (expr.op()) {
                case ADD: answer = one.add(other); break;
                case SUBTRACT: answer = one.subtract(other); break;
                case MULTIPLY: answer = one.multiply(other); break;
                case DIVIDE: answer = width == INT_TYPE
                        ? one.divideToIntegralValue(other) : one.divide(other); break;
                case MODULO: answer = one.remainder(other); break;
                default: return null;
            }
        } catch (ArithmeticException e) {
            return null;
        }
        return new Constant(withinItsType(one, other, answer, width), width);
    }

    /**
     * The answer an integer fold came to, once it has been held to the width its operands had.
     *
     * <p>PostgreSQL works a constant out in the type its operands are, and an integer literal is as
     * narrow as it fits: two that fit in an {@code integer} are added as integers, and a sum that
     * does not fit is out of range rather than a step up to a wider type. Folding into a number
     * with no width at all answered a predicate PostgreSQL never got as far as reading — it raises
     * while it plans, before the arbiter is looked for — and a statement it refuses was accepted.
     */
    private static BigDecimal withinItsType(BigDecimal one, BigDecimal other, BigDecimal answer,
                                            int width) {
        if (width != INT_TYPE) return answer;
        boolean wide = !fitsInteger(one) || !fitsInteger(other);
        if (!wide && !fitsInteger(answer)) {
            throw new MemgresException("integer out of range", "22003");
        }
        if (wide && answer.abs().compareTo(LONG_LIMIT) > 0) {
            throw new MemgresException("bigint out of range", "22003");
        }
        return answer;
    }

    /** Whether a whole number is one an {@code integer} holds. */
    private static boolean fitsInteger(BigDecimal value) {
        return value.compareTo(INT_FLOOR) >= 0 && value.compareTo(INT_CEILING) <= 0;
    }

    private static final BigDecimal INT_FLOOR = BigDecimal.valueOf(Integer.MIN_VALUE);
    private static final BigDecimal INT_CEILING = BigDecimal.valueOf(Integer.MAX_VALUE);
    private static final BigDecimal LONG_LIMIT = BigDecimal.valueOf(Long.MAX_VALUE);

    /** Which of two constants is the greater, or null where nothing here can say. */
    private static Integer compare(Object left, Object right) {
        if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return Integer.valueOf(((BigDecimal) left).compareTo((BigDecimal) right));
        }
        if (left instanceof String && right instanceof String) {
            if (left.equals(right)) return Integer.valueOf(0);
            // Which of two texts is the greater is the collation's answer, and the collation this
            // sorts by is a linguistic one: it reads a letter beside the other case of itself and
            // both before the next letter, so 'a' comes before 'A' and both before 'B', which is
            // no order the codepoints are in. PostgreSQL falls back on the codepoints only where
            // the collation calls two spellings alike, which is what keeps its ordering total.
            int words = Collator.getInstance(Locale.ROOT).compare(left, right);
            int bytes = ((String) left).compareTo((String) right);
            if (lettersAndDigits((String) left) && lettersAndDigits((String) right)) {
                return Integer.valueOf(words != 0 ? words : bytes);
            }
            // Collations agree about letters and digits and part company over what a space or a
            // punctuation mark weighs, so a text carrying one of those is settled only where the
            // codepoints answer as the collation did, and is left unproved where they do not.
            if (words == 0 || (bytes < 0) != (words < 0)) return null;
            return Integer.valueOf(bytes < 0 ? -1 : 1);
        }
        BigDecimal one = left instanceof BigDecimal ? (BigDecimal) left : number((String) left);
        BigDecimal other = right instanceof BigDecimal ? (BigDecimal) right : number((String) right);
        return one == null || other == null ? null : Integer.valueOf(one.compareTo(other));
    }

    /** Whether a text is made of nothing but letters and digits, which every collation agrees on. */
    private static boolean lettersAndDigits(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (!Character.isLetterOrDigit(text.charAt(i))) return false;
        }
        return true;
    }

    private static BigDecimal number(String text) {
        if (text == null) return null;
        try {
            return new BigDecimal(text.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Whether two expressions are the same one. An index predicate reads a single relation, so a
     * qualifier on a column only says again which relation it is and two spellings of one column
     * are one expression.
     */
    private static boolean sameExpression(Expression left, Expression right) {
        if (left == null || right == null) return left == right;
        if (left instanceof ColumnRef && right instanceof ColumnRef) {
            return ((ColumnRef) left).column().equalsIgnoreCase(((ColumnRef) right).column());
        }
        if (left.getClass() != right.getClass()) return false;
        if (left instanceof BinaryExpr) {
            BinaryExpr one = (BinaryExpr) left;
            BinaryExpr other = (BinaryExpr) right;
            return one.op() == other.op() && sameExpression(one.left(), other.left())
                    && sameExpression(one.right(), other.right());
        }
        if (left instanceof UnaryExpr) {
            UnaryExpr one = (UnaryExpr) left;
            UnaryExpr other = (UnaryExpr) right;
            return one.op() == other.op() && sameExpression(one.operand(), other.operand());
        }
        if (left instanceof CastExpr) {
            CastExpr one = (CastExpr) left;
            CastExpr other = (CastExpr) right;
            return one.typeName().equalsIgnoreCase(other.typeName())
                    && sameExpression(one.expr(), other.expr());
        }
        if (left instanceof IsNullExpr) {
            IsNullExpr one = (IsNullExpr) left;
            IsNullExpr other = (IsNullExpr) right;
            return one.negated() == other.negated() && sameExpression(one.expr(), other.expr());
        }
        if (left instanceof FunctionCallExpr) {
            FunctionCallExpr one = (FunctionCallExpr) left;
            FunctionCallExpr other = (FunctionCallExpr) right;
            if (!one.name().equalsIgnoreCase(other.name())) return false;
            List<Expression> ours = one.args();
            List<Expression> theirs = other.args();
            if (ours == null || theirs == null) return ours == theirs;
            if (ours.size() != theirs.size()) return false;
            for (int i = 0; i < ours.size(); i++) {
                if (!sameExpression(ours.get(i), theirs.get(i))) return false;
            }
            return true;
        }
        return left.equals(right);
    }

}
