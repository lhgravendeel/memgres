package com.memgres.engine;

import com.memgres.engine.parser.ast.AnyAllArrayExpr;
import com.memgres.engine.parser.ast.AtTimeZoneExpr;
import com.memgres.engine.parser.ast.NamedArgExpr;
import com.memgres.engine.parser.ast.SubscriptExpr;
import com.memgres.engine.parser.ast.ArrayExpr;
import com.memgres.engine.parser.ast.BetweenExpr;
import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.CaseExpr;
import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.CollateExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.CustomOperatorExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.InExpr;
import com.memgres.engine.parser.ast.IsBooleanExpr;
import com.memgres.engine.parser.ast.IsNullExpr;
import com.memgres.engine.parser.ast.LikeExpr;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.UnaryExpr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deparses expression ASTs the way PostgreSQL's {@code ruleutils.c} does, for
 * {@code pg_get_constraintdef()}, {@code pg_get_indexdef()} and
 * {@code information_schema.check_constraints.check_clause}.
 *
 * <p>PostgreSQL never echoes the SQL text that was typed. It deparses the
 * <em>post-parse-analysis</em> node tree, in which every implicit cast the analyzer
 * inserted is present and every constant carries a resolved type. That is why
 * {@code CHECK (price >= 0)} on a {@code numeric} column comes back as
 * {@code CHECK ((price >= (0)::numeric))} while the same text on an {@code integer}
 * column comes back unchanged. Reproducing that output therefore needs two things,
 * both of which live in this class:
 *
 * <ol>
 *   <li>a type-resolution pass ({@link #typeOf}) that mirrors the subset of PG's
 *       {@code parse_coerce.c} / operator-resolution rules reachable from CHECK and
 *       index expressions, so we know which implicit casts PG would have added; and</li>
 *   <li>a renderer ({@link #render}) that follows {@code get_rule_expr} /
 *       {@code get_const_expr} / {@code get_coercion_expr} — notably the unconditional
 *       parenthesisation used when {@code prettyFlags == 0}, and the rule that decides
 *       whether a constant is printed bare or as {@code 'value'::type}.</li>
 * </ol>
 *
 * <p>When a type cannot be resolved the renderer simply omits the cast rather than
 * guessing, so an unhandled construct degrades to the pre-existing output instead of
 * producing something wrong.
 */
public final class RuleDeparser {

    private RuleDeparser() {
    }

    // ------------------------------------------------------------------
    // Resolved types
    // ------------------------------------------------------------------

    /** A resolved PostgreSQL type: a built-in {@link DataType}, or a named user type. */
    public static final class PgType {
        final DataType dt;
        final String custom;   // enum / domain / composite type name (dt is null)
        final DataType elem;   // element type when this is an array type
        final int typmod;      // -1 when none

        private PgType(DataType dt, String custom, DataType elem, int typmod) {
            this.dt = dt;
            this.custom = custom;
            this.elem = elem;
            this.typmod = typmod;
        }

        public static PgType of(DataType dt) {
            return new PgType(dt, null, null, -1);
        }

        public static PgType of(DataType dt, int typmod) {
            return new PgType(dt, null, null, typmod);
        }

        public static PgType custom(String name) {
            return new PgType(null, name, null, -1);
        }

        public static PgType arrayOf(DataType elem) {
            return new PgType(null, null, elem, -1);
        }

        boolean sameAs(PgType o) {
            if (o == null) return false;
            if (custom != null || o.custom != null) return custom != null && custom.equalsIgnoreCase(o.custom);
            if (elem != null || o.elem != null) return elem == o.elem;
            return dt == o.dt;
        }
    }

    /** Resolves a column name to its type; returns null for unknown columns. */
    public interface ColumnTypes {
        PgType typeOf(String columnName);
    }

    /** Column-type lookup backed by a table's column list. */
    public static ColumnTypes forTable(final Table table) {
        final Map<String, PgType> byName = new HashMap<String, PgType>();
        if (table != null) {
            for (Column c : table.getColumns()) {
                byName.put(c.getName().toLowerCase(), fromColumn(c));
            }
        }
        return new ColumnTypes() {
            @Override
            public PgType typeOf(String columnName) {
                return columnName == null ? null : byName.get(columnName.toLowerCase());
            }
        };
    }

    /** Column-type lookup for a domain CHECK, where the only "column" is {@code VALUE}. */
    public static ColumnTypes forDomain(final PgType valueType) {
        return new DomainValue(valueType);
    }

    /**
     * The lookup a domain CHECK is read against. PostgreSQL writes the domain's own placeholder
     * back as the keyword {@code VALUE}, so the renderer has to be able to tell that placeholder
     * from an ordinary relation column that happens to be called value — which PostgreSQL writes
     * in lower case like any other name.
     */
    private static final class DomainValue implements ColumnTypes {
        private final PgType valueType;

        DomainValue(PgType valueType) {
            this.valueType = valueType;
        }

        @Override
        public PgType typeOf(String columnName) {
            return "value".equalsIgnoreCase(columnName) ? valueType : null;
        }
    }

    /** A lookup that resolves nothing; casts are then never inserted. */
    public static final ColumnTypes NO_COLUMNS = new ColumnTypes() {
        @Override
        public PgType typeOf(String columnName) {
            return null;
        }
    };

    static PgType fromColumn(Column c) {
        if (c.getArrayElementType() != null) return PgType.arrayOf(c.getArrayElementType());
        if (c.getEnumTypeName() != null) return PgType.custom(c.getEnumTypeName());
        if (c.getDomainTypeName() != null) return PgType.custom(c.getDomainTypeName());
        if (c.getCompositeTypeName() != null) return PgType.custom(c.getCompositeTypeName());
        return PgType.of(c.getType());
    }

    // ------------------------------------------------------------------
    // Public entry points
    // ------------------------------------------------------------------

    /**
     * Deparses a boolean expression the way PG renders a CHECK body or an index
     * predicate: {@code (price >= (0)::numeric)}.
     */
    public static String deparse(Expression e, ColumnTypes cols) {
        if (e == null) return "true";
        try {
            return render(e, null, cols == null ? NO_COLUMNS : cols, false);
        } catch (RuntimeException ex) {
            return SqlUnparser.exprToSql(e);
        }
    }

    /**
     * Deparses a value in a context that wants a particular type — a VALUES item written against
     * a known column, say — so that an untyped literal is printed in that type's own form.
     */
    static String deparseValue(Expression e, PgType target, ColumnTypes cols) {
        if (e == null) return "NULL";
        try {
            return render(e, target, cols == null ? NO_COLUMNS : cols, false);
        } catch (RuntimeException ex) {
            return SqlUnparser.exprToSql(e);
        }
    }

    /**
     * Deparses one index key expression, applying PG's {@code looks_like_function} rule:
     * a bare function call is printed as-is, anything else is wrapped in parentheses
     * (so {@code lower(name)} but {@code ((qty + 1))}).
     */
    public static String deparseIndexElement(Expression e, ColumnTypes cols) {
        String body = deparse(e, cols);
        return looksLikeFunction(e) ? body : "(" + body + ")";
    }

    private static boolean looksLikeFunction(Expression e) {
        // AT TIME ZONE is one of the calls SQL gives a syntax of its own, and PostgreSQL's rule
        // admits those as readily as an ordinary call: the spelling already carries parentheses
        // of its own, so a second pair would be one pair too many.
        if (e instanceof AtTimeZoneExpr) return true;
        if (!(e instanceof FunctionCallExpr)) return false;
        String n = ((FunctionCallExpr) e).name();
        // XMLSERIALIZE produces text and is then coerced to whatever type the key asked for, so a
        // key asking for anything but text has that coercion sitting on top of the call and is
        // parenthesised for it.
        if ("xmlserialize".equalsIgnoreCase(n)) return xmlSerializeType((FunctionCallExpr) e) == null;
        // A cast written in function syntax deparses as a cast, not a call.
        return n != null && !isTypeName(n);
    }

    /**
     * The type an XMLSERIALIZE was asked for, when it is one the call answers with on its own —
     * null when it is text, which is what the call already produces.
     */
    private static PgType xmlSerializeType(FunctionCallExpr fn) {
        List<Expression> args = fn.args();
        if (args == null || args.size() < 3 || !(args.get(2) instanceof Literal)) return null;
        PgType asked = parseTypeName(((Literal) args.get(2)).value());
        return asked != null && asked.dt == DataType.TEXT ? null : asked;
    }

    // ------------------------------------------------------------------
    // Type resolution (the parse_coerce.c subset)
    // ------------------------------------------------------------------

    private static final int CAT_UNKNOWN = 0;
    private static final int CAT_INT = 1;
    private static final int CAT_NUMERIC = 2;
    private static final int CAT_FLOAT = 3;
    private static final int CAT_STRING = 4;
    private static final int CAT_OTHER = 5;

    private static int category(PgType t) {
        if (t == null) return CAT_UNKNOWN;
        if (t.custom != null || t.elem != null || t.dt == null) return CAT_OTHER;
        switch (t.dt) {
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case SERIAL:
            case BIGSERIAL:
            case SMALLSERIAL:
                return CAT_INT;
            case NUMERIC:
                return CAT_NUMERIC;
            case REAL:
            case DOUBLE_PRECISION:
                return CAT_FLOAT;
            case TEXT:
            case VARCHAR:
            case CHAR:
            case NAME:
                return CAT_STRING;
            default:
                return CAT_OTHER;
        }
    }

    /**
     * PG's grammar folds a unary minus applied to a numeric literal straight into the
     * constant, so {@code > -1} is a negative Const rather than a negation node.
     */
    private static Expression fold(Expression e) {
        if (e instanceof UnaryExpr) {
            UnaryExpr u = (UnaryExpr) e;
            if (u.op() == UnaryExpr.UnaryOp.NEGATE && u.operand() instanceof Literal) {
                Literal l = (Literal) u.operand();
                if (l.literalType() == Literal.LiteralType.INTEGER
                        || l.literalType() == Literal.LiteralType.FLOAT) {
                    return new Literal(l.literalType(), "-" + l.value());
                }
            }
        }
        return e;
    }

    /** Natural (pre-coercion) type of an expression, or null when it cannot be resolved. */
    static PgType typeOf(Expression expr, ColumnTypes cols) {
        Expression e = fold(expr);
        if (e == null) return null;
        if (e instanceof Literal) return literalType((Literal) e);
        if (e instanceof ColumnRef) return cols.typeOf(((ColumnRef) e).column());
        if (e instanceof CastExpr) return parseTypeName(((CastExpr) e).typeName());
        if (e instanceof CollateExpr) return typeOf(((CollateExpr) e).expr(), cols);
        if (e instanceof NamedArgExpr) return typeOf(((NamedArgExpr) e).value(), cols);
        if (e instanceof AtTimeZoneExpr) return atTimeZoneType(typeOf(((AtTimeZoneExpr) e).expr(), cols));
        if (e instanceof UnaryExpr) {
            UnaryExpr u = (UnaryExpr) e;
            if (u.op() == UnaryExpr.UnaryOp.NOT) return PgType.of(DataType.BOOLEAN);
            return typeOf(u.operand(), cols);
        }
        if (e instanceof IsNullExpr || e instanceof IsBooleanExpr
                || e instanceof LikeExpr || e instanceof BetweenExpr || e instanceof InExpr
                || e instanceof AnyAllArrayExpr) {
            return PgType.of(DataType.BOOLEAN);
        }
        if (e instanceof BinaryExpr) {
            BinaryExpr b = (BinaryExpr) e;
            if (isBooleanResult(b.op())) return PgType.of(DataType.BOOLEAN);
            PgType json = jsonResultType(b.op(), typeOf(b.left(), cols));
            if (json != null) return json;
            if (b.op() == BinaryExpr.BinOp.CONCAT) return PgType.of(DataType.TEXT);
            PgType[] tg = operandTargets(b.op(), typeOf(b.left(), cols), typeOf(b.right(), cols));
            return tg[0] != null ? tg[0] : tg[1];
        }
        if (e instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) e;
            PgType t = null;
            for (CaseExpr.WhenClause w : c.whenClauses()) {
                t = unify(t, typeOf(w.result(), cols));
            }
            return unify(t, typeOf(c.elseExpr(), cols));
        }
        if (e instanceof FunctionCallExpr) {
            return functionReturnType((FunctionCallExpr) e, cols);
        }
        return null;
    }

    /**
     * The type AT TIME ZONE answers with. It moves a value across the boundary the zone marks:
     * a value that carried a zone loses it and one that carried none gains it, which is why the
     * same clause reads a timestamptz out of a timestamp and a timestamp out of a timestamptz.
     */
    private static PgType atTimeZoneType(PgType inner) {
        if (inner == null || inner.dt == null) return null;
        switch (inner.dt) {
            case TIMESTAMP:
                return PgType.of(DataType.TIMESTAMPTZ);
            case TIMESTAMPTZ:
                return PgType.of(DataType.TIMESTAMP);
            case TIME:
            case TIMETZ:
                return PgType.of(DataType.TIMETZ);
            default:
                return null;
        }
    }

    private static boolean isBooleanResult(BinaryExpr.BinOp op) {
        switch (op) {
            case EQUAL:
            case NOT_EQUAL:
            case LESS_THAN:
            case GREATER_THAN:
            case LESS_EQUAL:
            case GREATER_EQUAL:
            case AND:
            case OR:
            case LIKE:
            case ILIKE:
            case SIMILAR_TO:
            case REGEX_MATCH:
            case REGEX_IMATCH:
            case NOT_REGEX_MATCH:
            case NOT_REGEX_IMATCH:
            case IS_DISTINCT_FROM:
            case IS_NOT_DISTINCT_FROM:
            case CONTAINS:
            case CONTAINED_BY:
            case OVERLAP:
            case TS_MATCH:
            case JSONB_EXISTS:
            case JSONB_EXISTS_ANY:
            case JSONB_EXISTS_ALL:
                return true;
            default:
                return false;
        }
    }

    private static PgType literalType(Literal lit) {
        switch (lit.literalType()) {
            case INTEGER:
                return PgType.of(integerLiteralType(lit.value()));
            case FLOAT:
                return PgType.of(DataType.NUMERIC);
            case BIT_STRING:
                // A bit-string literal is a constant of the bit type whatever reads it, which is
                // what makes a comparison with a bit varying resolve to bit varying's operator.
                return PgType.of(DataType.BIT);
            case BOOLEAN:
                return PgType.of(DataType.BOOLEAN);
            default:
                // String and NULL literals are of PG's "unknown" type until coerced.
                return null;
        }
    }

    /** PG's scanner types an integer literal int4, else int8, else numeric. */
    private static DataType integerLiteralType(String v) {
        try {
            BigInteger bi = new BigInteger(v.trim());
            if (bi.bitLength() < 32) return DataType.INTEGER;
            if (bi.bitLength() < 64) return DataType.BIGINT;
            return DataType.NUMERIC;
        } catch (NumberFormatException ex) {
            return DataType.INTEGER;
        }
    }

    /** PG's {@code select_common_type}, restricted to the numeric and string categories. */
    private static PgType unify(PgType a, PgType b) {
        if (a == null) return b;
        if (b == null) return a;
        if (a.sameAs(b)) return a;
        int ac = category(a), bc = category(b);
        if (numericRank(a) > 0 && numericRank(b) > 0) {
            return numericRank(a) >= numericRank(b) ? a : b;
        }
        if (ac == CAT_STRING && bc == CAT_STRING) return PgType.of(DataType.TEXT);
        return a;
    }

    private static int numericRank(PgType t) {
        if (category(t) != CAT_INT && category(t) != CAT_NUMERIC && category(t) != CAT_FLOAT) return 0;
        switch (t.dt) {
            case SMALLINT:
            case SMALLSERIAL:
                return 1;
            case INTEGER:
            case SERIAL:
                return 2;
            case BIGINT:
            case BIGSERIAL:
                return 3;
            case NUMERIC:
                return 4;
            case REAL:
                return 5;
            default:
                return 6;
        }
    }

    /**
     * The types PG's operator resolution settles on for the two operands. Both slots are
     * always filled; the renderer emits a cast only where the resolved type differs from
     * the operand's own type.
     */
    private static PgType[] operandTargets(BinaryExpr.BinOp op, PgType lt, PgType rt) {
        if (lt == null && rt == null) return new PgType[]{null, null};
        // An untyped literal takes the type of its sibling before resolution runs.
        if (lt == null) lt = rt;
        PgType json = jsonOperandTarget(op, lt, rt);
        if (json != null) return new PgType[]{lt, json};
        if (rt == null) rt = lt;

        PgType text = PgType.of(DataType.TEXT);

        // LIKE/regex/SIMILAR TO are text-only operators.
        if (stringOperator(op)) return new PgType[]{text, text};

        if (op == BinaryExpr.BinOp.CONCAT) {
            if (concatenable(lt) && concatenable(rt)) return new PgType[]{text, text};
            return new PgType[]{lt, rt};
        }

        // "^" exists only for float8 and numeric, so two integers promote to float8.
        if (op == BinaryExpr.BinOp.POWER && category(lt) == CAT_INT && category(rt) == CAT_INT) {
            PgType f8 = PgType.of(DataType.DOUBLE_PRECISION);
            return new PgType[]{f8, f8};
        }

        // Everything else is settled by the entry of pg_operator the two types resolve to, which
        // is what puts inet's operator between a cidr and an inet and bit varying's between a bit
        // varying and a bit. The rules below stand in wherever that leaves the choice open.
        PgType[] resolved = resolvedTargets(op, lt, rt);
        if (resolved != null) return resolved;

        PgType[] str = stringTargets(lt, rt);
        if (str != null) return str;

        if (lt.sameAs(rt)) return new PgType[]{lt, rt};

        PgType[] num = numericTargets(lt, rt);
        if (num != null) return num;

        return new PgType[]{lt, rt};
    }

    /**
     * The types the operator this spelling resolves to reads its operands as, or null where
     * neither type is one pg_operator is written over or the choice between its entries is not
     * settled. A type carrying a width keeps the one it was declared with, since the conversion
     * PostgreSQL leaves out is the one that would have nothing to do.
     */
    private static PgType[] resolvedTargets(BinaryExpr.BinOp op, PgType lt, PgType rt) {
        if (lt.dt == null || rt.dt == null) return null;
        int[] readAs = OperandTypes.forOperator(operatorText(op), lt.dt.getOid(), rt.dt.getOid());
        if (readAs == null) return null;
        DataType left = DataType.fromOid(readAs[0]);
        DataType right = DataType.fromOid(readAs[1]);
        if (left == null || right == null) return null;
        return new PgType[]{left == lt.dt ? lt : PgType.of(left),
                right == rt.dt ? rt : PgType.of(right)};
    }

    /**
     * What a json operator answers with, or null where the operator is not one of them. The two
     * arrows that end in a second {@code >} read the member out as text; the rest hand back a
     * document, which is why an index over one of them indexes json and an index over the other
     * indexes text.
     */
    private static PgType jsonResultType(BinaryExpr.BinOp op, PgType lt) {
        if (lt == null || (lt.dt != DataType.JSON && lt.dt != DataType.JSONB)) return null;
        switch (op) {
            case JSON_ARROW_TEXT:
            case JSON_HASH_ARROW_TEXT:
                return PgType.of(DataType.TEXT);
            case JSON_ARROW:
            case JSON_SUBSCRIPT:
            case JSON_HASH_ARROW:
            case SUBTRACT:
            case CONCAT:
                return lt;
            default:
                return null;
        }
    }

    /**
     * What the right-hand side of a json operator resolves to, or null where the operator is not
     * one of them.
     *
     * <p>These operators are declared over the type they take rather than over the document they
     * read, so a constant written beside one does not become another document: {@code j -> 'k'}
     * names the member called k, which is a text key, and {@code j #> '{a}'} walks a path, which
     * is an array of them. Taking the type from the left operand instead wrote a key down as a
     * document, which is not what the operator was resolved against.
     */
    private static PgType jsonOperandTarget(BinaryExpr.BinOp op, PgType lt, PgType rt) {
        if (lt == null || (lt.dt != DataType.JSON && lt.dt != DataType.JSONB)) return null;
        switch (op) {
            case JSON_ARROW:
            case JSON_SUBSCRIPT:
            case JSON_ARROW_TEXT:
            case SUBTRACT:
                // A number picks an element out of an array, and there is an operator for that.
                return category(rt) == CAT_INT ? rt : PgType.of(DataType.TEXT);
            case JSONB_EXISTS:
                return PgType.of(DataType.TEXT);
            case JSON_HASH_ARROW:
            case JSON_HASH_ARROW_TEXT:
            case JSONB_EXISTS_ANY:
            case JSONB_EXISTS_ALL:
                return PgType.arrayOf(DataType.TEXT);
            default:
                return null;
        }
    }

    private static boolean concatenable(PgType t) {
        int c = category(t);
        return c == CAT_STRING || c == CAT_INT || c == CAT_NUMERIC || c == CAT_FLOAT;
    }

    private static boolean stringOperator(BinaryExpr.BinOp op) {
        switch (op) {
            case LIKE:
            case ILIKE:
            case SIMILAR_TO:
            case REGEX_MATCH:
            case REGEX_IMATCH:
            case NOT_REGEX_MATCH:
            case NOT_REGEX_IMATCH:
                return true;
            default:
                return false;
        }
    }

    /**
     * PG's numeric-category resolution: the integer types have cross-type operators for
     * every combination (as do float4/float8), so no cast appears there. Mixing a float
     * with anything else promotes the other side to float8; mixing numeric with an
     * integer promotes the integer to numeric.
     */
    private static PgType[] numericTargets(PgType lt, PgType rt) {
        int lc = category(lt), rc = category(rt);
        boolean lnum = lc == CAT_INT || lc == CAT_NUMERIC || lc == CAT_FLOAT;
        boolean rnum = rc == CAT_INT || rc == CAT_NUMERIC || rc == CAT_FLOAT;
        if (!lnum || !rnum) return null;
        if (lc == rc) return new PgType[]{lt, rt};  // int/int and float/float have cross-type operators
        if (lc == CAT_FLOAT || rc == CAT_FLOAT) {
            PgType f8 = PgType.of(DataType.DOUBLE_PRECISION);
            return new PgType[]{lc == CAT_FLOAT ? lt : f8, rc == CAT_FLOAT ? rt : f8};
        }
        PgType num = PgType.of(DataType.NUMERIC);
        return new PgType[]{lc == CAT_NUMERIC ? lt : num, rc == CAT_NUMERIC ? rt : num};
    }

    /**
     * PG's operator resolution over the string category. text/name have direct
     * cross-type operators; varchar always resolves through text except against
     * bpchar, where {@code bpchareq} wins.
     */
    private static PgType[] stringTargets(PgType lt, PgType rt) {
        if (category(lt) != CAT_STRING || category(rt) != CAT_STRING) return null;
        DataType l = lt.dt, r = rt.dt;
        PgType text = PgType.of(DataType.TEXT);
        PgType bpchar = PgType.of(DataType.CHAR);
        if (l == DataType.NAME && r == DataType.NAME) return new PgType[]{lt, rt};
        if (l == DataType.NAME && r == DataType.TEXT) return new PgType[]{lt, rt};
        if (l == DataType.TEXT && r == DataType.NAME) return new PgType[]{lt, rt};
        if (l == DataType.CHAR && r == DataType.CHAR) return new PgType[]{lt, rt};
        if (l == DataType.VARCHAR && r == DataType.CHAR) return new PgType[]{bpchar, rt};
        if (l == DataType.CHAR && r == DataType.VARCHAR) return new PgType[]{lt, bpchar};
        return new PgType[]{text, text};
    }

    // ------------------------------------------------------------------
    // Function knowledge
    // ------------------------------------------------------------------

    /** Functions whose string arguments are text and whose result is text. */
    private static final Set<String> TEXT_FUNCS = new HashSet<String>(Arrays.asList(
            "lower", "upper", "initcap", "btrim", "ltrim", "rtrim", "md5", "reverse",
            "quote_ident", "quote_literal", "quote_nullable", "replace", "translate",
            "left", "right", "lpad", "rpad", "split_part", "substr", "substring",
            "repeat", "regexp_replace", "concat_ws", "concat", "format", "chr",
            "to_hex", "encode", "normalize", "unaccent", "trim"));

    /** Functions over text whose result is integer. */
    private static final Set<String> TEXT_TO_INT_FUNCS = new HashSet<String>(Arrays.asList(
            "length", "char_length", "character_length", "octet_length", "bit_length",
            "strpos", "position", "ascii"));

    /** Functions whose result type is the type of their first argument. */
    private static final Set<String> FIRST_ARG_FUNCS = new HashSet<String>(Arrays.asList(
            "abs", "round", "ceil", "ceiling", "floor", "trunc", "sign", "mod",
            "greatest", "least", "coalesce", "nullif"));

    /** Functions PG prints in upper case because they are special node types, not calls. */
    private static final Set<String> UPPERCASE_FUNCS = new HashSet<String>(Arrays.asList(
            "coalesce", "nullif", "greatest", "least"));

    /** Functions whose arguments PG unifies to a single common type. */
    private static final Set<String> UNIFYING_FUNCS = new HashSet<String>(Arrays.asList(
            "coalesce", "nullif", "greatest", "least"));

    private static PgType functionReturnType(FunctionCallExpr fn, ColumnTypes cols) {
        String name = lower(fn.name());
        if (isTypeName(name)) return parseTypeName(name);
        if (TEXT_TO_INT_FUNCS.contains(name)) return PgType.of(DataType.INTEGER);
        if (UNIFYING_FUNCS.contains(name)) {
            PgType t = null;
            for (Expression a : fn.args()) t = unify(t, typeOf(a, cols));
            return t;
        }
        if (TEXT_FUNCS.contains(name)) return PgType.of(DataType.TEXT);
        if (FIRST_ARG_FUNCS.contains(name) && !fn.args().isEmpty()) {
            return typeOf(fn.args().get(0), cols);
        }
        return null;
    }

    // ------------------------------------------------------------------
    // Rendering (the ruleutils.c subset)
    // ------------------------------------------------------------------

    /**
     * @param target       type the parent context coerces this node to, or null
     * @param showImplicit false suppresses an implicit cast, as PG does for the
     *                     argument of a SQL-syntax function such as {@code TRIM}
     */
    private static String render(Expression expr, PgType target, ColumnTypes cols, boolean suppressCast) {
        Expression e = fold(expr);
        if (e == null) return "NULL";

        if (e instanceof Literal) {
            return renderLiteral((Literal) e, target);
        }
        // An array constructor takes its element type from the context it stands in, so a bare
        // constant inside one is printed as a constant of that type: ARRAY['x'::text] beside a
        // text[] column, not ARRAY['x'].
        if (e instanceof ArrayExpr && target != null && target.elem != null
                && !((ArrayExpr) e).isRow()) {
            return renderArray((ArrayExpr) e, PgType.of(target.elem), cols);
        }

        String body = renderNode(e, cols);
        if (suppressCast || target == null) return body;
        PgType natural = typeOf(e, cols);
        if (natural == null || natural.sameAs(target)) return body;
        return "(" + body + ")::" + formatType(target);
    }

    private static String renderNode(Expression e, ColumnTypes cols) {
        if (e instanceof ColumnRef) {
            ColumnRef r = (ColumnRef) e;
            String col = "value".equalsIgnoreCase(r.column()) && cols instanceof DomainValue
                    ? "VALUE" : quoteIdentifier(r.column());
            return (r.table() != null ? quoteIdentifier(r.table()) + "." : "") + col;
        }
        if (e instanceof CastExpr) {
            CastExpr c = (CastExpr) e;
            PgType to = parseTypeName(c.typeName());
            if (c.expr() instanceof Literal) {
                return renderLiteral((Literal) c.expr(), to);
            }
            return "(" + renderNode(c.expr(), cols) + ")::" + formatType(to);
        }
        if (e instanceof CollateExpr) {
            // A collation written inside an expression is a node of that expression and is
            // bracketed like one: upper((b COLLATE "C")). Only a collation an index key takes for
            // its own stands unbracketed, and that one is no longer part of the expression.
            CollateExpr c = (CollateExpr) e;
            return "(" + renderNode(c.expr(), cols) + " COLLATE " + quoteCollation(c.collation()) + ")";
        }
        if (e instanceof AtTimeZoneExpr) {
            // The clause is a call on the zone and the value, and PostgreSQL writes the call back
            // in the syntax it was written in, parentheses included. A zone written as a bare
            // string is a constant of no type until the call resolves it, and the call it resolves
            // to is the one taking text.
            AtTimeZoneExpr z = (AtTimeZoneExpr) e;
            PgType zoneType = typeOf(z.zone(), cols);
            return "(" + render(z.expr(), null, cols, false) + " AT TIME ZONE "
                    + render(z.zone(), zoneType == null ? PgType.of(DataType.TEXT) : null, cols, false)
                    + ")";
        }
        if (e instanceof NamedArgExpr) {
            // An argument written under a parameter's name is written back under it, because
            // which parameter it fills is part of what the call says.
            NamedArgExpr named = (NamedArgExpr) e;
            return quoteIdentifier(named.name()) + " => " + render(named.value(), null, cols, false);
        }
        if (e instanceof BinaryExpr) {
            return renderBinary((BinaryExpr) e, cols);
        }
        if (e instanceof UnaryExpr) {
            UnaryExpr u = (UnaryExpr) e;
            switch (u.op()) {
                case NOT:
                    // PG's parser builds the negated operator directly for NOT LIKE / NOT ILIKE.
                    if (u.operand() instanceof LikeExpr) {
                        LikeExpr l = (LikeExpr) u.operand();
                        return renderLike(new LikeExpr(l.left(), l.pattern(), l.escape(),
                                l.caseInsensitive(), !l.negated()), cols);
                    }
                    if (u.operand() instanceof BinaryExpr) {
                        BinaryExpr b = (BinaryExpr) u.operand();
                        if (b.op() == BinaryExpr.BinOp.LIKE || b.op() == BinaryExpr.BinOp.ILIKE) {
                            return renderLike(new LikeExpr(b.left(), b.right(), null,
                                    b.op() == BinaryExpr.BinOp.ILIKE, true), cols);
                        }
                    }
                    return "(NOT " + render(u.operand(), PgType.of(DataType.BOOLEAN), cols, false) + ")";
                case NEGATE:
                    return "(- " + render(u.operand(), null, cols, false) + ")";
                case POSITIVE:
                    return "(+ " + render(u.operand(), null, cols, false) + ")";
                case BIT_NOT:
                    return "(~ " + render(u.operand(), null, cols, false) + ")";
                default:
                    return render(u.operand(), null, cols, false);
            }
        }
        if (e instanceof IsNullExpr) {
            IsNullExpr n = (IsNullExpr) e;
            return "(" + render(n.expr(), null, cols, false)
                    + (n.negated() ? " IS NOT NULL" : " IS NULL") + ")";
        }
        if (e instanceof IsBooleanExpr) {
            IsBooleanExpr b = (IsBooleanExpr) e;
            return "(" + render(b.expr(), null, cols, false) + " " + booleanTestText(b.test()) + ")";
        }
        if (e instanceof LikeExpr) {
            return renderLike((LikeExpr) e, cols);
        }
        if (e instanceof BetweenExpr) {
            return renderBetween((BetweenExpr) e, cols);
        }
        if (e instanceof InExpr) {
            return renderIn((InExpr) e, cols);
        }
        if (e instanceof AnyAllArrayExpr) {
            AnyAllArrayExpr a = (AnyAllArrayExpr) e;
            PgType lt = typeOf(a.left(), cols);
            return "(" + render(a.left(), null, cols, false) + " " + operatorText(a.op())
                    + (a.isAll() ? " ALL (" : " ANY (") + renderArrayOperand(a.array(), lt, cols) + "))";
        }
        if (e instanceof CaseExpr) {
            return renderCase((CaseExpr) e, cols);
        }
        if (e instanceof FunctionCallExpr) {
            return renderFunction((FunctionCallExpr) e, cols);
        }
        if (e instanceof SubscriptExpr) {
            // Brackets are written back as brackets, so a rule that reads an array element still
            // reads that element when its definition is read back.
            SubscriptExpr sub = (SubscriptExpr) e;
            StringBuilder sb = new StringBuilder(deparse(sub.base(), cols));
            for (SubscriptExpr.Subscript one : sub.subscripts()) {
                sb.append('[');
                if (one.lower() != null) sb.append(deparse(one.lower(), cols));
                if (one.slice()) {
                    sb.append(':');
                    if (one.upper() != null) sb.append(deparse(one.upper(), cols));
                }
                sb.append(']');
            }
            return sb.toString();
        }
        if (e instanceof ArrayExpr) {
            return renderArray((ArrayExpr) e, null, cols);
        }
        if (e instanceof CustomOperatorExpr) {
            CustomOperatorExpr c = (CustomOperatorExpr) e;
            if (c.left() != null) {
                return "(" + render(c.left(), null, cols, false) + " " + c.opSymbol() + " "
                        + render(c.right(), null, cols, false) + ")";
            }
            return "(" + c.opSymbol() + " " + render(c.right(), null, cols, false) + ")";
        }
        return SqlUnparser.exprToSql(e);
    }

    private static String renderBinary(BinaryExpr b, ColumnTypes cols) {
        if (b.op() == BinaryExpr.BinOp.AND || b.op() == BinaryExpr.BinOp.OR) {
            List<Expression> operands = new ArrayList<Expression>();
            flatten(b, b.op(), operands);
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < operands.size(); i++) {
                if (i > 0) sb.append(b.op() == BinaryExpr.BinOp.AND ? " AND " : " OR ");
                sb.append(render(operands.get(i), PgType.of(DataType.BOOLEAN), cols, false));
            }
            return sb.append(")").toString();
        }
        if (b.op() == BinaryExpr.BinOp.SIMILAR_TO) {
            String pat = render(b.right(), PgType.of(DataType.TEXT), cols, false);
            return "(" + render(b.left(), PgType.of(DataType.TEXT), cols, false)
                    + " ~ similar_to_escape(" + pat + "))";
        }
        PgType lt = typeOf(b.left(), cols);
        PgType rt = typeOf(b.right(), cols);
        PgType[] tg = operandTargets(b.op(), lt, rt);
        return "(" + render(b.left(), tg[0], cols, false) + " " + operatorText(b.op()) + " "
                + render(b.right(), tg[1], cols, false) + ")";
    }

    private static void flatten(Expression e, BinaryExpr.BinOp op, List<Expression> out) {
        if (e instanceof BinaryExpr && ((BinaryExpr) e).op() == op) {
            flatten(((BinaryExpr) e).left(), op, out);
            flatten(((BinaryExpr) e).right(), op, out);
        } else {
            out.add(e);
        }
    }

    private static String renderLike(LikeExpr l, ColumnTypes cols) {
        String op = l.caseInsensitive()
                ? (l.negated() ? "!~~*" : "~~*")
                : (l.negated() ? "!~~" : "~~");
        PgType text = PgType.of(DataType.TEXT);
        String s = "(" + render(l.left(), text, cols, false) + " " + op + " "
                + render(l.pattern(), text, cols, false);
        if (l.escape() != null) {
            s += " ESCAPE " + quoteLiteral(l.escape());
        }
        return s + ")";
    }

    /** PG rewrites BETWEEN into comparisons during parse analysis; so do we. */
    private static String renderBetween(BetweenExpr b, ColumnTypes cols) {
        String lo = comparison(b.expr(), BinaryExpr.BinOp.GREATER_EQUAL, b.low(), cols);
        String hi = comparison(b.expr(), BinaryExpr.BinOp.LESS_EQUAL, b.high(), cols);
        String forward = "(" + lo + " AND " + hi + ")";
        String body;
        if (b.symmetric()) {
            String lo2 = comparison(b.expr(), BinaryExpr.BinOp.GREATER_EQUAL, b.high(), cols);
            String hi2 = comparison(b.expr(), BinaryExpr.BinOp.LESS_EQUAL, b.low(), cols);
            body = "(" + forward + " OR (" + lo2 + " AND " + hi2 + "))";
        } else {
            body = forward;
        }
        return b.negated() ? "(NOT " + body + ")" : body;
    }

    private static String comparison(Expression l, BinaryExpr.BinOp op, Expression r, ColumnTypes cols) {
        return renderBinary(new BinaryExpr(l, op, r), cols);
    }

    /** PG rewrites IN (list) into {@code = ANY (ARRAY[...])}. */
    private static String renderIn(InExpr in, ColumnTypes cols) {
        PgType lt = typeOf(in.expr(), cols);
        // Every item of the list is read as the one type the list settles on together with the
        // value being tested, and the comparison is resolved over that type. That is what puts a
        // conversion on the value tested and another on the array of items: a character varying
        // has no equality of its own, so both sides come back read as text.
        PgType element = lt;
        for (int i = 0; i < in.values().size(); i++) {
            element = unify(element, typeOf(in.values().get(i), cols));
        }
        PgType[] readAs = lt == null || element == null ? null
                : operandTargets(in.negated() ? BinaryExpr.BinOp.NOT_EQUAL
                        : BinaryExpr.BinOp.EQUAL, lt, element);
        StringBuilder items = new StringBuilder("ARRAY[");
        for (int i = 0; i < in.values().size(); i++) {
            if (i > 0) items.append(", ");
            items.append(render(in.values().get(i), element, cols, false));
        }
        items.append(']');
        StringBuilder sb = new StringBuilder("(");
        sb.append(render(in.expr(), readAs == null ? null : readAs[0], cols, false));
        sb.append(in.negated() ? " <> ALL (" : " = ANY (");
        if (readAs != null && !readAs[1].sameAs(element) && readAs[1].dt != null) {
            sb.append('(').append(items).append(")::")
              .append(formatType(PgType.arrayOf(readAs[1].dt)));
        } else {
            sb.append(items);
        }
        return sb.append("))").toString();
    }

    private static String renderArrayOperand(Expression arr, PgType elemTarget, ColumnTypes cols) {
        if (arr instanceof ArrayExpr) return renderArray((ArrayExpr) arr, elemTarget, cols);
        return render(arr, null, cols, false);
    }

    private static String renderArray(ArrayExpr arr, PgType elemTarget, ColumnTypes cols) {
        StringBuilder sb = new StringBuilder(arr.isRow() ? "ROW(" : "ARRAY[");
        for (int i = 0; i < arr.elements().size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(render(arr.elements().get(i), elemTarget, cols, false));
        }
        return sb.append(arr.isRow() ? ")" : "]").toString();
    }

    /** PG prints CASE across several lines even with pretty-printing disabled. */
    private static String renderCase(CaseExpr c, ColumnTypes cols) {
        StringBuilder sb = new StringBuilder("\nCASE");
        if (c.operand() != null) sb.append(' ').append(render(c.operand(), null, cols, false));
        PgType operandType = c.operand() == null ? null : typeOf(c.operand(), cols);
        for (CaseExpr.WhenClause w : c.whenClauses()) {
            sb.append("\n    WHEN ")
              .append(render(w.condition(), c.operand() == null ? PgType.of(DataType.BOOLEAN) : operandType, cols, false))
              .append(" THEN ")
              .append(render(w.result(), null, cols, false));
        }
        if (c.elseExpr() != null) {
            sb.append("\n    ELSE ").append(render(c.elseExpr(), null, cols, false));
        }
        return sb.append("\nEND").toString();
    }

    private static String renderFunction(FunctionCallExpr fn, ColumnTypes cols) {
        // A name written under a schema the search path reaches on its own is written back without
        // it: what PG prints is the function the call resolved to, and pg_catalog and public are
        // always in reach, so pg_catalog.lower(c) reads back as lower(c).
        String name = unqualify(lower(fn.name()));

        // A value function is a keyword of the grammar rather than a call, so a default or a
        // check written with one reads back as the keyword: CURRENT_DATE, not current_date().
        if (!fn.star() && !fn.distinct() && (fn.args() == null || fn.args().isEmpty())) {
            String keyword = SqlValueFunctions.keywordOf(name, false);
            if (keyword != null) return keyword;
        }

        // A cast written in function-call syntax, e.g. int4(x), deparses as a cast.
        if (isTypeName(name) && fn.args().size() == 1) {
            PgType to = parseTypeName(name);
            if (fn.args().get(0) instanceof Literal) {
                return renderLiteral((Literal) fn.args().get(0), to);
            }
            return "(" + renderNode(fn.args().get(0), cols) + ")::" + formatType(to);
        }

        if ("position".equals(name) && fn.args().size() == 2) {
            return "POSITION((" + render(fn.args().get(0), PgType.of(DataType.TEXT), cols, false)
                    + ") IN (" + render(fn.args().get(1), PgType.of(DataType.TEXT), cols, false) + "))";
        }

        if ("xmlserialize".equals(name)) {
            String written = renderXmlSerialize(fn, cols);
            if (written != null) return written;
        }

        // A call SQL spells with keywords of its own is written back with them. PostgreSQL keeps
        // which of the two spellings was used, so substring(s, 1, 2) and SUBSTRING(s FROM 1 FOR 2)
        // read back differently even though they call the same function.
        if (fn.spelledInGrammar) {
            String written = renderSqlSyntax(fn, name, cols);
            if (written != null) return written;
        }

        List<PgType> targets = argumentTargets(fn, name, cols);
        // A name the grammar knows is written in quotes, or reading the definition back would
        // find the construct that name spells instead of the function: substring(s, 1, 2) is a
        // call on "substring", while SUBSTRING(s FROM 1 FOR 2) is the construct.
        StringBuilder sb = new StringBuilder(
                UPPERCASE_FUNCS.contains(name) ? name.toUpperCase() : quoteFunctionName(name));
        sb.append('(');
        if (fn.distinct()) sb.append("DISTINCT ");
        if (fn.star()) {
            sb.append('*');
        } else {
            for (int i = 0; i < fn.args().size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(render(fn.args().get(i), targets.get(i), cols, false));
            }
        }
        return sb.append(')').toString();
    }

    /**
     * The SQL-syntax spelling of a call PostgreSQL prints back in that syntax, or null when the
     * call is not one of them. EXTRACT names its field as a word rather than a string; TRIM says
     * which end it trims in place of choosing between three function names; SUBSTRING and OVERLAY
     * write their offsets after FROM and FOR.
     */
    private static String renderSqlSyntax(FunctionCallExpr fn, String name, ColumnTypes cols) {
        List<Expression> args = fn.args();
        PgType text = PgType.of(DataType.TEXT);
        int n = args.size();
        if ("extract".equals(name) && n == 2 && args.get(0) instanceof Literal) {
            return "EXTRACT(" + ((Literal) args.get(0)).value()
                    + " FROM " + render(args.get(1), null, cols, false) + ")";
        }
        if ((n == 1 || n == 2)
                && ("btrim".equals(name) || "ltrim".equals(name) || "rtrim".equals(name))) {
            String end = "ltrim".equals(name) ? "LEADING" : "rtrim".equals(name) ? "TRAILING" : "BOTH";
            return "TRIM(" + end + " "
                    + (n == 2 ? render(args.get(1), text, cols, false) + " " : "")
                    + "FROM " + render(args.get(0), text, cols, false) + ")";
        }
        if ("substring".equals(name) && (n == 2 || n == 3)) {
            // Only the offset form keeps the spelling: SUBSTRING(s FROM pattern) resolves to the
            // call that matches a pattern, which is a different function and prints as one. An
            // uncoerced string constant is a pattern here, whatever its type is later settled to.
            PgType from = typeOf(args.get(1), cols);
            if (from != null && category(from) == CAT_STRING) return null;
            if (args.get(1) instanceof Literal
                    && ((Literal) args.get(1)).literalType() == Literal.LiteralType.STRING) {
                return null;
            }
            StringBuilder sb = new StringBuilder("SUBSTRING(")
                    .append(render(args.get(0), text, cols, false))
                    .append(" FROM ").append(render(args.get(1), null, cols, false));
            if (n == 3) sb.append(" FOR ").append(render(args.get(2), null, cols, false));
            return sb.append(')').toString();
        }
        if ("overlay".equals(name) && (n == 3 || n == 4)) {
            StringBuilder sb = new StringBuilder("OVERLAY(")
                    .append(render(args.get(0), text, cols, false))
                    .append(" PLACING ").append(render(args.get(1), text, cols, false))
                    .append(" FROM ").append(render(args.get(2), null, cols, false));
            if (n == 4) sb.append(" FOR ").append(render(args.get(3), null, cols, false));
            return sb.append(')').toString();
        }
        return null;
    }

    /**
     * XMLSERIALIZE written back as XMLSERIALIZE. It has no other spelling — SQL gives it a syntax
     * and PostgreSQL declares no function of that name — so the record of the call is read back in
     * that syntax whether or not the grammar was what built it. Which of CONTENT and DOCUMENT was
     * asked for and which type the result was wanted as are both part of what it says, and so is
     * whether the output is laid out: the clause has no default spelling, so a serialisation that
     * is not indented reads back as NO INDENT rather than as nothing at all.
     */
    private static String renderXmlSerialize(FunctionCallExpr fn, ColumnTypes cols) {
        List<Expression> args = fn.args();
        int n = args == null ? 0 : args.size();
        if ((n != 3 && n != 4) || !(args.get(0) instanceof Literal)
                || !(args.get(2) instanceof Literal)) {
            return null;
        }
        boolean indent = n == 4 && args.get(3) instanceof Literal
                && "indent".equalsIgnoreCase(((Literal) args.get(3)).value());
        return "XMLSERIALIZE("
                + ("document".equalsIgnoreCase(((Literal) args.get(0)).value())
                        ? "DOCUMENT " : "CONTENT ")
                + render(args.get(1), null, cols, false)
                + " AS " + formatType(parseTypeName(((Literal) args.get(2)).value()))
                + (indent ? " INDENT" : " NO INDENT") + ")";
    }

    /** A function's name written so that reading it back names the function again, part by part. */
    private static String quoteFunctionName(String name) {
        if (name == null || name.indexOf('.') < 0) return quoteIdentifier(name);
        StringBuilder sb = new StringBuilder();
        for (String part : name.split("\\.", -1)) {
            if (sb.length() > 0) sb.append('.');
            sb.append(quoteIdentifier(part));
        }
        return sb.toString();
    }

    /** A function name stripped of a schema every search path reaches without being told to. */
    private static String unqualify(String name) {
        if (name == null) return null;
        if (name.startsWith("pg_catalog.")) return name.substring("pg_catalog.".length());
        if (name.startsWith("public.")) return name.substring("public.".length());
        return name;
    }

    private static List<PgType> argumentTargets(FunctionCallExpr fn, String name, ColumnTypes cols) {
        int n = fn.args().size();
        List<PgType> targets = new ArrayList<PgType>(Collections.<PgType>nCopies(n, null));
        if (UNIFYING_FUNCS.contains(name)) {
            PgType common = null;
            for (Expression a : fn.args()) common = unify(common, typeOf(a, cols));
            if (common != null) {
                for (int i = 0; i < n; i++) targets.set(i, common);
            }
            return targets;
        }
        PgType text = PgType.of(DataType.TEXT);
        boolean allText = TEXT_FUNCS.contains(name) || TEXT_TO_INT_FUNCS.contains(name);
        for (int i = 0; i < n; i++) {
            // A parameter declared text takes a string constant as text, and PG's deparse shows
            // the coercion that resolution inserted -- date_trunc('month', ts) reads back as
            // date_trunc('month'::text, ts). Which parameters those are is read off the signatures
            // PostgreSQL declares for the name, so a position one of them declares text
            // is read as text; a name it declares nothing for is left alone.
            if (!allText && !BuiltinFunctionSignatures.someSignatureTakesTextAt(name, n, i)) continue;
            PgType at = typeOf(fn.args().get(i), cols);
            if (at == null || category(at) == CAT_STRING) targets.set(i, text);
        }
        return targets;
    }

    // ------------------------------------------------------------------
    // Constants (get_const_expr)
    // ------------------------------------------------------------------

    private static String renderLiteral(Literal lit, PgType target) {
        switch (lit.literalType()) {
            case NULL:
                return target == null ? "NULL" : "NULL::" + formatType(target);
            case DEFAULT:
                return "DEFAULT";
            case BOOLEAN:
                return "true".equalsIgnoreCase(lit.value()) ? "true" : "false";
            case BIT_STRING: {
                // The B in front of the quotes is grammar rather than value: what is stored is a
                // constant of the bit type, and a conversion to another one stands in front of it.
                PgType bit = PgType.of(DataType.BIT);
                if (target == null || target.sameAs(bit)) return constant(lit.value(), bit);
                return "(" + constant(lit.value(), bit) + ")::" + formatType(target);
            }
            case STRING: {
                // An untyped literal is folded into a constant of the target type at
                // parse-analysis time, so it is printed in the target type's own form.
                if (target == null) return quoteLiteral(lit.value());
                return constant(canonicalise(lit.value(), target), target);
            }
            case INTEGER:
            case FLOAT:
            default: {
                PgType natural = literalType(lit);
                String extval = canonicalise(lit.value(), natural);
                if (target == null || target.sameAs(natural)) return constant(extval, natural);
                // A literal that already has a type is coerced by a cast node, not re-typed.
                return "(" + constant(extval, natural) + ")::" + formatType(target);
            }
        }
    }

    /**
     * PG's {@code get_const_expr}: a constant is printed bare only when re-reading it
     * would yield the same type — booleans and non-negative int4, plus numerics that
     * look like a float literal. Everything else is quoted and labelled.
     */
    private static String constant(String extval, PgType type) {
        if (extval == null) return "NULL";
        boolean needLabel;
        if (type == null) {
            needLabel = false;
        } else if (type.custom != null || type.elem != null || type.dt == null) {
            needLabel = true;
        } else {
            switch (type.dt) {
                case BOOLEAN:
                    needLabel = false;
                    break;
                case INTEGER:
                case SERIAL:
                    needLabel = extval.startsWith("-");
                    break;
                case NUMERIC:
                    needLabel = extval.startsWith("-") || !looksLikeFloat(extval);
                    break;
                default:
                    needLabel = true;
                    break;
            }
        }
        if (!needLabel) return extval;
        return quoteLiteral(extval) + "::" + formatType(type);
    }

    private static boolean looksLikeFloat(String s) {
        return s.indexOf('.') >= 0 || s.indexOf('e') >= 0 || s.indexOf('E') >= 0;
    }

    /** Renders a literal's text the way the target type's output function would. */
    private static String canonicalise(String raw, PgType type) {
        if (raw == null || type == null) return raw;
        // An array is read and written element by element, by the readers a cast to the array
        // type goes through, so a default written '{ 1, 2 }' is reported as the '{1,2}' reading
        // the value again would produce.
        if (type.elem != null) return ViewDeparser.arrayText(raw, type.elem);
        if (type.dt == null) return raw;
        try {
            switch (type.dt) {
                case NUMERIC:
                    return new BigDecimal(raw.trim()).toPlainString();
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case SERIAL:
                case BIGSERIAL:
                case SMALLSERIAL:
                    return new BigInteger(raw.trim()).toString();
                case TIME:
                case TIMETZ:
                    return canonicaliseTime(raw.trim(), type.dt);
                case BYTEA:
                    return raw.startsWith("\\x") ? raw : "\\x" + hex(raw);
                case JSONB: {
                    // A jsonb constant keeps the value it was read as rather than the text it was
                    // written in, so it is printed back the way the type's own output writes it —
                    // one space after each colon, and each object's keys in the order jsonb holds
                    // them. json keeps its text, which is why only jsonb is answered here.
                    Object stored = TypeCoercion.coerce(raw, DataType.JSONB);
                    return stored == null ? raw : TypeCoercion.toString(stored);
                }
                default:
                    return raw;
            }
        } catch (RuntimeException ex) {
            return raw;
        }
    }

    /**
     * PostgreSQL's time output is always hh:mm:ss[.ffffff], and which reading a spelling names is
     * for the type's own reader to say: counting the colons wrote a default of '3:4' back as
     * '3:4:00', which is no time at all, and one of '10:30+02' as '10:30+02:00'.
     */
    private static String canonicaliseTime(String s, DataType type) {
        Object value = TypeCoercion.coerce(s, type);
        return value == null ? s : TypeCoercion.toString(value);
    }

    private static String hex(String s) {
        StringBuilder sb = new StringBuilder();
        for (byte b : s.getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------
    // Names and operators
    // ------------------------------------------------------------------

    private static String operatorText(BinaryExpr.BinOp op) {
        switch (op) {
            case ADD: return "+";
            case SUBTRACT: return "-";
            case MULTIPLY: return "*";
            case DIVIDE: return "/";
            case MODULO: return "%";
            case POWER: return "^";
            case EQUAL: return "=";
            case NOT_EQUAL: return "<>";
            case LESS_THAN: return "<";
            case GREATER_THAN: return ">";
            case LESS_EQUAL: return "<=";
            case GREATER_EQUAL: return ">=";
            case AND: return "AND";
            case OR: return "OR";
            case CONCAT: return "||";
            case LIKE: return "~~";
            case ILIKE: return "~~*";
            case REGEX_MATCH: return "~";
            case REGEX_IMATCH: return "~*";
            case NOT_REGEX_MATCH: return "!~";
            case NOT_REGEX_IMATCH: return "!~*";
            case IS_DISTINCT_FROM: return "IS DISTINCT FROM";
            case IS_NOT_DISTINCT_FROM: return "IS NOT DISTINCT FROM";
            case JSON_ARROW:
            case JSON_SUBSCRIPT: return "->";
            case JSON_ARROW_TEXT: return "->>";
            case JSON_HASH_ARROW: return "#>";
            case JSON_HASH_ARROW_TEXT: return "#>>";
            case CONTAINS: return "@>";
            case CONTAINED_BY: return "<@";
            case OVERLAP: return "&&";
            case JSONB_EXISTS: return "?";
            case JSONB_EXISTS_ANY: return "?|";
            case JSONB_EXISTS_ALL: return "?&";
            case TS_MATCH: return "@@";
            case BIT_AND: return "&";
            case BIT_OR: return "|";
            case BIT_XOR: return "#";
            case SHIFT_LEFT: return "<<";
            case SHIFT_RIGHT: return ">>";
            default: return op.name();
        }
    }

    private static String booleanTestText(IsBooleanExpr.BooleanTest t) {
        switch (t) {
            case IS_TRUE: return "IS TRUE";
            case IS_NOT_TRUE: return "IS NOT TRUE";
            case IS_FALSE: return "IS FALSE";
            case IS_NOT_FALSE: return "IS NOT FALSE";
            case IS_UNKNOWN: return "IS UNKNOWN";
            case IS_NOT_UNKNOWN: return "IS NOT UNKNOWN";
            case IS_DOCUMENT: return "IS DOCUMENT";
            default: return "IS NOT DOCUMENT";
        }
    }

    private static String lower(String s) {
        return s == null ? null : s.toLowerCase();
    }

    private static String quoteLiteral(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    private static String quoteCollation(String s) {
        if (s == null) return null;
        String bare = s.replace("\"", "");
        return "\"" + bare + "\"";
    }

    /** PG's {@code quote_identifier}: quote unless the name is a safe bare lower-case word. */
    public static String quoteIdentifier(String name) {
        if (name == null) return null;
        boolean safe = !name.isEmpty() && (name.charAt(0) == '_' || isLowerAlpha(name.charAt(0)));
        if (safe) {
            for (int i = 1; i < name.length(); i++) {
                char c = name.charAt(i);
                if (!(isLowerAlpha(c) || (c >= '0' && c <= '9') || c == '_' || c == '$')) {
                    safe = false;
                    break;
                }
            }
        }
        // Every word the grammar knows has to be quoted, not only the ones it reserves outright:
        // a name spelled like one of the constructs that take an argument list — substring, time,
        // xmlelement — reads back as that construct unless the quotes say it is a name. The list
        // this used to carry left those out, so a column called "time" was written down as one
        // nothing could read again.
        if (safe && com.memgres.engine.parser.PgKeywords.isKeywordOrReserved(name)) safe = false;
        return safe ? name : "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static boolean isLowerAlpha(char c) {
        return c >= 'a' && c <= 'z';
    }

    // ------------------------------------------------------------------
    // Type names
    // ------------------------------------------------------------------

    private static final Map<String, DataType> TYPE_ALIASES;

    static {
        Map<String, DataType> m = new HashMap<String, DataType>();
        m.put("int", DataType.INTEGER);
        m.put("int4", DataType.INTEGER);
        m.put("integer", DataType.INTEGER);
        m.put("int2", DataType.SMALLINT);
        m.put("smallint", DataType.SMALLINT);
        m.put("int8", DataType.BIGINT);
        m.put("bigint", DataType.BIGINT);
        m.put("float4", DataType.REAL);
        m.put("real", DataType.REAL);
        m.put("float8", DataType.DOUBLE_PRECISION);
        m.put("float", DataType.DOUBLE_PRECISION);
        m.put("double precision", DataType.DOUBLE_PRECISION);
        m.put("numeric", DataType.NUMERIC);
        m.put("decimal", DataType.NUMERIC);
        m.put("text", DataType.TEXT);
        m.put("varchar", DataType.VARCHAR);
        m.put("character varying", DataType.VARCHAR);
        m.put("char", DataType.CHAR);
        m.put("bpchar", DataType.CHAR);
        m.put("character", DataType.CHAR);
        m.put("name", DataType.NAME);
        m.put("bool", DataType.BOOLEAN);
        m.put("boolean", DataType.BOOLEAN);
        m.put("date", DataType.DATE);
        m.put("time", DataType.TIME);
        m.put("time without time zone", DataType.TIME);
        m.put("timetz", DataType.TIMETZ);
        m.put("time with time zone", DataType.TIMETZ);
        m.put("timestamp", DataType.TIMESTAMP);
        m.put("timestamp without time zone", DataType.TIMESTAMP);
        m.put("timestamptz", DataType.TIMESTAMPTZ);
        m.put("timestamp with time zone", DataType.TIMESTAMPTZ);
        m.put("interval", DataType.INTERVAL);
        m.put("uuid", DataType.UUID);
        m.put("json", DataType.JSON);
        m.put("jsonb", DataType.JSONB);
        m.put("bytea", DataType.BYTEA);
        m.put("inet", DataType.INET);
        m.put("cidr", DataType.CIDR);
        m.put("macaddr", DataType.MACADDR);
        m.put("money", DataType.MONEY);
        m.put("oid", DataType.OID);
        m.put("xml", DataType.XML);
        m.put("tsvector", DataType.TSVECTOR);
        m.put("tsquery", DataType.TSQUERY);
        TYPE_ALIASES = Collections.unmodifiableMap(m);
    }

    private static boolean isTypeName(String name) {
        return name != null && TYPE_ALIASES.containsKey(name.toLowerCase());
    }

    /** Parses a type name as written in a cast into a resolved type. */
    static PgType parseTypeName(String typeName) {
        if (typeName == null) return null;
        String t = typeName.trim();
        boolean array = t.endsWith("[]");
        if (array) t = t.substring(0, t.length() - 2).trim();
        int typmod = -1;
        int paren = t.indexOf('(');
        if (paren > 0 && t.endsWith(")")) {
            typmod = parseTypmod(t.substring(0, paren).trim().toLowerCase(),
                    t.substring(paren + 1, t.length() - 1));
            t = t.substring(0, paren).trim();
        }
        DataType dt = TYPE_ALIASES.get(t.toLowerCase());
        if (dt == null) return PgType.custom(t);
        return array ? PgType.arrayOf(dt) : PgType.of(dt, typmod);
    }

    private static int parseTypmod(String base, String args) {
        try {
            String[] parts = args.split(",");
            if ("numeric".equals(base) || "decimal".equals(base)) {
                int p = Integer.parseInt(parts[0].trim());
                int s = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
                return ((p << 16) | s) + 4;
            }
            return Integer.parseInt(parts[0].trim()) + 4;
        } catch (RuntimeException ex) {
            return -1;
        }
    }

    /** PG's {@code format_type_with_typemod}. */
    static String formatType(PgType t) {
        if (t == null) return "text";
        if (t.custom != null) return quoteIdentifier(t.custom);
        if (t.elem != null) return formatType(PgType.of(t.elem)) + "[]";
        switch (t.dt) {
            case INTEGER:
            case SERIAL:
                return "integer";
            case BIGINT:
            case BIGSERIAL:
                return "bigint";
            case SMALLINT:
            case SMALLSERIAL:
                return "smallint";
            case REAL:
                return "real";
            case DOUBLE_PRECISION:
                return "double precision";
            case NUMERIC:
                if (t.typmod > 0) {
                    int raw = t.typmod - 4;
                    return "numeric(" + ((raw >> 16) & 0xFFFF) + "," + (raw & 0xFFFF) + ")";
                }
                return "numeric";
            case TEXT:
                return "text";
            case VARCHAR:
                return t.typmod > 0 ? "character varying(" + (t.typmod - 4) + ")" : "character varying";
            case CHAR:
                // format_type prints "bpchar" for an explicit typmod of -1, which is what
                // every implicit coercion to bpchar carries.
                return t.typmod > 0 ? "character(" + (t.typmod - 4) + ")" : "bpchar";
            case NAME:
                return "name";
            case BIT:
                // An unadorned bit means bit(1) to the grammar, so the name is written in quotes
                // for the text to read back as the type it names.
                return "\"bit\"";
            case VARBIT:
                return "bit varying";
            case BOOLEAN:
                return "boolean";
            case DATE:
                return "date";
            case TIME:
                return "time without time zone";
            case TIMETZ:
                return "time with time zone";
            case TIMESTAMP:
                return "timestamp without time zone";
            case TIMESTAMPTZ:
                return "timestamp with time zone";
            case INTERVAL:
                return "interval";
            default:
                return t.dt.getPgName();
        }
    }
}
