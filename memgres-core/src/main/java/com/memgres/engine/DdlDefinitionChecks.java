package com.memgres.engine;

import com.memgres.engine.parser.ast.ArraySubqueryExpr;
import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.ExistsExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.SubqueryExpr;
import com.memgres.engine.parser.ast.UnaryExpr;
import com.memgres.engine.util.Cols;

import java.util.Set;

/**
 * Definition-time checks PostgreSQL makes before it will record a table or column definition.
 *
 * <p>These all reject definitions PostgreSQL considers incoherent — a column that shadows a
 * system column, a DEFAULT that can never be evaluated, a CHECK that is not a predicate, an
 * identity column whose type has no sequence behind it. Accepting them stores a definition
 * PostgreSQL could not have produced, and the contradiction surfaces later at some unrelated
 * statement (an INSERT, a catalog read) that then looks like the culprit.
 */
public final class DdlDefinitionChecks {

    private DdlDefinitionChecks() {
    }

    /**
     * The system columns every table carries. A user column of the same name would shadow the
     * system one, so anything reading {@code ctid} or {@code xmin} from that table would get the
     * user's data instead.
     */
    private static final Set<String> SYSTEM_COLUMN_NAMES = Cols.setOf(
            "ctid", "xmin", "cmin", "xmax", "cmax", "tableoid");

    /** Types whose values are varlena, and so can be stored out of line or compressed. */
    private static final Set<DataType> VARLENA_TYPES = Cols.setOf(
            DataType.NUMERIC, DataType.VARCHAR, DataType.CHAR, DataType.TEXT,
            DataType.INET, DataType.CIDR, DataType.BYTEA, DataType.JSON, DataType.JSONB,
            DataType.TSVECTOR, DataType.PATH, DataType.POLYGON, DataType.XML,
            DataType.BIT, DataType.VARBIT, DataType.HSTORE, DataType.RECORD,
            DataType.INT4RANGE, DataType.INT8RANGE, DataType.NUMRANGE, DataType.DATERANGE,
            DataType.TSRANGE, DataType.TSTZRANGE,
            DataType.INT4MULTIRANGE, DataType.INT8MULTIRANGE, DataType.NUMMULTIRANGE,
            DataType.DATEMULTIRANGE, DataType.TSMULTIRANGE, DataType.TSTZMULTIRANGE);

    /** Types that carry a collation. Everything else rejects a COLLATE clause outright. */
    private static final Set<DataType> COLLATABLE_TYPES = Cols.setOf(
            DataType.TEXT, DataType.VARCHAR, DataType.CHAR, DataType.NAME,
            DataType.TEXT_ARRAY, DataType.VARCHAR_ARRAY, DataType.CHAR_ARRAY, DataType.NAME_ARRAY);

    // ---- range subtypes ----

    /**
     * Types PostgreSQL can put no two values of in order: it has no default btree operator class
     * for them and none for any type they are binary-coercible to either. Measured against
     * PostgreSQL 18 rather than read off its opclass list, because the list is not the whole rule —
     * a varchar borrows text's class and a regclass borrows oid's, so neither belongs here even
     * though neither has a class of its own.
     */
    private static final Set<DataType> UNORDERED_TYPES = Cols.setOf(
            DataType.JSON, DataType.XML, DataType.XID,
            DataType.POINT, DataType.LINE, DataType.LSEG, DataType.BOX,
            DataType.PATH, DataType.POLYGON, DataType.CIRCLE);

    /**
     * A range keeps its bounds in order, and it is the subtype's default btree operator class that
     * puts them there — so a subtype nothing can order is one no range can be built over.
     * PostgreSQL says so when the type is defined, not when a value of it is first written.
     */
    public static void requireOrderableRangeSubtype(DataType subtype) {
        if (subtype == null || !UNORDERED_TYPES.contains(subtype)) return;
        throw new MemgresException("data type " + CatalogHelper.pgTypeName(subtype)
                + " has no default operator class for access method \"btree\""
                + "\n  Hint: You must specify an operator class for the range type or define a"
                + " default operator class for the subtype.", "42704");
    }

    // ---- column names ----

    /**
     * {@code 42701} when the name would shadow one of the system columns. The comparison is
     * case-sensitive on purpose: unquoted names have already been folded to lower case, so a
     * name that still has capitals was quoted and is a different identifier from {@code xmax}.
     */
    public static void rejectSystemColumnName(String name) {
        if (name != null && SYSTEM_COLUMN_NAMES.contains(name)) {
            throw new MemgresException("column name \"" + name
                    + "\" conflicts with a system column name", "42701");
        }
    }

    // ---- DEFAULT expressions ----

    /**
     * A DEFAULT is evaluated with no row and no query in scope, so a subquery or a column
     * reference in one can never produce a value. PostgreSQL refuses the definition rather than
     * storing a default that would fail at every insert.
     */
    public static void validateDefaultExpression(Expression expr) {
        if (expr == null) return;
        if (AstWalk.anyMatch(expr, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                return n instanceof SubqueryExpr || n instanceof ExistsExpr
                        || n instanceof ArraySubqueryExpr || n instanceof SelectStmt;
            }
        })) {
            throw PgErrors.notImplemented("cannot use subquery in DEFAULT expression");
        }
        if (AstWalk.anyMatch(expr, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) { return n instanceof ColumnRef; }
        })) {
            throw PgErrors.notImplemented("cannot use column reference in DEFAULT expression");
        }
    }

    /** Functions that change something when called, so calling one to inspect it is not free. */
    private static final Set<String> SIDE_EFFECTING_FUNCTIONS = Cols.setOf("nextval", "setval");

    /**
     * Whether a DEFAULT can be evaluated now purely to see what it produces. Most expressions can:
     * running {@code now()} to find it is a timestamp costs nothing. {@code nextval} is different
     * — evaluating it consumes a sequence value, so the first row inserted would come out with the
     * second number. PostgreSQL type-checks a default without ever running it, and this is the one
     * place where the difference between checking and running is visible.
     */
    public static boolean isEvaluableAtDefinitionTime(Expression expr) {
        if (expr == null) return false;
        return !AstWalk.anyMatch(expr, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                return n instanceof FunctionCallExpr
                        && ((FunctionCallExpr) n).name != null
                        && SIDE_EFFECTING_FUNCTIONS.contains(
                                ((FunctionCallExpr) n).name.toLowerCase());
            }
        });
    }

    // ---- CHECK / POLICY predicates ----

    /**
     * {@code 42804} when the expression is plainly of some non-boolean type, and {@code 22P02}
     * when a bare string literal stands where the condition does and is not a word boolean's
     * input function knows. PostgreSQL names the context ({@code CHECK}, {@code POLICY}) and the
     * type it found. Expressions whose type this engine cannot decide statically are left alone
     * rather than guessed at, and so are the conditions written inside this one.
     */
    public static void requireBooleanPredicate(Expression expr, Table table, String context) {
        BooleanContext.check(expr, context, BooleanContext.Types.of(table));
    }

    /**
     * The type a DEFAULT expression plainly has, where that is decidable without a full type
     * resolver: a cast names its type, and a literal that is not the untyped string kind carries
     * one of its own. An integer literal's type follows its magnitude, the way PostgreSQL's lexer
     * settles it — {@code 2147483648} is a bigint, not an integer.
     *
     * <p>Returns null when the type is not decidable here, which leaves the caller to judge the
     * expression by what it evaluates to rather than guessing.
     */
    public static String defaultExpressionTypeName(Expression expr, Object value) {
        if (expr instanceof CastExpr) {
            DataType dt = DataType.fromPgName(baseTypeName(((CastExpr) expr).typeName()));
            boolean isArray = ((CastExpr) expr).typeName() != null
                    && ((CastExpr) expr).typeName().replaceAll("\\(.*\\)", "").trim().endsWith("[]");
            return dt == null || isArray ? null : dt.toRegtypeDisplay();
        }
        if (expr instanceof UnaryExpr && ((UnaryExpr) expr).op() == UnaryExpr.UnaryOp.NEGATE) {
            return defaultExpressionTypeName(((UnaryExpr) expr).operand(), value);
        }
        if (expr instanceof Literal) {
            switch (((Literal) expr).literalType()) {
                case INTEGER: return runtimeTypeName(value);
                case FLOAT: return "numeric";
                case BOOLEAN: return "boolean";
                default: return null; // a bare string literal is still of type unknown
            }
        }
        return null;
    }

    /**
     * True for a bare string literal, which is still of type {@code unknown}. PostgreSQL reports
     * a bad one as invalid input for the column's type; anything else already has a type of its
     * own, so the same failure is a type mismatch instead.
     */
    public static boolean isUntypedLiteral(Expression expr) {
        return expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.STRING;
    }

    /**
     * The PostgreSQL type name for an evaluated value, used to say what type a default expression
     * turned out to be. Returns null for values whose type this mapping does not cover.
     */
    public static String runtimeTypeName(Object value) {
        if (value instanceof java.time.OffsetDateTime) return "timestamp with time zone";
        if (value instanceof java.time.LocalDateTime) return "timestamp without time zone";
        if (value instanceof java.time.LocalDate) return "date";
        if (value instanceof java.time.OffsetTime) return "time with time zone";
        if (value instanceof java.time.LocalTime) return "time without time zone";
        if (value instanceof PgInterval) return "interval";
        if (value instanceof String) return "text";
        if (value instanceof Boolean) return "boolean";
        if (value instanceof java.util.UUID) return "uuid";
        if (value instanceof byte[]) return "bytea";
        if (value instanceof java.math.BigDecimal) return "numeric";
        if (value instanceof Double || value instanceof Float) return "double precision";
        if (value instanceof Long) return "bigint";
        if (value instanceof Integer || value instanceof Short) return "integer";
        return null;
    }

    // ---- COLLATE ----

    /**
     * {@code 42804} when a COLLATE clause names a type that carries no collation. Type names
     * this engine does not recognise (domains, enums, composites) are left to the executor.
     */
    public static void rejectUncollatableType(String typeName) {
        DataType dt = DataType.fromPgName(baseTypeName(typeName));
        if (dt == null || COLLATABLE_TYPES.contains(dt)) return;
        boolean isArray = typeName != null && typeName.replaceAll("\\(.*\\)", "").trim().endsWith("[]");
        throw PgErrors.datatypeMismatch("collations are not supported by type "
                + dt.toRegtypeDisplay() + (isArray ? "[]" : ""));
    }

    // ---- identity ----

    /** {@code 22023} — identity is fed by a sequence, so only the integer types can carry it. */
    public static void requireIdentityType(DataType dt) {
        if (dt == DataType.SMALLINT || dt == DataType.INTEGER || dt == DataType.BIGINT
                || dt == DataType.SMALLSERIAL || dt == DataType.SERIAL || dt == DataType.BIGSERIAL) {
            return;
        }
        throw PgErrors.invalidParameter(
                "identity column type must be smallint, integer, or bigint");
    }

    // ---- storage and compression ----

    /**
     * Translate a SET STORAGE option to its {@code pg_attribute.attstorage} code, rejecting both
     * an unknown option ({@code 22023}) and a real one the column's type cannot hold
     * ({@code 0A000}) — a fixed-length value is never stored out of line.
     */
    public static String storageCode(String storageType, Column col) {
        String code;
        switch (storageType.toUpperCase()) {
            case "PLAIN": code = "p"; break;
            case "EXTERNAL": code = "e"; break;
            case "EXTENDED": code = "x"; break;
            case "MAIN": code = "m"; break;
            case "DEFAULT": code = null; break;
            default:
                throw PgErrors.invalidParameter("invalid storage type \"" + storageType + "\"");
        }
        if (!"p".equals(code) && !isVarlena(col)) {
            throw PgErrors.notImplemented("column data type " + typeDisplay(col)
                    + " can only have storage PLAIN");
        }
        return code;
    }

    /**
     * Translate a SET COMPRESSION method to its {@code pg_attribute.attcompression} code.
     * Compression only applies to varlena values, and only the two built-in methods exist.
     */
    public static String compressionCode(String method, Column col) {
        String code;
        if ("pglz".equals(method)) code = "p";
        else if ("lz4".equals(method)) code = "l";
        else if ("default".equals(method)) code = "";
        else throw PgErrors.invalidParameter("invalid compression method \"" + method + "\"");
        if (!isVarlena(col)) {
            throw PgErrors.notImplemented("column data type " + typeDisplay(col)
                    + " does not support compression");
        }
        return code;
    }

    /** True when the column holds varlena values: an array, or one of the varlena base types. */
    private static boolean isVarlena(Column col) {
        if (col.getArrayElementType() != null) return true;
        DataType dt = col.getType();
        if (dt == null) return false;
        if (dt.getPgName().startsWith("_")) return true;
        return VARLENA_TYPES.contains(dt);
    }

    /** The column's type as PostgreSQL spells it in an error about that type. */
    private static String typeDisplay(Column col) {
        if (col.getEnumTypeName() != null) return TypeNamespace.nameOfKey(col.getEnumTypeName());
        if (col.getDomainTypeName() != null) return TypeNamespace.nameOfKey(col.getDomainTypeName());
        DataType dt = col.getType();
        if (dt == null) return "unknown";
        String base = dt.toRegtypeDisplay();
        return col.getArrayElementType() != null ? base + "[]" : base;
    }

    /** Strip typmod and array brackets, leaving the bare type name. */
    private static String baseTypeName(String typeName) {
        if (typeName == null) return "";
        return typeName.replaceAll("\\(.*\\)", "").replace("[]", "").trim();
    }

    /**
     * A relation has at most one primary key. A second one written in the same CREATE TABLE, or
     * added to a table that already has one, is refused as a fault in the definition rather than
     * stored as a second constraint nothing would ever consult.
     */
    static void rejectSecondPrimaryKey(Table table, String tableName) {
        int keys = 0;
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY) keys++;
        }
        if (keys > 1) {
            throw new MemgresException("multiple primary keys for table \"" + tableName
                    + "\" are not allowed", "42P16");
        }
    }
}
