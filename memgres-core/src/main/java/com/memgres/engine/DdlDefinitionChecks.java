package com.memgres.engine;

import com.memgres.engine.parser.ast.ArraySubqueryExpr;
import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.ExistsExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.SubqueryExpr;
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

    // ---- CHECK / POLICY predicates ----

    /**
     * {@code 42804} when the expression is plainly of some non-boolean type. PostgreSQL names
     * the context ({@code CHECK}, {@code POLICY}) and the type it found. Expressions whose type
     * this engine cannot decide statically are left alone rather than guessed at.
     */
    public static void requireBooleanPredicate(Expression expr, Table table, String context) {
        String type = staticTypeName(expr, table);
        if (type != null && !"boolean".equals(type)) {
            throw PgErrors.datatypeMismatch("argument of " + context
                    + " must be type boolean, not type " + type);
        }
    }

    /**
     * The expression's type where it is decidable without a full type resolver: a column
     * reference, a cast, or a literal that is not of the untyped {@code unknown} kind. Returns
     * null when the type is not decidable here.
     */
    private static String staticTypeName(Expression expr, Table table) {
        if (expr instanceof ColumnRef && table != null) {
            int idx = table.getColumnIndex(((ColumnRef) expr).column());
            if (idx < 0) return null;
            Column col = table.getColumns().get(idx);
            if (col.getEnumTypeName() != null || col.getDomainTypeName() != null) return null;
            return col.getType() != null ? col.getType().toRegtypeDisplay() : null;
        }
        if (expr instanceof CastExpr) {
            DataType dt = DataType.fromPgName(baseTypeName(((CastExpr) expr).typeName()));
            return dt != null ? dt.toRegtypeDisplay() : null;
        }
        if (expr instanceof Literal) {
            Literal lit = (Literal) expr;
            switch (lit.literalType()) {
                case INTEGER: return "integer";
                case FLOAT: return "numeric";
                case BOOLEAN: return "boolean";
                // A bare string literal is still of type unknown and coerces to boolean.
                default: return null;
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
        if (col.getEnumTypeName() != null) return col.getEnumTypeName();
        if (col.getDomainTypeName() != null) return col.getDomainTypeName();
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
}
