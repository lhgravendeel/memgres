package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.List;

/**
 * Static utility methods shared by PgCatalogBuilder and InfoSchemaBuilder.
 */
public final class CatalogHelper {

    private CatalogHelper() {}

    /** Shorthand for a nullable column with no default. */
    public static Column col(String name, DataType type) {
        return new Column(name, type, true, false, null);
    }

    /** Shorthand for a non-nullable column with no default. */
    public static Column colNN(String name, DataType type) {
        return new Column(name, type, false, false, null);
    }

    /** Create an empty virtual table with a single dummy column. */
    public static Table emptyTable(String name) {
        return new Table(name, Cols.listOf(col("dummy", DataType.TEXT)));
    }

    /** Map DataType to information_schema-style type name. */
    public static String pgTypeName(DataType dt) {
        switch (dt) {
            case SMALLINT:
            case SMALLSERIAL:
                return "smallint";
            case INTEGER:
            case SERIAL:
                return "integer";
            case BIGINT:
            case BIGSERIAL:
                return "bigint";
            case REAL:
                return "real";
            case DOUBLE_PRECISION:
                return "double precision";
            case NUMERIC:
                return "numeric";
            case VARCHAR:
                return "character varying";
            case CHAR:
                return "character";
            case TEXT:
                return "text";
            case NAME:
                return "name";
            case BOOLEAN:
                return "boolean";
            case DATE:
                return "date";
            case TIMESTAMP:
                return "timestamp without time zone";
            case TIMESTAMPTZ:
                return "timestamp with time zone";
            case TIME:
                return "time without time zone";
            case TIMETZ:
                return "time with time zone";
            case INTERVAL:
                return "interval";
            case BYTEA:
                return "bytea";
            case UUID:
                return "uuid";
            case JSON:
                return "json";
            case JSONB:
                return "jsonb";
            case INET:
                return "inet";
            case CIDR:
                return "cidr";
            case MACADDR:
                return "macaddr";
            case MACADDR8:
                return "macaddr8";
            case TSVECTOR:
                return "tsvector";
            case TSQUERY:
                return "tsquery";
            case POINT:
                return "point";
            case LINE:
                return "line";
            case LSEG:
                return "lseg";
            case BOX:
                return "box";
            case PATH:
                return "path";
            case POLYGON:
                return "polygon";
            case CIRCLE:
                return "circle";
            case MONEY:
                return "money";
            case BIT:
                return "bit";
            case VARBIT:
                return "bit varying";
            case XML:
                return "xml";
            case INT4RANGE:
                return "int4range";
            case INT8RANGE:
                return "int8range";
            case NUMRANGE:
                return "numrange";
            case DATERANGE:
                return "daterange";
            case TSRANGE:
                return "tsrange";
            case TSTZRANGE:
                return "tstzrange";
            case INT4MULTIRANGE:
                return "int4multirange";
            case INT8MULTIRANGE:
                return "int8multirange";
            case NUMMULTIRANGE:
                return "nummultirange";
            case DATEMULTIRANGE:
                return "datemultirange";
            case TSMULTIRANGE:
                return "tsmultirange";
            case TSTZMULTIRANGE:
                return "tstzmultirange";
            case ENUM:
                return "USER-DEFINED";
            case XID:
                return "xid";
            case OID:
                return "oid";
            case REGPROC:
                return "regproc";
            case REGCLASS:
                return "regclass";
            case REGTYPE:
                return "regtype";
            case OIDVECTOR:
                return "oidvector";
            case INT2VECTOR:
                return "int2vector";
            case HSTORE:
                return "hstore";
            default:
                // Every remaining type spells its SQL name the same way pg_type does. An array
                // is written as its element type followed by [], and a type with no case above
                // is still a type: throwing here would take out whichever catalog view is being
                // built, which is a far worse answer than the name itself.
                String pgName = dt.getPgName();
                if (pgName.startsWith("_")) {
                    DataType element = byPgName(pgName.substring(1));
                    return (element != null ? pgTypeName(element) : pgName.substring(1)) + "[]";
                }
                return pgName;
        }
    }

    /** The type spelled exactly {@code pgName} in pg_type, or null. */
    private static DataType byPgName(String pgName) {
        for (DataType dt : DataType.values()) {
            if (dt.getPgName().equals(pgName)) return dt;
        }
        return null;
    }

    // ---- Type modifiers ----

    /**
     * The interval field qualifiers, and the bit each one sets in an interval typmod. PostgreSQL
     * packs the fields a qualifier keeps into the high half of the modifier and the
     * fractional-seconds precision into the low half, so {@code interval day to second(3)} is one
     * number a client can read back as both.
     */
    private static final String[] INTERVAL_RANGES = {
            "year", "month", "day", "hour", "minute", "second",
            "year to month", "day to hour", "day to minute", "day to second",
            "hour to minute", "hour to second", "minute to second"};
    private static final int[] INTERVAL_MASKS = {
            1 << 2, 1 << 1, 1 << 3, 1 << 10, 1 << 11, 1 << 12,
            (1 << 2) | (1 << 1),
            (1 << 3) | (1 << 10),
            (1 << 3) | (1 << 10) | (1 << 11),
            (1 << 3) | (1 << 10) | (1 << 11) | (1 << 12),
            (1 << 10) | (1 << 11),
            (1 << 10) | (1 << 11) | (1 << 12),
            (1 << 11) | (1 << 12)};
    /** Every field: what an interval with no qualifier keeps. */
    private static final int INTERVAL_FULL_RANGE = 0x7FFF;

    /** The bits an interval qualifier sets, or the full range when there is none. */
    private static int intervalMask(String qualifier) {
        if (qualifier == null) return INTERVAL_FULL_RANGE;
        for (int i = 0; i < INTERVAL_RANGES.length; i++) {
            if (INTERVAL_RANGES[i].equals(qualifier.toLowerCase())) return INTERVAL_MASKS[i];
        }
        return INTERVAL_FULL_RANGE;
    }

    /** The qualifier a set of interval typmod bits stands for, or null for the full range. */
    private static String intervalQualifierOf(int mask) {
        for (int i = 0; i < INTERVAL_MASKS.length; i++) {
            if (INTERVAL_MASKS[i] == mask) return INTERVAL_RANGES[i];
        }
        return null;
    }

    /**
     * The type modifier PostgreSQL stores in {@code pg_attribute.atttypmod} for a column: the
     * declaration's width, precision or field qualifier packed the way {@code format_type} and
     * every client that decodes a column width expect to read it back. A column that declared
     * nothing has none, which is -1.
     */
    public static int attTypmod(Column c) {
        return attTypmod(c.getType(), c.getPrecision(), c.getScale(), c.getIntervalQualifier());
    }

    /** As above, from a declaration's parts. */
    public static int attTypmod(DataType dt, Integer precision, Integer scale, String intervalQualifier) {
        if (dt == null) return -1;
        switch (dt) {
            case VARCHAR:
            case CHAR:
                return precision == null ? -1 : precision.intValue() + 4;
            case NUMERIC:
                if (precision == null) return -1;
                return ((precision.intValue() << 16) | (scale == null ? 0 : scale.intValue())) + 4;
            case TIMESTAMP:
            case TIMESTAMPTZ:
            case TIME:
            case TIMETZ:
                return precision == null ? -1 : precision.intValue();
            case INTERVAL: {
                if (precision == null && intervalQualifier == null) return -1;
                int mask = intervalMask(intervalQualifier);
                int prec = precision == null ? 0xFFFF : precision.intValue();
                return (mask << 16) | prec;
            }
            case BIT:
            case VARBIT:
                return precision == null ? -1 : precision.intValue();
            default:
                return -1;
        }
    }

    /**
     * A type named the way {@code format_type} names it, with the modifier applied.
     *
     * <p>The modifier is part of what the type is: a client sizing an input reads
     * "character varying(10)[]" and knows both that it holds an array and how wide each element
     * may be, where a bare type name tells it neither.
     */
    public static String formatType(DataType dt, DataType elementType, int typmod) {
        if (elementType != null) return formatType(elementType, null, typmod) + "[]";
        if (dt == null) return "-";
        switch (dt) {
            case VARCHAR:
                return typmod >= 4 ? "character varying(" + (typmod - 4) + ")" : "character varying";
            case CHAR:
                return typmod >= 4 ? "character(" + (typmod - 4) + ")" : "bpchar";
            case NUMERIC:
                if (typmod >= 4) {
                    int raw = typmod - 4;
                    return "numeric(" + ((raw >> 16) & 0xFFFF) + "," + (raw & 0xFFFF) + ")";
                }
                return "numeric";
            case TIMESTAMP:
                return typmod >= 0 ? "timestamp(" + typmod + ") without time zone"
                        : "timestamp without time zone";
            case TIMESTAMPTZ:
                return typmod >= 0 ? "timestamp(" + typmod + ") with time zone"
                        : "timestamp with time zone";
            case TIME:
                return typmod >= 0 ? "time(" + typmod + ") without time zone" : "time without time zone";
            case TIMETZ:
                return typmod >= 0 ? "time(" + typmod + ") with time zone" : "time with time zone";
            case INTERVAL: {
                if (typmod < 0) return "interval";
                String qualifier = intervalQualifierOf((typmod >> 16) & 0x7FFF);
                int prec = typmod & 0xFFFF;
                StringBuilder sb = new StringBuilder("interval");
                if (qualifier != null) sb.append(' ').append(qualifier);
                if (prec != 0xFFFF) sb.append('(').append(prec).append(')');
                return sb.toString();
            }
            case BIT:
                return typmod >= 0 ? "bit(" + typmod + ")" : "bit";
            case VARBIT:
                return typmod >= 0 ? "bit varying(" + typmod + ")" : "bit varying";
            default:
                return pgTypeName(dt);
        }
    }

    /** Return numeric precision for information_schema.columns, or null if not numeric. */
    public static Integer numericPrecision(DataType dt) {
        switch (dt) {
            case SMALLINT:
                return 16;
            case INTEGER:
            case SERIAL:
                return 32;
            case BIGINT:
            case BIGSERIAL:
                return 64;
            case REAL:
                return 24;
            case DOUBLE_PRECISION:
                return 53;
            default:
                return null;
        }
    }

    /**
     * Render a domain CHECK expression the way PG's ruleutils deparser does, resolving
     * VALUE against the domain's base type so the implicit casts PG inserted show up —
     * e.g. a numeric domain's {@code CHECK (VALUE > 0)} renders "(VALUE > (0)::numeric)".
     */
    public static String renderDomainCheck(DomainType domain,
                                           com.memgres.engine.parser.ast.Expression parsed) {
        if (parsed == null) return "";
        RuleDeparser.PgType valueType = domain == null ? null
                : (domain.getBaseTypeName() != null && domain.getBaseType() == null
                        ? RuleDeparser.PgType.custom(domain.getBaseTypeName())
                        : RuleDeparser.PgType.of(domain.getBaseType()));
        return RuleDeparser.deparse(parsed, RuleDeparser.forDomain(valueType));
    }

    /** Format a column default for information_schema / pg_attrdef, matching PG conventions. */
    public static String formatColumnDefault(Column col) {
        String def = col.getDefaultValue();
        if (def == null) return null;
        if (def.startsWith("__identity__")) return null;
        // H14: DEFAULT now() renders as now() (a plain function call); the
        // CURRENT_TIMESTAMP keyword renders as CURRENT_TIMESTAMP (matching PG).
        if (def.equalsIgnoreCase("now()")) return "now()";
        if (def.equalsIgnoreCase("current_timestamp()")
                || def.equalsIgnoreCase("current_timestamp")) return "CURRENT_TIMESTAMP";
        if (def.equalsIgnoreCase("current_date") || def.equalsIgnoreCase("current_date()")) return "CURRENT_DATE";
        if (def.toLowerCase().startsWith("nextval(")) return def;
        if (def.startsWith("'") && def.endsWith("'")) {
            String typeName = pgTypeName(col.getType());
            if (col.getDomainTypeName() != null) typeName = col.getDomainTypeName();
            else if (col.getEnumTypeName() != null) typeName = col.getEnumTypeName();
            return def + "::" + typeName;
        }
        return def;
    }

    /**
     * Format a domain's default the way information_schema.domains reports it. PG stores the
     * default already coerced to the domain's base type, so a string literal comes back with the
     * cast that coercion left behind: {@code 'x'::character varying}.
     */
    public static String formatDomainDefault(DomainType domain) {
        String def = domain.getDefaultValue();
        if (def == null) return null;
        if (def.startsWith("'") && def.endsWith("'") && domain.getBaseType() != null) {
            return def + "::" + pgTypeName(domain.getBaseType());
        }
        return def;
    }

    /** Map FK action to pg_constraint single-char code. */
    public static String fkActionCode(StoredConstraint.FkAction action) {
        if (action == null) return "a";
        switch (action) {
            case NO_ACTION:
                return "a";
            case RESTRICT:
                return "r";
            case CASCADE:
                return "c";
            case SET_NULL:
                return "n";
            case SET_DEFAULT:
                return "d";
            default:
                throw new IllegalStateException("Unknown FK action: " + action);
        }
    }

    /** Map FK action to information_schema string. */
    public static String fkActionToString(StoredConstraint.FkAction action) {
        switch (action) {
            case CASCADE:
                return "CASCADE";
            case SET_NULL:
                return "SET NULL";
            case SET_DEFAULT:
                return "SET DEFAULT";
            case RESTRICT:
                return "RESTRICT";
            case NO_ACTION:
                return "NO ACTION";
            default:
                throw new IllegalStateException("Unknown FK action: " + action);
        }
    }

    /** Default max value for a sequence based on its data type. */
    public static long getDefaultSeqMax(DataType dt) {
        switch (dt) {
            case SMALLINT:
            case SMALLSERIAL:
                return 32767L;
            case INTEGER:
            case SERIAL:
                return 2147483647L;
            default:
                return Long.MAX_VALUE;
        }
    }

    /** Map ALTER DEFAULT PRIVILEGES object type to pg_default_acl single char. */
    public static char objectTypeChar(String objectType) {
        if (objectType == null) return 'r';
        switch (objectType.toUpperCase()) {
            case "TABLES":
                return 'r';
            case "SEQUENCES":
                return 'S';
            case "FUNCTIONS":
            case "ROUTINES":
                return 'f';
            case "TYPES":
                return 'T';
            case "SCHEMAS":
                return 'n';
            default:
                return 'r';
        }
    }

    /** Convert column names to PG attnum array string, e.g. "{1,3}". */
    public static List<Object> columnNamesToAttnums(Table table, List<String> columns) {
        if (columns == null || columns.isEmpty()) return null;
        List<Object> attnums = new java.util.ArrayList<>();
        for (String col : columns) {
            int idx = table.getColumnIndex(col);
            attnums.add(idx + 1);
        }
        return attnums;
    }

    /** Find the first PK or UNIQUE constraint name on the given table (for FK referential_constraints). */
    public static String findReferencedConstraintName(Table t) {
        if (t == null) return null;
        for (StoredConstraint sc : t.getConstraints()) {
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY ||
                    sc.getType() == StoredConstraint.Type.UNIQUE) {
                return sc.getName();
            }
        }
        return null;
    }

    /** Find a table by name across all schemas. */
    public static Table findTable(Database database, String tableName) {
        for (Schema schema : database.getSchemas().values()) {
            Table t = schema.getTable(tableName);
            if (t != null) return t;
        }
        return null;
    }

    /** Collect all sequence names (explicit + implicit from SERIAL/identity columns). */
    public static java.util.List<String> getSequenceNames(Database database) {
        java.util.Set<String> names = new java.util.LinkedHashSet<>(database.getSequences().keySet());
        for (java.util.Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (java.util.Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                for (Column col : t.getColumns()) {
                    if (col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL || col.getType() == DataType.SMALLSERIAL) {
                        names.add(t.getName() + "_" + col.getName() + "_seq");
                    } else if (col.getDefaultValue() != null && col.getDefaultValue().contains("__identity__")) {
                        names.add(t.getName() + "_" + col.getName() + "_seq");
                    }
                }
            }
        }
        return new java.util.ArrayList<>(names);
    }

    /** Determine the data type for a sequence based on the source SERIAL column type. */
    public static DataType getSequenceDataType(Database database, String seqName) {
        for (Schema schema : database.getSchemas().values()) {
            for (Table t : schema.getTables().values()) {
                for (Column col : t.getColumns()) {
                    String expected = t.getName() + "_" + col.getName() + "_seq";
                    if (expected.equalsIgnoreCase(seqName)) {
                        switch (col.getType()) {
                            case SMALLSERIAL:
                            case SMALLINT:
                                return DataType.SMALLINT;
                            case SERIAL:
                            case INTEGER:
                                return DataType.INTEGER;
                            default:
                                return DataType.BIGINT;
                        }
                    }
                }
            }
        }
        return DataType.BIGINT;
    }

    /** Look up a table by a possibly schema-qualified name; null when it cannot be found. */
    public static Table resolveTable(Database database, String qualifiedName) {
        if (database == null || qualifiedName == null) return null;
        String schema = null;
        String name = qualifiedName;
        int dot = qualifiedName.lastIndexOf('.');
        if (dot > 0) {
            schema = qualifiedName.substring(0, dot);
            name = qualifiedName.substring(dot + 1);
        }
        if (schema != null) {
            Schema s = database.getSchemas().get(schema);
            return s == null ? null : s.getTables().get(name);
        }
        for (Schema s : database.getSchemas().values()) {
            Table t = s.getTables().get(name);
            if (t != null) return t;
        }
        return null;
    }

    /**
     * Renders index key columns the way pg_get_indexdef does: a plain column prints as
     * its (quoted-if-needed) name, while an expression is deparsed with the implicit
     * casts PG's analyzer inserted and wrapped in parentheses unless it is a bare
     * function call.
     */
    public static List<String> deparseIndexColumns(Database database, String qualifiedTable,
                                                   List<String> cols) {
        List<String> out = new java.util.ArrayList<>();
        if (cols == null) return out;
        Table t = resolveTable(database, qualifiedTable);
        RuleDeparser.ColumnTypes types = RuleDeparser.forTable(t);
        for (String col : cols) {
            out.add(deparseIndexElement(t, types, col));
        }
        return out;
    }

    private static String deparseIndexElement(Table t, RuleDeparser.ColumnTypes types, String col) {
        if (col == null) return null;
        if (t != null) {
            int idx = t.getColumnIndex(col);
            if (idx >= 0) return RuleDeparser.quoteIdentifier(t.getColumns().get(idx).getName());
        }
        try {
            return RuleDeparser.deparseIndexElement(
                    com.memgres.engine.parser.Parser.parseExpression(col), types);
        } catch (Exception e) {
            return col;
        }
    }

    /** Renders a partial-index predicate the way pg_get_indexdef does. */
    public static String deparseIndexPredicate(Database database, String qualifiedTable, String where) {
        if (where == null || where.isEmpty()) return where;
        Table t = resolveTable(database, qualifiedTable);
        try {
            return RuleDeparser.deparse(
                    com.memgres.engine.parser.Parser.parseExpression(where), RuleDeparser.forTable(t));
        } catch (Exception e) {
            return where;
        }
    }

    /** Resolve the owner OID for an object key, defaulting to 10. */
    public static int resolveOwnerOid(Database database, OidSupplier oids, String objectKey) {
        String owner = database.getObjectOwner(objectKey);
        if (owner != null) {
            return oids.oid("role:" + owner);
        }
        return 10;
    }
}
