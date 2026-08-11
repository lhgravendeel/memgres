package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

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
            case TEXT_ARRAY:
                return "text[]";
            case INT4_ARRAY:
                return "integer[]";
            case ACLITEM_ARRAY:
                return "aclitem[]";
            case NAME_ARRAY:
                return "name[]";
            case BOOL_ARRAY:
                return "boolean[]";
            case INT2_ARRAY:
                return "smallint[]";
            case INT8_ARRAY:
                return "bigint[]";
            case FLOAT4_ARRAY:
                return "real[]";
            case FLOAT8_ARRAY:
                return "double precision[]";
            case NUMERIC_ARRAY:
                return "numeric[]";
            case VARCHAR_ARRAY:
                return "character varying[]";
            case CHAR_ARRAY:
                return "character[]";
            case DATE_ARRAY:
                return "date[]";
            case TIMESTAMP_ARRAY:
                return "timestamp without time zone[]";
            case TIMESTAMPTZ_ARRAY:
                return "timestamp with time zone[]";
            case TIME_ARRAY:
                return "time without time zone[]";
            case TIMETZ_ARRAY:
                return "time with time zone[]";
            case INTERVAL_ARRAY:
                return "interval[]";
            case UUID_ARRAY:
                return "uuid[]";
            case BYTEA_ARRAY:
                return "bytea[]";
            case JSON_ARRAY:
                return "json[]";
            case JSONB_ARRAY:
                return "jsonb[]";
            case INET_ARRAY:
                return "inet[]";
            case RECORD_ARRAY:
                return "record[]";
            case OID_ARRAY:
                return "oid[]";
            case INTERNAL_CHAR_ARRAY:
                return "\"char\"[]";
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
            // The types the system catalogs declare columns of. PostgreSQL spells its
            // single-byte flag type with the quotes, and a caller pasting the name into a
            // cast needs them: "char" is a different type from char.
            case INTERNAL_CHAR:
                return "\"char\"";
            case PG_NODE_TREE:
                return "pg_node_tree";
            case ANYARRAY:
                return "anyarray";
            case PG_LSN:
                return "pg_lsn";
            case PG_NDISTINCT:
                return "pg_ndistinct";
            case PG_DEPENDENCIES:
                return "pg_dependencies";
            case PG_MCV_LIST:
                return "pg_mcv_list";
            case RECORD:
                return "record";
            case VOID:
                return "void";
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
                // Unmodified bpchar is "character", the SQL name, not the catalog's typname:
                // format_type prints the spelling a client could write back, and PG's identity
                // form of length(bpchar) is length(character) for exactly that reason.
                return typmod >= 4 ? "character(" + (typmod - 4) + ")" : "character";
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
                int mask = (typmod >> 16) & 0x7FFF;
                // The high half is a field mask, not a number: an interval typmod that does not
                // name a qualifier PG can write is not a narrower interval, it is not a typmod at
                // all, and PG refuses to print one rather than inventing 'interval(3)'.
                if (mask != INTERVAL_FULL_RANGE && intervalQualifierOf(mask) == null) {
                    throw new MemgresException(
                            "invalid INTERVAL typmod: 0x" + Integer.toHexString(typmod), "XX000");
                }
                String qualifier = intervalQualifierOf(mask);
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
                return withPlainTypmod(dt, pgTypeName(dt), typmod);
        }
    }

    /**
     * PostgreSQL's {@code printTypmod} for a type that has no typmod output function of its own:
     * the modifier is shown as the plain number it is. {@code format_type(1082, 3)} is
     * {@code date(3)} and {@code format_type('_date'::regtype, 3)} is {@code date(3)[]}. Dropping
     * it made format_type disagree with itself, answering one name for two different typmods.
     *
     * <p>The names PostgreSQL writes out by hand never carry one — measured on PG 18, every
     * built-in whose {@code format_type(oid, 5)} shows no {@code (5)} is one of bool, float4,
     * float8, int2, int4, int8 and json, plus the three that fold the modifier into their own
     * spelling (bpchar, varchar, numeric) and are handled above.
     */
    public static String withPlainTypmod(DataType dt, String name, int typmod) {
        if (typmod < 0 || dt == null || name == null) return name;
        if (TYPMOD_IGNORED.contains(dt)) return name;
        // A name that already carries a modifier, or that is an array spelled with brackets, has
        // had the modifier applied where it belongs.
        if (name.indexOf('(') >= 0 || name.endsWith("]")) return name;
        return name + "(" + typmod + ")";
    }

    /** The types {@code format_type} names without ever showing a modifier. */
    private static final Set<DataType> TYPMOD_IGNORED = java.util.Collections.unmodifiableSet(
            EnumSet.of(DataType.BOOLEAN, DataType.REAL, DataType.DOUBLE_PRECISION,
                    DataType.SMALLINT, DataType.INTEGER, DataType.BIGINT,
                    DataType.SERIAL, DataType.BIGSERIAL, DataType.SMALLSERIAL,
                    DataType.JSON));

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

    /**
     * Render a generated column's expression the way PostgreSQL's deparser renders it.
     *
     * <p>The clause is kept as the text it was written as, and echoing that text back handed the
     * reader a token stream rather than a definition -- {@code upper ( a :: TEXT )}. PostgreSQL
     * prints the analysed tree: it brackets what needs bracketing and leaves alone what does not
     * ({@code a} for a bare column reference, {@code (a * 2)} for an operator), names types in
     * pg_catalog's own spelling, and shows the casts parse analysis inserted, so {@code b / 2}
     * over a numeric column comes back as {@code (b / (2)::numeric)}.
     *
     * @param owner the relation the column belongs to; its column types are what let the deparser
     *              decide which casts PostgreSQL would have inserted
     */
    public static String renderGeneratedExpr(Table owner, Column col) {
        String raw = col == null ? null : col.getGeneratedExpr();
        if (raw == null) return null;
        try {
            com.memgres.engine.parser.ast.Expression parsed =
                    com.memgres.engine.parser.Parser.parseExpression(raw);
            if (parsed == null) return raw;
            return RuleDeparser.deparse(parsed, RuleDeparser.forTable(owner));
        } catch (RuntimeException e) {
            // An expression that will not parse is reported as it was written
            return raw;
        }
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
            // The type a default is labelled with is named as the reader would write it, which
            // for a type in a schema of its own is its bare name.
            String typeName = pgTypeName(col.getType());
            if (col.getDomainTypeName() != null) {
                typeName = TypeNamespace.nameOfKey(col.getDomainTypeName());
            } else if (col.getEnumTypeName() != null) {
                typeName = TypeNamespace.nameOfKey(col.getEnumTypeName());
            }
            def = def + "::" + typeName;
        }
        return deparseStoredDefault(def);
    }

    /**
     * A stored default reported the way {@code pg_get_expr} reports it.
     *
     * <p>PostgreSQL never echoes the text a default was written as. It prints the tree parse
     * analysis left behind, in which a cast of a constant to the type that constant already reads
     * as has folded away, a bare literal carries the label its column's type gave it, and every
     * operator expression wears the parentheses the unpretty form always puts round one -- so
     * {@code 1::int} is reported as {@code 1}, {@code 1.9::numeric} as {@code 1.9} and
     * {@code 2 + 3} as {@code (2 + 3)}. Echoing the written text instead gave a catalogue that
     * disagreed with the one pg_dump reads, for a default that behaves identically either way.
     */
    private static String deparseStoredDefault(String def) {
        try {
            com.memgres.engine.parser.ast.Expression parsed =
                    com.memgres.engine.parser.Parser.parseExpression(def);
            if (parsed == null) return def;
            return RuleDeparser.deparse(parsed, null);
        } catch (RuntimeException e) {
            // A default that will not parse is reported as it was written
            return def;
        }
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
            // A dropped column does not give its number back, so a key holds the relation's
            // attribute number rather than the column's position among the columns that are left.
            attnums.add(table.attnumOf(col));
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

    /**
     * Every sequence in the database (explicit + implicit from SERIAL/identity columns), named
     * {@code schema.name}. A sequence is a relation, so two schemas may each hold one of the same
     * name and only the pair identifies it.
     */
    public static java.util.List<String> getSequenceNames(Database database) {
        // Every serial and identity column has a real sequence, created with the column, so the
        // registry is the whole list. Composing <table>_<column>_seq for such columns as well
        // named relations nothing backs: after a table or column rename the composed name is not
        // the sequence's, and a partition that inherits an identity column has no sequence at all.
        return new java.util.ArrayList<>(database.getSequences().keySet());
    }

    /** The schema half of a {@code schema.name} pair from {@link #getSequenceNames}. */
    public static String schemaOf(String qualified) {
        int dot = qualified.indexOf('.');
        return dot > 0 ? qualified.substring(0, dot) : "public";
    }

    /** The name half of a {@code schema.name} pair from {@link #getSequenceNames}. */
    public static String nameOf(String qualified) {
        int dot = qualified.indexOf('.');
        return dot > 0 ? qualified.substring(dot + 1) : qualified;
    }

    /**
     * A sequence's data type, which is what {@code AS} settled when it was created and what
     * pg_sequence.seqtypid reports. This used to be guessed from a column whose composed
     * {@code <table>_<column>_seq} name matched, so a standalone {@code CREATE SEQUENCE ... AS
     * integer} — which matches no column — always answered bigint.
     */
    public static DataType getSequenceDataType(Database database, String qualifiedSeqName) {
        Sequence seq = database.getSequence(qualifiedSeqName);
        if (seq != null) {
            String declared = seq.getDataType();
            if ("smallint".equals(declared)) return DataType.SMALLINT;
            if ("integer".equals(declared)) return DataType.INTEGER;
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

    /**
     * The per-key options an index definition prints. An operator class is left out where it is
     * the one the key's own type takes by default: PostgreSQL prints a class only when reading the
     * definition back would otherwise choose a different one.
     */
    public static List<String> deparseIndexOptions(Database database, String qualifiedTable,
                                                   String method, List<String> cols,
                                                   List<String> options) {
        if (options == null) return null;
        Table t = resolveTable(database, qualifiedTable);
        List<String> out = new java.util.ArrayList<>(options.size());
        for (int i = 0; i < options.size(); i++) {
            String key = cols != null && i < cols.size() ? cols.get(i) : null;
            out.add(withoutDefaultOpclass(database, t, method, key, options.get(i)));
        }
        return out;
    }

    private static String withoutDefaultOpclass(Database database, Table t, String method,
                                                String key, String opts) {
        if (opts == null || !opts.contains("opclass:")) return opts;
        String am = method == null || method.isEmpty() ? "btree" : method.toLowerCase();
        String written = DdlIndexValidator.defaultOpclass(am, indexKeyTypeName(database, t, key));
        if (written == null) return opts;
        StringBuilder kept = new StringBuilder();
        for (String part : opts.split(" ")) {
            if (part.startsWith("opclass:")
                    && written.equalsIgnoreCase(part.substring("opclass:".length()))) {
                continue;
            }
            if (kept.length() > 0) kept.append(' ');
            kept.append(part);
        }
        return kept.toString();
    }

    /**
     * The type an index key is of: a column's declared type, or the type the expression comes out
     * as. An operator class belongs to a type, so this is what decides whether the one written
     * down is the one the key would have taken anyway.
     */
    private static String indexKeyTypeName(Database database, Table t, String key) {
        if (t == null || key == null) return null;
        int idx = t.getColumnIndex(key);
        if (idx >= 0) {
            Column col = t.getColumns().get(idx);
            // A domain has no operator class of its own: it indexes through the class the type
            // underneath it takes, so that is the type the written class is compared against.
            if (col.getDomainTypeName() != null) {
                DomainType domain = database == null ? null
                        : database.getDomain(col.getDomainTypeName());
                return domain == null || domain.getBaseType() == null
                        ? null : pgTypeName(domain.getBaseType());
            }
            return DdlIndexValidator.indexedTypeName(col);
        }
        try {
            RuleDeparser.PgType type = RuleDeparser.typeOf(
                    com.memgres.engine.parser.Parser.parseExpression(key), RuleDeparser.forTable(t));
            return DdlIndexValidator.indexedTypeName(type);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * The relation an index definition is written against, as {@code pg_get_indexdef} writes it.
     *
     * <p>An index on a partitioned table stores no rows of its own: it is the parent that the
     * partitions' own indexes hang from. PostgreSQL therefore writes it {@code ON ONLY}, so that
     * replaying the definition rebuilds that parent alone instead of indexing every partition
     * over again.
     */
    public static String indexRelationRef(Database database, String qualifiedTable, String shown) {
        Table owner = resolveTable(database, qualifiedTable);
        return owner != null && owner.getPartitionStrategy() != null ? "ONLY " + shown : shown;
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
