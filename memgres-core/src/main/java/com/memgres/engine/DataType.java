package com.memgres.engine;

/**
 * PostgreSQL-compatible data types.
 */
public enum DataType {

    // Numeric types
    SMALLINT(21, "int2"),
    INTEGER(23, "int4"),
    BIGINT(20, "int8"),
    REAL(700, "float4"),
    DOUBLE_PRECISION(701, "float8"),
    NUMERIC(1700, "numeric"),

    // Character types
    VARCHAR(1043, "varchar"),
    CHAR(1042, "bpchar"),
    TEXT(25, "text"),
    NAME(19, "name"),

    // Boolean
    BOOLEAN(16, "bool"),

    // Date/time types
    DATE(1082, "date"),
    TIMESTAMP(1114, "timestamp"),
    TIMESTAMPTZ(1184, "timestamptz"),
    TIME(1083, "time"),
    TIMETZ(1266, "timetz"),
    INTERVAL(1186, "interval"),

    // Network types
    INET(869, "inet"),
    CIDR(650, "cidr"),
    MACADDR(829, "macaddr"),
    MACADDR8(774, "macaddr8"),

    // Binary
    BYTEA(17, "bytea"),

    // UUID
    UUID(2950, "uuid"),

    // JSON
    JSON(114, "json"),
    JSONB(3802, "jsonb"),

    // Money type
    MONEY(790, "money"),

    // Serial (auto-increment) types
    SERIAL(23, "serial"),
    BIGSERIAL(20, "bigserial"),
    SMALLSERIAL(21, "smallserial"),

    // Extension types
    HSTORE(90001, "hstore"),

    // Full-text search
    TSVECTOR(3614, "tsvector"),
    TSQUERY(3615, "tsquery"),

    // Geometric types
    POINT(600, "point"),
    LINE(628, "line"),
    LSEG(601, "lseg"),
    BOX(603, "box"),
    PATH(602, "path"),
    POLYGON(604, "polygon"),
    CIRCLE(718, "circle"),

    // XML type
    XML(142, "xml"),

    // Bit string types
    BIT(1560, "bit"),
    VARBIT(1562, "varbit"),

    // Range types
    INT4RANGE(3904, "int4range"),
    INT8RANGE(3926, "int8range"),
    NUMRANGE(3906, "numrange"),
    DATERANGE(3912, "daterange"),
    TSRANGE(3908, "tsrange"),
    TSTZRANGE(3910, "tstzrange"),

    // Multirange types (PG 14+)
    INT4MULTIRANGE(4451, "int4multirange"),
    INT8MULTIRANGE(4532, "int8multirange"),
    NUMMULTIRANGE(4533, "nummultirange"),
    DATEMULTIRANGE(4534, "datemultirange"),
    TSMULTIRANGE(4535, "tsmultirange"),
    TSTZMULTIRANGE(4536, "tstzmultirange"),

    // Array types (used for system catalog columns and array expressions)
    BOOL_ARRAY(1000, "_bool"),
    INT2_ARRAY(1005, "_int2"),
    INT4_ARRAY(1007, "_int4"),
    INT8_ARRAY(1016, "_int8"),
    FLOAT4_ARRAY(1021, "_float4"),
    FLOAT8_ARRAY(1022, "_float8"),
    NUMERIC_ARRAY(1231, "_numeric"),
    TEXT_ARRAY(1009, "_text"),
    VARCHAR_ARRAY(1015, "_varchar"),
    CHAR_ARRAY(1014, "_bpchar"),
    NAME_ARRAY(1003, "_name"),
    DATE_ARRAY(1182, "_date"),
    TIMESTAMP_ARRAY(1115, "_timestamp"),
    TIMESTAMPTZ_ARRAY(1185, "_timestamptz"),
    TIME_ARRAY(1183, "_time"),
    TIMETZ_ARRAY(1270, "_timetz"),
    UUID_ARRAY(2951, "_uuid"),
    BYTEA_ARRAY(1001, "_bytea"),
    INTERVAL_ARRAY(1187, "_interval"),
    JSON_ARRAY(199, "_json"),
    JSONB_ARRAY(3807, "_jsonb"),
    INET_ARRAY(1041, "_inet"),
    ACLITEM_ARRAY(1034, "_aclitem"),
    RECORD_ARRAY(2287, "_record"),

    // OID type
    OID(26, "oid"),

    // Object-identifier aliases. Catalog columns holding a function or relation reference are
    // these types in PG, not text, so a query comparing one to 0 or joining on it resolves.
    REGPROC(24, "regproc"),
    REGCLASS(2205, "regclass"),
    REGTYPE(2206, "regtype"),

    // The 0-based vector types the catalogs use for argument and key lists
    INT2VECTOR(22, "int2vector"),
    OIDVECTOR(30, "oidvector"),

    // Transaction ID type
    XID(28, "xid"),

    // Record (composite) type
    RECORD(2249, "record"),

    // Pseudo-type returned by a function declared RETURNS void
    VOID(2278, "void"),

    // Custom enum placeholder; actual enum types are resolved by name
    ENUM(0, "enum");

    private final int oid;
    private final String pgName;

    DataType(int oid, String pgName) {
        this.oid = oid;
        this.pgName = pgName;
    }

    public int getOid() {
        return oid;
    }

    public String getPgName() {
        return pgName;
    }

    public static DataType fromPgName(String name) {
        String normalized = name.toLowerCase().trim();
        switch (normalized) {
            case "int2":
            case "smallint":
                return SMALLINT;
            case "int":
            case "int4":
            case "integer":
                return INTEGER;
            case "int8":
            case "bigint":
                return BIGINT;
            case "float4":
            case "real":
                return REAL;
            case "float":
            case "float8":
            case "double precision":
            case "double":
                return DOUBLE_PRECISION;
            case "numeric":
            case "decimal":
                return NUMERIC;
            case "varchar":
            case "character varying":
                return VARCHAR;
            case "char":
            case "character":
            case "bpchar":
                return CHAR;
            case "text":
            case "citext":
                return TEXT;
            case "name":
                return NAME;
            case "oid":
                return OID;
            case "regclass":
            case "regtype":
            case "regproc":
                return INTEGER;
            case "bool":
            case "boolean":
                return BOOLEAN;
            case "void":
                return VOID;
            case "date":
                return DATE;
            case "timestamp":
            case "timestamp without time zone":
                return TIMESTAMP;
            case "timestamptz":
            case "timestamp with time zone":
                return TIMESTAMPTZ;
            case "time":
            case "time without time zone":
                return TIME;
            case "timetz":
            case "time with time zone":
                return TIMETZ;
            case "interval":
                return INTERVAL;
            case "bytea":
                return BYTEA;
            case "uuid":
                return UUID;
            case "json":
                return JSON;
            case "jsonb":
                return JSONB;
            case "hstore":
                return HSTORE;
            case "serial":
                return SERIAL;
            case "bigserial":
                return BIGSERIAL;
            case "smallserial":
                return SMALLSERIAL;
            case "money":
                return MONEY;
            case "inet":
                return INET;
            case "cidr":
                return CIDR;
            case "macaddr":
                return MACADDR;
            case "macaddr8":
                return MACADDR8;
            case "tsvector":
                return TSVECTOR;
            case "tsquery":
                return TSQUERY;
            case "point":
                return POINT;
            case "line":
                return LINE;
            case "lseg":
                return LSEG;
            case "box":
                return BOX;
            case "path":
                return PATH;
            case "polygon":
                return POLYGON;
            case "circle":
                return CIRCLE;
            case "bit":
                return BIT;
            case "varbit":
            case "bit varying":
                return VARBIT;
            case "xml":
                return XML;
            case "int4range":
                return INT4RANGE;
            case "int8range":
                return INT8RANGE;
            case "numrange":
                return NUMRANGE;
            case "daterange":
                return DATERANGE;
            case "tsrange":
                return TSRANGE;
            case "tstzrange":
                return TSTZRANGE;
            case "int4multirange":
                return INT4MULTIRANGE;
            case "int8multirange":
                return INT8MULTIRANGE;
            case "nummultirange":
                return NUMMULTIRANGE;
            case "datemultirange":
                return DATEMULTIRANGE;
            case "tsmultirange":
                return TSMULTIRANGE;
            case "tstzmultirange":
                return TSTZMULTIRANGE;
            case "text[]":
            case "_text":
                return TEXT_ARRAY;
            case "int[]":
            case "int4[]":
            case "integer[]":
            case "_int4":
                return INT4_ARRAY;
            case "name[]":
            case "_name":
                return NAME_ARRAY;
            case "aclitem[]":
            case "_aclitem":
                return ACLITEM_ARRAY;
            default:
                // Any other array spelling resolves through its element type, so a cast to
                // date[] or uuid[] carries its own array OID instead of falling back to text.
                if (normalized.endsWith("[]")) {
                    return arrayOf(fromPgName(normalized.substring(0, normalized.length() - 2)));
                }
                if (normalized.startsWith("_")) {
                    return arrayOf(fromPgName(normalized.substring(1)));
                }
                return null;
        }
    }

    /** The array type over an element type, or null when memgres carries no OID for it. */
    public static DataType arrayOf(DataType element) {
        if (element == null) return null;
        switch (element) {
            case BOOLEAN: return BOOL_ARRAY;
            case SMALLINT: case SMALLSERIAL: return INT2_ARRAY;
            case INTEGER: case SERIAL: return INT4_ARRAY;
            case BIGINT: case BIGSERIAL: return INT8_ARRAY;
            case REAL: return FLOAT4_ARRAY;
            case DOUBLE_PRECISION: return FLOAT8_ARRAY;
            case NUMERIC: return NUMERIC_ARRAY;
            case TEXT: return TEXT_ARRAY;
            case VARCHAR: return VARCHAR_ARRAY;
            case CHAR: return CHAR_ARRAY;
            case NAME: return NAME_ARRAY;
            case DATE: return DATE_ARRAY;
            case TIMESTAMP: return TIMESTAMP_ARRAY;
            case TIMESTAMPTZ: return TIMESTAMPTZ_ARRAY;
            case TIME: return TIME_ARRAY;
            case TIMETZ: return TIMETZ_ARRAY;
            case INTERVAL: return INTERVAL_ARRAY;
            case UUID: return UUID_ARRAY;
            case BYTEA: return BYTEA_ARRAY;
            case JSON: return JSON_ARRAY;
            case JSONB: return JSONB_ARRAY;
            case INET: return INET_ARRAY;
            case RECORD: return RECORD_ARRAY;
            default: return null;
        }
    }

    /** The element type an array type is built over, or null when it is not an array type. */
    public static DataType elementOf(DataType array) {
        if (array == null) return null;
        switch (array) {
            case BOOL_ARRAY: return BOOLEAN;
            case INT2_ARRAY: return SMALLINT;
            case INT4_ARRAY: return INTEGER;
            case INT8_ARRAY: return BIGINT;
            case FLOAT4_ARRAY: return REAL;
            case FLOAT8_ARRAY: return DOUBLE_PRECISION;
            case NUMERIC_ARRAY: return NUMERIC;
            case TEXT_ARRAY: return TEXT;
            case VARCHAR_ARRAY: return VARCHAR;
            case CHAR_ARRAY: return CHAR;
            case NAME_ARRAY: return NAME;
            case DATE_ARRAY: return DATE;
            case TIMESTAMP_ARRAY: return TIMESTAMP;
            case TIMESTAMPTZ_ARRAY: return TIMESTAMPTZ;
            case TIME_ARRAY: return TIME;
            case TIMETZ_ARRAY: return TIMETZ;
            case INTERVAL_ARRAY: return INTERVAL;
            case UUID_ARRAY: return UUID;
            case BYTEA_ARRAY: return BYTEA;
            case JSON_ARRAY: return JSON;
            case JSONB_ARRAY: return JSONB;
            case INET_ARRAY: return INET;
            case RECORD_ARRAY: return RECORD;
            default: return null;
        }
    }

    /** True for the array types, whose operators answer in the array type rather than in text. */
    public static boolean isArrayType(DataType t) {
        return elementOf(t) != null;
    }

    /** Look up DataType by OID. Returns null if not found. */
    public static DataType fromOid(int oid) {
        for (DataType dt : values()) {
            if (dt.oid == oid && dt != SERIAL && dt != BIGSERIAL && dt != SMALLSERIAL && dt != ENUM) {
                return dt;
            }
        }
        return null;
    }

    /** Return the regtype display name (PG format used in pg_prepared_statements.result_types). */
    public String toRegtypeDisplay() {
        switch (this) {
            case SMALLINT: return "smallint";
            case INTEGER: case SERIAL: return "integer";
            case BIGINT: case BIGSERIAL: return "bigint";
            case REAL: return "real";
            case DOUBLE_PRECISION: return "double precision";
            case NUMERIC: return "numeric";
            case VARCHAR: return "character varying";
            case CHAR: return "character";
            case TEXT: return "text";
            case NAME: return "name";
            case BOOLEAN: return "boolean";
            case DATE: return "date";
            case TIMESTAMP: return "timestamp without time zone";
            case TIMESTAMPTZ: return "timestamp with time zone";
            case TIME: return "time without time zone";
            case TIMETZ: return "time with time zone";
            case INTERVAL: return "interval";
            case BYTEA: return "bytea";
            case UUID: return "uuid";
            case JSON: return "json";
            case JSONB: return "jsonb";
            case MONEY: return "money";
            case INET: return "inet";
            case CIDR: return "cidr";
            case MACADDR: return "macaddr";
            case MACADDR8: return "macaddr8";
            case HSTORE: return "hstore";
            case TSVECTOR: return "tsvector";
            case TSQUERY: return "tsquery";
            case POINT: return "point";
            case LINE: return "line";
            case LSEG: return "lseg";
            case BOX: return "box";
            case PATH: return "path";
            case POLYGON: return "polygon";
            case CIRCLE: return "circle";
            case XML: return "xml";
            case BIT: return "bit";
            case VARBIT: return "bit varying";
            case INT4RANGE: return "int4range";
            case INT8RANGE: return "int8range";
            case NUMRANGE: return "numrange";
            case DATERANGE: return "daterange";
            case TSRANGE: return "tsrange";
            case TSTZRANGE: return "tstzrange";
            case INT4MULTIRANGE: return "int4multirange";
            case INT8MULTIRANGE: return "int8multirange";
            case NUMMULTIRANGE: return "nummultirange";
            case DATEMULTIRANGE: return "datemultirange";
            case TSMULTIRANGE: return "tsmultirange";
            case TSTZMULTIRANGE: return "tstzmultirange";
            case TEXT_ARRAY: return "text[]";
            case INT4_ARRAY: return "integer[]";
            case NAME_ARRAY: return "name[]";
            case ACLITEM_ARRAY: return "aclitem[]";
            default: return pgName;
        }
    }

    /** Type names that fromPgName folds onto another type; their own spelling is canonical. */
    private static final java.util.Set<String> ALIASED_ONTO_OTHER_TYPE =
            new java.util.HashSet<>(java.util.Arrays.asList("regclass", "regtype", "regproc", "citext"));

    /**
     * The name PostgreSQL uses for a type in messages and catalogs: {@code int} is reported as
     * {@code integer}, {@code float8} as {@code double precision}. Unknown names (user-defined
     * types) are returned lowercased and otherwise unchanged.
     */
    public static String canonicalName(String typeName) {
        if (typeName == null) return null;
        String lower = typeName.trim().toLowerCase();
        if (lower.isEmpty() || ALIASED_ONTO_OTHER_TYPE.contains(lower)) return lower;
        int paren = lower.indexOf('(');
        String base = paren > 0 ? lower.substring(0, paren).trim() : lower;
        boolean array = base.endsWith("[]");
        if (array) base = base.substring(0, base.length() - 2).trim();
        DataType dt = fromPgName(base);
        String canon = dt != null ? dt.toRegtypeDisplay() : base;
        return array ? canon + "[]" : canon;
    }
}
