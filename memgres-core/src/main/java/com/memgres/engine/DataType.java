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
    INTERVAL(1186, "interval"),

    // Network types
    INET(869, "inet"),
    CIDR(650, "cidr"),
    MACADDR(829, "macaddr"),

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
    TEXT_ARRAY(1009, "_text"),
    INT4_ARRAY(1007, "_int4"),
    ACLITEM_ARRAY(1034, "_aclitem"),

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
            case "bool":
            case "boolean":
                return BOOLEAN;
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
            case "serial":
                return SERIAL;
            case "bigserial":
                return BIGSERIAL;
            case "smallserial":
                return SMALLSERIAL;
            case "money":
                return MONEY;
            case "timetz":
            case "time with time zone":
                return TIME;
            case "inet":
                return INET;
            case "cidr":
                return CIDR;
            case "macaddr":
                return MACADDR;
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
            case "aclitem[]":
            case "_aclitem":
                return ACLITEM_ARRAY;
            default:
                return null;
        }
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
            case INTERVAL: return "interval";
            case BYTEA: return "bytea";
            case UUID: return "uuid";
            case JSON: return "json";
            case JSONB: return "jsonb";
            case MONEY: return "money";
            case INET: return "inet";
            case CIDR: return "cidr";
            case MACADDR: return "macaddr";
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
            case ACLITEM_ARRAY: return "aclitem[]";
            default: return pgName;
        }
    }
}
