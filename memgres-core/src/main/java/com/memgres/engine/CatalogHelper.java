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
            case ENUM:
                return "USER-DEFINED";
            case XID:
                return "xid";
            default:
                throw new IllegalStateException("Unknown data type: " + dt);
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

    /** Format a column default for information_schema / pg_attrdef, matching PG conventions. */
    public static String formatColumnDefault(Column col) {
        String def = col.getDefaultValue();
        if (def == null) return null;
        if (def.startsWith("__identity__")) return null;
        if (def.equalsIgnoreCase("now()") || def.equalsIgnoreCase("current_timestamp()")
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

    /** Resolve the owner OID for an object key, defaulting to 10. */
    public static int resolveOwnerOid(Database database, OidSupplier oids, String objectKey) {
        String owner = database.getObjectOwner(objectKey);
        if (owner != null) {
            return oids.oid("role:" + owner);
        }
        return 10;
    }
}
