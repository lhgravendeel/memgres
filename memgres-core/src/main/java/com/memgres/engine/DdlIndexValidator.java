package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Definition-time checking of CREATE INDEX: the access method, its operator classes, and what the
 * chosen method is capable of.
 *
 * <p>Ordering matters and follows PostgreSQL: the access method is resolved before anything is
 * asked of it, its capabilities are checked before the columns are resolved, and the index name
 * clash is reported last. That ordering is observable — {@code CREATE INDEX existing_name ON t
 * USING nosuchmethod (x)} is an unknown-access-method error, not a duplicate-name error.
 */
final class DdlIndexValidator {

    private DdlIndexValidator() {
    }

    /** What each built-in access method can do, in the order PostgreSQL checks the capabilities. */
    private static final class AccessMethod {
        final boolean canUnique;
        final boolean canInclude;
        final boolean canMulticol;
        final boolean canOrder;

        AccessMethod(boolean canUnique, boolean canInclude, boolean canMulticol, boolean canOrder) {
            this.canUnique = canUnique;
            this.canInclude = canInclude;
            this.canMulticol = canMulticol;
            this.canOrder = canOrder;
        }
    }

    private static final Map<String, AccessMethod> ACCESS_METHODS = new HashMap<>();

    static {
        ACCESS_METHODS.put("btree", new AccessMethod(true, true, true, true));
        ACCESS_METHODS.put("hash", new AccessMethod(false, false, false, false));
        ACCESS_METHODS.put("gist", new AccessMethod(false, true, true, false));
        ACCESS_METHODS.put("gin", new AccessMethod(false, false, true, false));
        ACCESS_METHODS.put("spgist", new AccessMethod(false, true, false, false));
        ACCESS_METHODS.put("brin", new AccessMethod(false, false, true, false));
    }

    /**
     * Operator classes per access method, mapped to the type they accept. A value of {@code null}
     * marks a polymorphic input type (anyarray, anyenum, record, ...) whose acceptance depends on
     * more than the column's own type, so only the class's existence is checked for those.
     */
    private static final Map<String, Map<String, String>> OPCLASSES = new HashMap<>();

    private static void opclass(String method, String name, String forType) {
        Map<String, String> byName = OPCLASSES.get(method);
        if (byName == null) {
            byName = new HashMap<>();
            OPCLASSES.put(method, byName);
        }
        byName.put(name, forType);
    }

    static {
        opclass("btree", "array_ops", null);
        opclass("btree", "bit_ops", "bit");
        opclass("btree", "bool_ops", "boolean");
        opclass("btree", "bpchar_ops", "text");
        opclass("btree", "bpchar_pattern_ops", "text");
        opclass("btree", "bytea_ops", "bytea");
        opclass("btree", "char_ops", null);
        opclass("btree", "cidr_ops", "inet");
        opclass("btree", "date_ops", "date");
        opclass("btree", "enum_ops", null);
        opclass("btree", "float4_ops", "real");
        opclass("btree", "float8_ops", "double precision");
        opclass("btree", "inet_ops", "inet");
        opclass("btree", "int2_ops", "smallint");
        opclass("btree", "int4_ops", "integer");
        opclass("btree", "int8_ops", "bigint");
        opclass("btree", "interval_ops", "interval");
        opclass("btree", "jsonb_ops", "jsonb");
        opclass("btree", "macaddr8_ops", "macaddr8");
        opclass("btree", "macaddr_ops", "macaddr");
        opclass("btree", "money_ops", "money");
        opclass("btree", "multirange_ops", null);
        opclass("btree", "name_ops", "name");
        opclass("btree", "numeric_ops", "numeric");
        opclass("btree", "oid_ops", "oid");
        opclass("btree", "oidvector_ops", null);
        opclass("btree", "pg_lsn_ops", null);
        opclass("btree", "range_ops", null);
        opclass("btree", "record_image_ops", null);
        opclass("btree", "record_ops", null);
        opclass("btree", "text_ops", "text");
        opclass("btree", "text_pattern_ops", "text");
        opclass("btree", "tid_ops", null);
        opclass("btree", "time_ops", "time without time zone");
        opclass("btree", "timestamp_ops", "timestamp without time zone");
        opclass("btree", "timestamptz_ops", "timestamp with time zone");
        opclass("btree", "timetz_ops", "time with time zone");
        opclass("btree", "tsquery_ops", "tsquery");
        opclass("btree", "tsvector_ops", "tsvector");
        opclass("btree", "uuid_ops", "uuid");
        opclass("btree", "varbit_ops", "bit varying");
        opclass("btree", "varchar_ops", "text");
        opclass("btree", "varchar_pattern_ops", "text");
        opclass("btree", "xid8_ops", null);

        opclass("hash", "aclitem_ops", null);
        opclass("hash", "array_ops", null);
        opclass("hash", "bool_ops", "boolean");
        opclass("hash", "bpchar_ops", "text");
        opclass("hash", "bpchar_pattern_ops", "text");
        opclass("hash", "bytea_ops", "bytea");
        opclass("hash", "char_ops", null);
        opclass("hash", "cid_ops", null);
        opclass("hash", "cidr_ops", "inet");
        opclass("hash", "date_ops", "date");
        opclass("hash", "enum_ops", null);
        opclass("hash", "float4_ops", "real");
        opclass("hash", "float8_ops", "double precision");
        opclass("hash", "inet_ops", "inet");
        opclass("hash", "int2_ops", "smallint");
        opclass("hash", "int4_ops", "integer");
        opclass("hash", "int8_ops", "bigint");
        opclass("hash", "interval_ops", "interval");
        opclass("hash", "jsonb_ops", "jsonb");
        opclass("hash", "macaddr8_ops", "macaddr8");
        opclass("hash", "macaddr_ops", "macaddr");
        opclass("hash", "multirange_ops", null);
        opclass("hash", "name_ops", "name");
        opclass("hash", "numeric_ops", "numeric");
        opclass("hash", "oid_ops", "oid");
        opclass("hash", "oidvector_ops", null);
        opclass("hash", "pg_lsn_ops", null);
        opclass("hash", "range_ops", null);
        opclass("hash", "record_ops", null);
        opclass("hash", "text_ops", "text");
        opclass("hash", "text_pattern_ops", "text");
        opclass("hash", "tid_ops", null);
        opclass("hash", "time_ops", "time without time zone");
        opclass("hash", "timestamp_ops", "timestamp without time zone");
        opclass("hash", "timestamptz_ops", "timestamp with time zone");
        opclass("hash", "timetz_ops", "time with time zone");
        opclass("hash", "uuid_ops", "uuid");
        opclass("hash", "varchar_ops", "text");
        opclass("hash", "varchar_pattern_ops", "text");
        opclass("hash", "xid8_ops", null);
        opclass("hash", "xid_ops", null);

        opclass("gin", "array_ops", null);
        opclass("gin", "bit_ops", "bit");
        opclass("gin", "bool_ops", "boolean");
        opclass("gin", "bpchar_ops", "text");
        opclass("gin", "bytea_ops", "bytea");
        opclass("gin", "char_ops", null);
        opclass("gin", "cidr_ops", "cidr");
        opclass("gin", "date_ops", "date");
        opclass("gin", "enum_ops", null);
        opclass("gin", "float4_ops", "real");
        opclass("gin", "float8_ops", "double precision");
        opclass("gin", "inet_ops", "inet");
        opclass("gin", "int2_ops", "smallint");
        opclass("gin", "int4_ops", "integer");
        opclass("gin", "int8_ops", "bigint");
        opclass("gin", "interval_ops", "interval");
        opclass("gin", "jsonb_ops", "jsonb");
        opclass("gin", "jsonb_path_ops", "jsonb");
        opclass("gin", "macaddr8_ops", "macaddr8");
        opclass("gin", "macaddr_ops", "macaddr");
        opclass("gin", "money_ops", "money");
        opclass("gin", "name_ops", "name");
        opclass("gin", "numeric_ops", "numeric");
        opclass("gin", "oid_ops", "oid");
        opclass("gin", "text_ops", "text");
        opclass("gin", "time_ops", "time without time zone");
        opclass("gin", "timestamp_ops", "timestamp without time zone");
        opclass("gin", "timestamptz_ops", "timestamp with time zone");
        opclass("gin", "timetz_ops", "time with time zone");
        opclass("gin", "tsvector_ops", "tsvector");
        opclass("gin", "uuid_ops", "uuid");
        opclass("gin", "varbit_ops", "bit varying");
        opclass("gin", "varchar_ops", "character varying");

        opclass("gist", "box_ops", "box");
        opclass("gist", "circle_ops", "circle");
        opclass("gist", "gist_bit_ops", "bit");
        opclass("gist", "gist_bool_ops", "boolean");
        opclass("gist", "gist_bpchar_ops", "text");
        opclass("gist", "gist_bytea_ops", "bytea");
        opclass("gist", "gist_cash_ops", "money");
        opclass("gist", "gist_cidr_ops", "cidr");
        opclass("gist", "gist_date_ops", "date");
        opclass("gist", "gist_enum_ops", null);
        opclass("gist", "gist_float4_ops", "real");
        opclass("gist", "gist_float8_ops", "double precision");
        opclass("gist", "gist_inet_ops", "inet");
        opclass("gist", "gist_int2_ops", "smallint");
        opclass("gist", "gist_int4_ops", "integer");
        opclass("gist", "gist_int8_ops", "bigint");
        opclass("gist", "gist_interval_ops", "interval");
        opclass("gist", "gist_macaddr8_ops", "macaddr8");
        opclass("gist", "gist_macaddr_ops", "macaddr");
        opclass("gist", "gist_numeric_ops", "numeric");
        opclass("gist", "gist_oid_ops", "oid");
        opclass("gist", "gist_text_ops", "text");
        opclass("gist", "gist_time_ops", "time without time zone");
        opclass("gist", "gist_timestamp_ops", "timestamp without time zone");
        opclass("gist", "gist_timestamptz_ops", "timestamp with time zone");
        opclass("gist", "gist_timetz_ops", "time with time zone");
        opclass("gist", "gist_uuid_ops", "uuid");
        opclass("gist", "gist_vbit_ops", "bit varying");
        opclass("gist", "inet_ops", "inet");
        opclass("gist", "multirange_ops", null);
        opclass("gist", "point_ops", "point");
        opclass("gist", "poly_ops", "polygon");
        opclass("gist", "range_ops", null);
        opclass("gist", "tsquery_ops", "tsquery");
        opclass("gist", "tsvector_ops", "tsvector");

        opclass("spgist", "box_ops", "box");
        opclass("spgist", "inet_ops", "inet");
        opclass("spgist", "kd_point_ops", "point");
        opclass("spgist", "poly_ops", "polygon");
        opclass("spgist", "quad_point_ops", "point");
        opclass("spgist", "range_ops", null);
        opclass("spgist", "text_ops", "text");

        // BRIN names its classes <base>_<strategy>_ops; the strategies a base supports vary, so
        // only the name is recognised here and the type is left to the access method.
        String[] brinBase = {"bit", "box", "bpchar", "bytea", "char", "date", "float4", "float8",
                "inet", "int2", "int4", "int8", "interval", "macaddr", "macaddr8", "name",
                "numeric", "oid", "pg_lsn", "range", "text", "tid", "time", "timestamp",
                "timestamptz", "timetz", "uuid", "varbit"};
        for (String base : brinBase) {
            opclass("brin", base + "_minmax_ops", null);
            opclass("brin", base + "_minmax_multi_ops", null);
            opclass("brin", base + "_bloom_ops", null);
            opclass("brin", base + "_inclusion_ops", null);
        }
    }

    /** Types that are binary-coercible to one another, so an opclass for one accepts the others. */
    private static final Set<String> STRING_FAMILY = Cols.setOf("text", "character varying", "character");

    /**
     * Check everything about a CREATE INDEX that PostgreSQL settles before touching the heap.
     *
     * @param table  the relation being indexed, already resolved
     * @param method the USING clause, or null for the default btree
     */
    static void validate(Table table, String method, boolean unique, List<String> columns,
                         List<String> columnOptions, List<String> includeColumns) {
        String am = method != null ? method.toLowerCase() : "btree";
        AccessMethod amInfo = ACCESS_METHODS.get(am);
        if (amInfo == null) {
            throw PgErrors.undefinedObject("access method", method);
        }
        if (unique && !amInfo.canUnique) {
            throw PgErrors.notImplemented("access method \"" + am + "\" does not support unique indexes");
        }
        if (includeColumns != null && !includeColumns.isEmpty() && !amInfo.canInclude) {
            throw PgErrors.notImplemented("access method \"" + am + "\" does not support included columns");
        }
        if (columns != null && columns.size() > 1 && !amInfo.canMulticol) {
            throw PgErrors.notImplemented("access method \"" + am + "\" does not support multicolumn indexes");
        }
        if (columns == null) return;
        for (int i = 0; i < columns.size(); i++) {
            String opts = columnOptions != null && i < columnOptions.size() ? columnOptions.get(i) : "";
            if (opts == null) opts = "";
            checkCollatable(table, columns.get(i), opts);
            checkOpclass(table, am, columns.get(i), opts);
            if (!amInfo.canOrder) {
                if (opts.contains("DESC")) {
                    throw PgErrors.notImplemented(
                            "access method \"" + am + "\" does not support ASC/DESC options");
                }
                if (opts.contains("NULLS FIRST") || opts.contains("NULLS LAST")) {
                    throw PgErrors.notImplemented(
                            "access method \"" + am + "\" does not support NULLS FIRST/LAST options");
                }
            }
        }
    }

    /** Only the string types carry a collation, so COLLATE on anything else is meaningless. */
    private static void checkCollatable(Table table, String column, String opts) {
        if (!opts.contains("collate:")) return;
        int colIdx = table.getColumnIndex(column);
        if (colIdx < 0) return;
        Column col = table.getColumns().get(colIdx);
        if (col.getEnumTypeName() != null || col.getCompositeTypeName() != null
                || col.getDomainTypeName() != null || col.getArrayElementType() != null) {
            return;
        }
        String colType = CatalogHelper.pgTypeName(col.getType());
        if (colType == null || STRING_FAMILY.contains(colType) || "name".equals(colType)) return;
        throw PgErrors.datatypeMismatch("collations are not supported by type " + colType);
    }

    /** The opclass named on a column must exist for the access method and accept the column's type. */
    private static void checkOpclass(Table table, String am, String column, String opts) {
        String opclassName = null;
        for (String part : opts.split(" ")) {
            if (part.startsWith("opclass:")) opclassName = part.substring("opclass:".length());
        }
        if (opclassName == null) return;
        Map<String, String> byName = OPCLASSES.get(am);
        if (byName == null || !byName.containsKey(opclassName.toLowerCase())) {
            throw new MemgresException("operator class \"" + opclassName
                    + "\" does not exist for access method \"" + am + "\"", "42704");
        }
        String accepts = byName.get(opclassName.toLowerCase());
        if (accepts == null) return;
        int colIdx = table.getColumnIndex(column);
        if (colIdx < 0) return;   // an unknown column is reported by the caller's own check
        Column col = table.getColumns().get(colIdx);
        if (col.getEnumTypeName() != null || col.getCompositeTypeName() != null) return;
        String colType = CatalogHelper.pgTypeName(col.getType());
        if (colType == null || colType.equals(accepts)) return;
        if (STRING_FAMILY.contains(colType) && STRING_FAMILY.contains(accepts)) return;
        throw PgErrors.datatypeMismatch("operator class \"" + opclassName
                + "\" does not accept data type " + colType);
    }

    /** Included columns are stored verbatim: they cannot be expressions, and must exist. */
    static void validateIncludeColumns(Table table, List<String> includeColumns) {
        if (includeColumns == null) return;
        for (String col : includeColumns) {
            if (col.contains("(") || col.contains(")") || col.contains(" ")) {
                throw PgErrors.notImplemented("expressions are not supported in included columns");
            }
            if (table.getColumnIndex(col) < 0) {
                throw new MemgresException("column \"" + col + "\" does not exist", "42703");
            }
        }
    }
}
