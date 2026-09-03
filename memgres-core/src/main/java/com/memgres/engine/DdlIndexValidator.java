package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.HashMap;
import java.util.LinkedHashMap;
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

    /**
     * PostgreSQL's {@code INDEX_MAX_KEYS}: an index tuple has room for exactly this many
     * attributes, so a wider index could not be built whatever the access method.
     */
    static final int MAX_INDEX_KEYS = 32;

    /**
     * Refuse an index — declared as one, or built to back a PRIMARY KEY or UNIQUE constraint —
     * that names more attributes than an index tuple holds. PostgreSQL counts the key columns and
     * the INCLUDE columns together, so 30 keys plus 3 included is over the limit as surely as 33
     * keys are.
     */
    static void checkIndexColumnCount(List<String> keyColumns, List<String> includeColumns) {
        int keys = keyColumns == null ? 0 : keyColumns.size();
        int included = includeColumns == null ? 0 : includeColumns.size();
        if (keys + included > MAX_INDEX_KEYS) {
            throw new MemgresException("cannot use more than " + MAX_INDEX_KEYS
                    + " columns in an index", "54011");
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
        opclass("btree", "bpchar_ops", "character");
        opclass("btree", "bpchar_pattern_ops", "character");
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
        opclass("hash", "bpchar_ops", "character");
        opclass("hash", "bpchar_pattern_ops", "character");
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
        opclass("gin", "bpchar_ops", "character");
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
        opclass("gist", "gist_bpchar_ops", "character");
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
     * The value functions SQL spells without an argument list. PostgreSQL's grammar admits them
     * wherever an index key admits a call, so one written as a key is an expression rather than
     * the name of a column — and, being no more than stable, it is refused as one.
     */
    private static final Set<String> SQL_VALUE_FUNCTIONS = Cols.setOf(
            "current_date", "current_time", "current_timestamp", "localtime", "localtimestamp",
            "current_catalog", "current_role", "current_schema", "current_user", "session_user",
            "user");

    /** True when an index key is one of the value functions written without an argument list. */
    static boolean isSqlValueFunction(String key) {
        return key != null && SQL_VALUE_FUNCTIONS.contains(key.toLowerCase(java.util.Locale.ROOT));
    }

    /**
     * Check everything about a CREATE INDEX that PostgreSQL settles before touching the heap.
     *
     * @param table  the relation being indexed, already resolved
     * @param method the USING clause, or null for the default btree
     */
    static void validate(Table table, String method, boolean unique, List<String> columns,
                         List<String> columnOptions, List<String> includeColumns) {
        validate(null, table, method, unique, columns, columnOptions, includeColumns, null);
    }

    /**
     * @param database  the catalog a named collation is looked up in, or null to skip that check
     * @param withOptions the WITH clause's storage parameters, or null when there is none
     */
    static void validate(Database database, Table table, String method, boolean unique,
                         List<String> columns, List<String> columnOptions,
                         List<String> includeColumns, Map<String, String> withOptions) {
        // The width of the index tuple is settled from the statement alone, so PostgreSQL checks
        // it before it looks the access method up.
        checkIndexColumnCount(columns, includeColumns);
        String am = method != null ? method.toLowerCase(java.util.Locale.ROOT) : "btree";
        AccessMethod amInfo = ACCESS_METHODS.get(am);
        if (amInfo == null) {
            throw PgErrors.undefinedObject("access method", method);
        }
        checkRelOptions(am, withOptions);
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
            // PostgreSQL resolves a key before it asks anything about how that key is to be
            // indexed, so a column the relation does not have is reported as a missing column and
            // not as a fault in the collation, the operator class or the ordering written after
            // it. The missing column itself is reported by the caller, which knows the relation.
            if (table != null && !DdlDefinitionChecks.isExpressionKeyElement(columns.get(i))
                    && table.getColumnIndex(columns.get(i)) < 0) {
                continue;
            }
            String opts = columnOptions != null && i < columnOptions.size() ? columnOptions.get(i) : "";
            if (opts == null) opts = "";
            checkCollationExists(database, opts);
            checkCollatable(database, table, columns.get(i), opts);
            checkOpclass(database, table, am, columns.get(i), opts);
            checkDefaultOpclass(table, am, columns.get(i), opts);
            if (!amInfo.canOrder) {
                // An ASC written out is refused as surely as a DESC. It asks for the ordering the
                // method would have used had it any ordering at all, and PostgreSQL judges the
                // clause by what it says rather than by what it would have come to.
                if (hasOption(opts, "ASC") || hasOption(opts, "DESC")) {
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

    /**
     * True when the stored options hold this word on its own. A collation or an operator class is
     * written into the same string, so a substring test would find ASC inside a name that merely
     * contains those letters.
     */
    private static boolean hasOption(String opts, String word) {
        for (String part : opts.split(" ")) {
            if (part.equals(word)) return true;
        }
        return false;
    }

    /** The collations every PostgreSQL carries, whatever the machine's locales happen to be. */
    private static final Set<String> BUILTIN_COLLATIONS = Cols.setOf(
            "c", "posix", "default", "ucs_basic", "unicode", "icu_root", "pg_c_utf8",
            "c.utf-8", "c.utf8");

    /**
     * A collation named on an index key has to exist, the same as one named in an expression.
     * The check was there for {@code x COLLATE "nosuch"} in a select list but not on the index
     * key, so the index was built over a collation nothing could resolve.
     */
    private static void checkCollationExists(Database database, String opts) {
        for (String part : opts.split(" ")) {
            if (!part.startsWith("collate:")) continue;
            String written = part.substring("collate:".length());
            String lower = written.toLowerCase(java.util.Locale.ROOT).replace("\"", "");
            if (BUILTIN_COLLATIONS.contains(lower)) return;
            if (lower.startsWith("pg_catalog.")
                    && BUILTIN_COLLATIONS.contains(lower.substring("pg_catalog.".length()))) {
                return;
            }
            if (database != null && database.getCollation(lower) != null) return;
            throw new MemgresException("collation \"" + written
                    + "\" for encoding \"UTF8\" does not exist", "42704");
        }
    }

    /**
     * Only the string types carry a collation, so COLLATE on anything else is meaningless. An
     * array carries the collation of what it holds, and a domain the collation of what it is
     * over, so those two are asked about the type underneath; an enum and a composite carry none
     * at all. Passed over rather than asked, an index was built over a collation nothing uses,
     * and PostgreSQL names the type it was asked for by the name the reader wrote it under.
     */
    /**
     * The same two checks a partition key needs, over a key element written as one string.
     *
     * <p>A partition key element is spelled the way an index column is — the column, then a
     * collation, then an operator class — and PostgreSQL holds it to the same two rules: the
     * type has to have a collation for one to be named on it, and the class has to be one the
     * access method has.
     */
    static void checkKeyElement(Database database, Table table, String am, String column,
                                String collation, String opclass) {
        StringBuilder opts = new StringBuilder();
        if (collation != null) opts.append("collate:").append(collation);
        if (opclass != null) {
            if (opts.length() > 0) opts.append(' ');
            opts.append("opclass:").append(opclass);
        }
        String written = opts.toString();
        checkCollatable(database, table, column, written);
        checkOpclass(database, table, am, column, written);
    }

    private static void checkCollatable(Database database, Table table, String column, String opts) {
        if (!opts.contains("collate:")) return;
        int colIdx = table.getColumnIndex(column);
        if (colIdx < 0) return;
        Column col = table.getColumns().get(colIdx);
        if (col.getEnumTypeName() != null) {
            throw PgErrors.datatypeMismatch("collations are not supported by type "
                    + qualifiedTypeName(database, col.getEnumTypeName()));
        }
        if (col.getCompositeTypeName() != null) {
            throw PgErrors.datatypeMismatch("collations are not supported by type "
                    + qualifiedTypeName(database, col.getCompositeTypeName()));
        }
        if (col.getDomainTypeName() != null) {
            DomainType domain = database == null ? null
                    : database.getDomain(col.getDomainTypeName());
            String base = domain == null ? null
                    : CatalogHelper.pgTypeName(domain.getBaseType());
            if (base != null && (STRING_FAMILY.contains(base) || "name".equals(base))) return;
            throw PgErrors.datatypeMismatch("collations are not supported by type "
                    + qualifiedTypeName(database, col.getDomainTypeName()));
        }
        if (col.getArrayElementType() != null) {
            String element = CatalogHelper.pgTypeName(col.getArrayElementType());
            if (element == null || STRING_FAMILY.contains(element) || "name".equals(element)) return;
            throw PgErrors.datatypeMismatch(
                    "collations are not supported by type " + element + "[]");
        }
        String colType = CatalogHelper.pgTypeName(col.getType());
        if (colType == null || STRING_FAMILY.contains(colType) || "name".equals(colType)) return;
        throw PgErrors.datatypeMismatch("collations are not supported by type " + colType);
    }

    /** A user-defined type as PostgreSQL names it in a complaint: schema and then name. */
    private static String qualifiedTypeName(Database database, String written) {
        if (written.indexOf('.') >= 0) return written;
        String schema = database == null ? null : TypeNamespace.schemaOf(database, written);
        return (schema == null ? "public" : schema) + "." + written;
    }

    /** The opclass named on a column must exist for the access method and accept the column's type. */
    private static void checkOpclass(Database database, Table table, String am, String column,
                                     String opts) {
        String opclassName = null;
        boolean parameterised = false;
        for (String part : opts.split(" ")) {
            if (part.startsWith("opclass:")) opclassName = part.substring("opclass:".length());
            else if ("opclassoptions".equals(part)) parameterised = true;
        }
        if (opclassName == null) return;
        // A written schema is opened before the class inside it is looked for, so naming one that
        // is not there is reported as the missing schema rather than as a missing class.
        int dot = opclassName.indexOf('.');
        if (dot >= 0) {
            String opclassSchema = opclassName.substring(0, dot);
            if (!SchemaQualifier.exists(database, null, opclassSchema)) {
                throw SchemaQualifier.missing(opclassSchema);
            }
            opclassName = opclassName.substring(dot + 1);
        }
        Map<String, String> byName = OPCLASSES.get(am);
        if (byName == null || !byName.containsKey(opclassName.toLowerCase(java.util.Locale.ROOT))) {
            throw new MemgresException("operator class \"" + opclassName
                    + "\" does not exist for access method \"" + am + "\"", "42704");
        }
        if (parameterised) {
            // None of the classes PostgreSQL 18 ships for these access methods takes parameters,
            // and it names the class here without quoting it.
            throw new MemgresException("operator class " + opclassName + " has no options", "22023");
        }
        String accepts = byName.get(opclassName.toLowerCase(java.util.Locale.ROOT));
        if (accepts == null) return;
        int colIdx = table.getColumnIndex(column);
        if (colIdx < 0) return;   // an unknown column is reported by the caller's own check
        Column col = table.getColumns().get(colIdx);
        if (col.getEnumTypeName() != null || col.getCompositeTypeName() != null) return;
        String colType = CatalogHelper.pgTypeName(col.getType());
        if (colType == null || colType.equals(accepts)) return;
        // text and character varying hold the same values, so a class for either indexes the
        // other; a blank-padded character does not compare like either of them, and only the
        // class of its own type takes it -- while its class still takes the two of them.
        if (STRING_FAMILY.contains(colType) && STRING_FAMILY.contains(accepts)
                && !"character".equals(colType)) return;
        // A cidr is an inet that happens to name a network, stored the same way, so the classes
        // that index an address index it too.
        if ("cidr".equals(colType) && "inet".equals(accepts)) return;
        throw PgErrors.datatypeMismatch("operator class \"" + opclassName
                + "\" does not accept data type " + colType);
    }

    /**
     * The default operator class each access method has for each type, in a PostgreSQL 18 with no
     * contrib extensions installed. Without an opclass the method has no operators to compare
     * values with, so PostgreSQL refuses the index rather than build one it could never search;
     * and where a definition names the class the type would have taken anyway, PostgreSQL leaves
     * it out of the definition it prints back.
     *
     * <p>Measured from {@code pg_opclass} rather than written from memory, and deliberately
     * excluding classes owned by an extension: {@code btree_gin} and {@code btree_gist} add the
     * scalar types to gin and gist, and a server with them installed accepts indexes this engine
     * does not implement.
     *
     * <p>A type with no class of its own indexes through the class of a type it is binary-coercible
     * to, which is why character varying answers text_ops and cidr answers inet_ops.
     */
    private static final Map<String, Map<String, String>> DEFAULT_OPCLASSES = new HashMap<>();

    private static void defaults(String method, String... typeThenClass) {
        DEFAULT_OPCLASSES.put(method, classesOf(typeThenClass));
    }

    private static Map<String, String> classesOf(String... typeThenClass) {
        Map<String, String> byType = new LinkedHashMap<>();
        for (int i = 0; i + 1 < typeThenClass.length; i += 2) {
            byType.put(typeThenClass[i], typeThenClass[i + 1]);
        }
        return byType;
    }

    static {
        // The scalar types, shared by the three methods that index values whole
        String[] scalar = {
            "\"char\"", "char_ops", "bigint", "int8_ops", "bit", "bit_ops",
            "bit varying", "varbit_ops", "boolean", "bool_ops", "bytea", "bytea_ops",
            "character", "bpchar_ops", "character varying", "text_ops", "date", "date_ops",
            "double precision", "float8_ops", "inet", "inet_ops", "integer", "int4_ops",
            "interval", "interval_ops", "macaddr", "macaddr_ops", "macaddr8", "macaddr8_ops",
            "money", "money_ops", "name", "name_ops", "numeric", "numeric_ops", "oid", "oid_ops",
            "oidvector", "oidvector_ops", "pg_lsn", "pg_lsn_ops", "real", "float4_ops",
            "smallint", "int2_ops", "text", "text_ops", "tid", "tid_ops",
            "time with time zone", "timetz_ops", "time without time zone", "time_ops",
            "timestamp with time zone", "timestamptz_ops",
            "timestamp without time zone", "timestamp_ops", "uuid", "uuid_ops",
            "xid", "xid_ops", "xid8", "xid8_ops", "jsonb", "jsonb_ops",
            "tsquery", "tsquery_ops", "tsvector", "tsvector_ops", "record", "record_ops",
            "cid", "cid_ops", "aclitem", "aclitem_ops", "cidr", "inet_ops",
        };
        // The polymorphic classes cover a whole family at once: an array, an enum or a range
        // indexes through one of these rather than through a class of its own name.
        Map<String, String> btree = classesOf(scalar);
        btree.remove("cid");
        btree.remove("aclitem");
        btree.putAll(classesOf("anyarray", "array_ops", "anyenum", "enum_ops",
                "anyrange", "range_ops", "anymultirange", "multirange_ops"));
        DEFAULT_OPCLASSES.put("btree", btree);
        Map<String, String> hash = classesOf(scalar);
        hash.remove("bit");
        hash.remove("bit varying");
        hash.remove("tsquery");
        hash.remove("tsvector");
        hash.remove("oidvector");
        hash.remove("money");
        hash.putAll(classesOf("anyarray", "array_ops", "anyenum", "enum_ops",
                "anyrange", "range_ops", "anymultirange", "multirange_ops"));
        DEFAULT_OPCLASSES.put("hash", hash);
        // BRIN summarises a range of values rather than storing them, so its class for a type is
        // named after the summary it keeps: the minimum and maximum for a scalar, and for a value
        // that spans an interval of its own, the box it fits inside.
        Map<String, String> brin = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : classesOf(scalar).entrySet()) {
            brin.put(e.getKey(), e.getValue().replace("_ops", "_minmax_ops"));
        }
        brin.remove("boolean");
        brin.remove("jsonb");
        brin.remove("money");
        brin.remove("tsquery");
        brin.remove("tsvector");
        brin.remove("record");
        brin.remove("cid");
        brin.remove("aclitem");
        brin.remove("xid");
        brin.put("inet", "inet_inclusion_ops");
        brin.put("cidr", "inet_inclusion_ops");
        brin.put("box", "box_inclusion_ops");
        brin.put("anyrange", "range_inclusion_ops");
        DEFAULT_OPCLASSES.put("brin", brin);
        // The three methods built for a particular shape of value index far fewer types
        defaults("gin", "jsonb", "jsonb_ops", "tsvector", "tsvector_ops", "anyarray", "array_ops");
        defaults("gist", "box", "box_ops", "circle", "circle_ops", "point", "point_ops",
                "polygon", "poly_ops", "tsquery", "tsquery_ops", "tsvector", "tsvector_ops",
                "anyrange", "range_ops", "anymultirange", "multirange_ops");
        defaults("spgist", "box", "box_ops", "inet", "inet_ops", "point", "quad_point_ops",
                "polygon", "poly_ops", "text", "text_ops", "anyrange", "range_ops");
    }

    /**
     * The operator class an index key of this type takes when the definition names none, or null
     * when the method has no default for it. The type name is the one {@link CatalogHelper}
     * writes, so it is the same name the column's own definition would print.
     */
    static String defaultOpclass(String method, String typeName) {
        Map<String, String> byType = DEFAULT_OPCLASSES.get(method == null ? "btree" : method);
        return byType == null || typeName == null ? null : byType.get(typeName);
    }

    /**
     * An indexed column needs an operator class the access method can use. Where none is named,
     * the type must have a default one — {@code 42704} otherwise, naming the type and the method.
     */
    private static void checkDefaultOpclass(Table table, String am, String column, String opts) {
        if (opts.contains("opclass:")) return;   // an explicit class is checked by checkOpclass
        if (!DEFAULT_OPCLASSES.containsKey(am)) return;
        int colIdx = table.getColumnIndex(column);
        if (colIdx < 0) return;   // an unknown column is reported by the caller's own check
        Column col = table.getColumns().get(colIdx);
        String colType = indexedTypeName(col);
        // A type this engine cannot name is left alone: refusing a valid index is the worse
        // failure of the two.
        if (colType == null || defaultOpclass(am, colType) != null) return;
        MemgresException ex = new MemgresException("data type " + colType
                + " has no default operator class for access method \"" + am + "\"", "42704");
        ex.setHint("You must specify an operator class for the index"
                + " or define a default operator class for the data type.");
        throw ex;
    }

    /**
     * The type name to look an operator class up by. Arrays, enums and ranges all index through a
     * polymorphic class, so they are answered by the family they belong to rather than by name.
     */
    static String indexedTypeName(Column col) {
        if (col.getArrayElementType() != null) return "anyarray";
        if (col.getEnumTypeName() != null) return "anyenum";
        if (col.getCompositeTypeName() != null || col.getDomainTypeName() != null) return null;
        return polymorphicName(CatalogHelper.pgTypeName(col.getType()));
    }

    /**
     * The same name for the type an index expression comes out as. A type this engine cannot
     * resolve is answered null, which leaves the key's operator class written down as it stands.
     */
    static String indexedTypeName(RuleDeparser.PgType type) {
        if (type == null) return null;
        if (type.elem != null) return "anyarray";
        if (type.custom != null || type.dt == null) return null;
        return polymorphicName(CatalogHelper.pgTypeName(type.dt));
    }

    private static String polymorphicName(String name) {
        if (name != null && name.endsWith("[]")) return "anyarray";
        if (name != null && (name.endsWith("range") || name.endsWith("multirange"))) {
            return name.endsWith("multirange") ? "anymultirange" : "anyrange";
        }
        return name;
    }

    // ---- Storage parameters (reloptions) ----

    /**
     * One storage parameter: the kind of value it takes and, for a number, the range PostgreSQL
     * will accept. {@code allowed} lists the words an enumerated option answers to.
     */
    private static final class RelOption {
        final char kind;          // 'i' integer, 'r' real, 'b' boolean, 'e' enumerated
        final double min;
        final double max;
        final List<String> allowed;

        RelOption(char kind, double min, double max, List<String> allowed) {
            this.kind = kind;
            this.min = min;
            this.max = max;
            this.allowed = allowed;
        }

        static RelOption integer(double min, double max) { return new RelOption('i', min, max, null); }
        static RelOption real(double min, double max) { return new RelOption('r', min, max, null); }
        static RelOption bool() { return new RelOption('b', 0, 0, null); }
        static RelOption enumerated(String... values) {
            return new RelOption('e', 0, 0, Cols.listOf(values));
        }
    }

    /**
     * The storage parameters each relation kind accepts, keyed by access method — {@code "heap"}
     * standing for a table's own WITH clause. Written from PostgreSQL 18's {@code reloptions.c}
     * and measured against a live server: a parameter this engine does not know about is refused
     * exactly as one PostgreSQL does not know about, because both are typing mistakes.
     */
    private static final Map<String, Map<String, RelOption>> REL_OPTIONS = new HashMap<>();

    private static void relopt(String kind, String name, RelOption option) {
        Map<String, RelOption> byName = REL_OPTIONS.get(kind);
        if (byName == null) {
            byName = new HashMap<>();
            REL_OPTIONS.put(kind, byName);
        }
        byName.put(name, option);
    }

    static {
        relopt("heap", "fillfactor", RelOption.integer(10, 100));
        relopt("heap", "toast_tuple_target", RelOption.integer(128, 8160));
        relopt("heap", "parallel_workers", RelOption.integer(0, 1024));
        relopt("heap", "autovacuum_enabled", RelOption.bool());
        relopt("heap", "user_catalog_table", RelOption.bool());
        relopt("heap", "vacuum_truncate", RelOption.bool());
        relopt("heap", "autovacuum_vacuum_threshold", RelOption.integer(0, 2147483647.0));
        // PostgreSQL 18 added this one; leaving it out refused a CREATE TABLE that PG runs.
        relopt("heap", "autovacuum_vacuum_max_threshold", RelOption.integer(-1, 2147483647.0));
        // The legacy spelling is still accepted on CREATE TABLE, and pg_dump still writes it.
        relopt("heap", "oids", RelOption.bool());
        relopt("heap", "autovacuum_vacuum_insert_threshold", RelOption.integer(-1, 2147483647.0));
        relopt("heap", "autovacuum_analyze_threshold", RelOption.integer(0, 2147483647.0));
        relopt("heap", "autovacuum_vacuum_cost_limit", RelOption.integer(1, 10000));
        relopt("heap", "autovacuum_freeze_min_age", RelOption.integer(0, 1000000000.0));
        relopt("heap", "autovacuum_freeze_max_age", RelOption.integer(100000, 2000000000.0));
        relopt("heap", "autovacuum_freeze_table_age", RelOption.integer(0, 2000000000.0));
        relopt("heap", "autovacuum_multixact_freeze_min_age", RelOption.integer(0, 1000000000.0));
        relopt("heap", "autovacuum_multixact_freeze_max_age", RelOption.integer(10000, 2000000000.0));
        relopt("heap", "autovacuum_multixact_freeze_table_age", RelOption.integer(0, 2000000000.0));
        relopt("heap", "log_autovacuum_min_duration", RelOption.integer(-1, 2147483647.0));
        relopt("heap", "autovacuum_vacuum_cost_delay", RelOption.real(-1, 100));
        relopt("heap", "autovacuum_vacuum_scale_factor", RelOption.real(0, 100));
        relopt("heap", "autovacuum_vacuum_insert_scale_factor", RelOption.real(0, 100));
        relopt("heap", "autovacuum_analyze_scale_factor", RelOption.real(0, 100));
        relopt("heap", "vacuum_index_cleanup", RelOption.enumerated("auto", "on", "off", "true", "false"));
        // vacuum_max_eager_scan_fraction is a GUC, not a storage parameter: PostgreSQL 18
        // refuses it in WITH (...), so listing it here made memgres accept what PG does not.

        // A TOAST table takes only the parameters that govern when it is vacuumed and analysed;
        // fillfactor and the rest belong to the relation itself and are refused here.
        relopt("toast", "autovacuum_enabled", RelOption.bool());
        relopt("toast", "vacuum_truncate", RelOption.bool());
        relopt("toast", "vacuum_index_cleanup", RelOption.enumerated("auto", "on", "off", "true", "false"));
        relopt("toast", "autovacuum_vacuum_threshold", RelOption.integer(0, 2147483647.0));
        relopt("toast", "autovacuum_vacuum_max_threshold", RelOption.integer(-1, 2147483647.0));
        relopt("toast", "autovacuum_vacuum_insert_threshold", RelOption.integer(-1, 2147483647.0));
        relopt("toast", "autovacuum_analyze_threshold", RelOption.integer(0, 2147483647.0));
        relopt("toast", "autovacuum_vacuum_cost_limit", RelOption.integer(1, 10000));
        relopt("toast", "autovacuum_freeze_min_age", RelOption.integer(0, 1000000000.0));
        relopt("toast", "autovacuum_freeze_max_age", RelOption.integer(100000, 2000000000.0));
        relopt("toast", "autovacuum_freeze_table_age", RelOption.integer(0, 2000000000.0));
        relopt("toast", "autovacuum_multixact_freeze_min_age", RelOption.integer(0, 1000000000.0));
        relopt("toast", "autovacuum_multixact_freeze_max_age", RelOption.integer(10000, 2000000000.0));
        relopt("toast", "autovacuum_multixact_freeze_table_age", RelOption.integer(0, 2000000000.0));
        relopt("toast", "log_autovacuum_min_duration", RelOption.integer(-1, 2147483647.0));
        relopt("toast", "autovacuum_vacuum_cost_delay", RelOption.real(-1, 100));
        relopt("toast", "autovacuum_vacuum_scale_factor", RelOption.real(0, 100));
        relopt("toast", "autovacuum_vacuum_insert_scale_factor", RelOption.real(0, 100));
        relopt("toast", "autovacuum_analyze_scale_factor", RelOption.real(0, 100));

        relopt("btree", "fillfactor", RelOption.integer(10, 100));
        relopt("btree", "deduplicate_items", RelOption.bool());
        relopt("btree", "vacuum_cleanup_index_scale_factor", RelOption.real(0, 1e10));
        relopt("hash", "fillfactor", RelOption.integer(10, 100));
        relopt("gist", "fillfactor", RelOption.integer(10, 100));
        relopt("gist", "buffering", RelOption.enumerated("auto", "on", "off"));
        relopt("spgist", "fillfactor", RelOption.integer(10, 100));
        relopt("gin", "fastupdate", RelOption.bool());
        relopt("gin", "gin_pending_list_limit", RelOption.integer(64, 2147483647.0));
        relopt("brin", "pages_per_range", RelOption.integer(1, 131072));
        relopt("brin", "autosummarize", RelOption.bool());
    }

    /** The words PostgreSQL reads as a boolean in a storage parameter. */
    private static final Set<String> TRUE_WORDS = Cols.setOf("true", "on", "yes", "1", "t", "y");
    private static final Set<String> FALSE_WORDS = Cols.setOf("false", "off", "no", "0", "f", "n");

    /**
     * Check a WITH clause of storage parameters against what the relation kind accepts.
     * PostgreSQL parses these while it is still defining the relation, so a parameter it does
     * not recognise or a value out of range stops the statement before anything is created.
     *
     * @param kind {@code "heap"} for a table, or the index access method
     */
    static void checkRelOptions(String kind, Map<String, String> options) {
        if (options == null || options.isEmpty()) return;
        Map<String, RelOption> known = REL_OPTIONS.get(kind == null ? "btree" : kind.toLowerCase(java.util.Locale.ROOT));
        if (known == null) return;   // an access method this engine does not model: leave it be
        Map<String, RelOption> toast = REL_OPTIONS.get("toast");
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String name = entry.getKey().toLowerCase(java.util.Locale.ROOT);
            // A namespaced parameter belongs to the TOAST table rather than to this relation, and
            // is judged against what a TOAST table takes. Skipped outright, toast.fillfactor --
            // which PostgreSQL does not know -- was accepted and then stored nowhere.
            Map<String, RelOption> takes = known;
            if (name.startsWith("toast.")) {
                name = name.substring(6);
                takes = toast;
            }
            RelOption option = takes.get(name);
            if (option == null) {
                throw new MemgresException("unrecognized parameter \"" + name + "\"", "22023");
            }
            String value = entry.getValue();
            // A bare parameter name means "true", which only a boolean option can mean.
            if (value == null) value = "true";
            value = value.trim();
            if (value.length() > 1 && value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
            switch (option.kind) {
                case 'b':
                    if (!TRUE_WORDS.contains(value.toLowerCase(java.util.Locale.ROOT))
                            && !FALSE_WORDS.contains(value.toLowerCase(java.util.Locale.ROOT))) {
                        throw new MemgresException("invalid value for boolean option \""
                                + name + "\": " + value, "22023");
                    }
                    break;
                case 'e':
                    if (!option.allowed.contains(value.toLowerCase(java.util.Locale.ROOT))) {
                        MemgresException ex = new MemgresException(
                                "invalid value for \"" + name + "\" option", "22023");
                        StringBuilder valid = new StringBuilder("Valid values are ");
                        for (int i = 0; i < option.allowed.size(); i++) {
                            if (i > 0) valid.append(i == option.allowed.size() - 1 ? ", and " : ", ");
                            valid.append('"').append(option.allowed.get(i)).append('"');
                        }
                        ex.setDetail(valid.append('.').toString());
                        throw ex;
                    }
                    break;
                default:
                    checkNumericOption(name, value, option);
            }
        }
    }

    /**
     * A storage parameter's value as PostgreSQL stores it. A boolean or an enumerated option keeps
     * the word that was written but in lower case -- {@code On} is stored as {@code on}, not
     * canonicalised to {@code true} -- so pg_class.reloptions reads back the way PostgreSQL's
     * does. The lexer hands keyword tokens over upper-cased, which is how {@code false} arrived as
     * {@code FALSE}. A parameter this engine does not model is left exactly as it was written.
     *
     * <p>A parameter written with no value at all is a flag being turned on: PostgreSQL stores
     * {@code autovacuum_enabled} as {@code autovacuum_enabled=true}.
     */
    static String normalizeRelOptionValue(String kind, String name, String value) {
        if (value == null) return "true";
        String trimmed = value.trim();
        Map<String, RelOption> known = REL_OPTIONS.get(kind == null ? "btree" : kind.toLowerCase(java.util.Locale.ROOT));
        RelOption option = known == null || name == null ? null : known.get(name.toLowerCase(java.util.Locale.ROOT));
        if (option == null) return trimmed;
        if (option.kind == 'b' || option.kind == 'e') {
            return trimmed.toLowerCase(java.util.Locale.ROOT);
        }
        return trimmed;
    }

    /**
     * A whole WITH clause, with every value normalised the way the catalogue reports it. A
     * namespaced parameter is set on the relation's TOAST table and not on the relation, so it
     * does not belong in the relation's own reloptions and is left out here.
     */
    static Map<String, String> normalizeRelOptions(String kind, Map<String, String> options) {
        if (options == null) return null;
        Map<String, String> out = new java.util.LinkedHashMap<String, String>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (entry.getKey().toLowerCase(java.util.Locale.ROOT).startsWith("toast.")) continue;
            out.put(entry.getKey(), normalizeRelOptionValue(kind, entry.getKey(), entry.getValue()));
        }
        return out;
    }

    /** An integer or floating-point storage parameter: it must parse, and it must be in range. */
    private static void checkNumericOption(String name, String value, RelOption option) {
        double parsed;
        try {
            parsed = option.kind == 'i' ? Long.parseLong(value) : Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid value for "
                    + (option.kind == 'i' ? "integer" : "floating point")
                    + " option \"" + name + "\": " + value, "22023");
        }
        if (parsed < option.min || parsed > option.max) {
            MemgresException ex = new MemgresException("value " + value
                    + " out of bounds for option \"" + name + "\"", "22023");
            ex.setDetail("Valid values are between \"" + number(option.min, option.kind)
                    + "\" and \"" + number(option.max, option.kind) + "\".");
            throw ex;
        }
    }

    private static String number(double value, char kind) {
        return kind == 'i' ? String.valueOf((long) value) : String.valueOf(value);
    }

    /** Included columns are stored verbatim: they cannot be expressions, and must exist. */
    static void validateIncludeColumns(Table table, List<String> includeColumns) {
        if (includeColumns == null) return;
        for (String col : includeColumns) {
            // A name written in quotes may hold a space, so a space is no sign of an expression:
            // INCLUDE ("my col") names a column and was refused as though it were a call.
            if (col.contains("(") || col.contains(")")) {
                throw PgErrors.notImplemented("expressions are not supported in included columns");
            }
            if (table.getColumnIndex(col) < 0) {
                throw new MemgresException("column \"" + col + "\" does not exist", "42703");
            }
        }
    }
}
