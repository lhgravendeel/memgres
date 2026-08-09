package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.*;

/**
 * Builds information_schema virtual tables from the current database metadata.
 * Extracted from SystemCatalog to keep that class focused on pg_catalog tables.
 */
public class InfoSchemaBuilder {

    /**
     * The views information_schema itself is made of. PG lists these in information_schema.tables
     * alongside the user's own relations, and a tool enumerating the schema reads that list to
     * decide which standard views it can rely on.
     */
    static final List<String> INFORMATION_SCHEMA_VIEWS = Cols.listOf(
        "administrable_role_authorizations", "applicable_roles", "attributes",
        "character_sets", "check_constraint_routine_usage", "check_constraints",
        "collation_character_set_applicability", "collations", "column_column_usage",
        "column_domain_usage", "column_options", "column_privileges", "column_udt_usage",
        "columns", "constraint_column_usage", "constraint_table_usage", "data_type_privileges",
        "domain_constraints", "domain_udt_usage", "domains", "element_types",
        "enabled_roles", "foreign_data_wrapper_options", "foreign_data_wrappers",
        "foreign_server_options", "foreign_servers", "foreign_table_options",
        "foreign_tables", "information_schema_catalog_name", "key_column_usage", "parameters",
        "referential_constraints", "role_column_grants", "role_routine_grants",
        "role_table_grants", "role_udt_grants", "role_usage_grants",
        "routine_column_usage", "routine_privileges", "routine_routine_usage",
        "routine_sequence_usage", "routine_table_usage", "routines", "schemata", "sequences",
        "sql_features", "sql_implementation_info", "sql_parts", "sql_sizing",
        "table_constraints", "table_privileges", "tables", "transforms",
        "triggered_update_columns", "triggers", "udt_privileges", "usage_privileges",
        "user_defined_types", "user_mapping_options", "user_mappings", "view_column_usage",
        "view_routine_usage", "view_table_usage", "views",
        // The underscore-prefixed relations PostgreSQL's own information_schema is built on.
        // They answer for their columns already; leaving them out of the listing made them
        // relations that exist but cannot be discovered.
        "_pg_foreign_data_wrappers", "_pg_foreign_servers", "_pg_foreign_table_columns",
        "_pg_foreign_tables", "_pg_user_mappings"
    );

    /**
     * The pg_catalog relations information_schema lists.
     *
     * <p>Every relation memgres publishes, not a chosen forty of them: PostgreSQL lists all of
     * its catalogs in information_schema.tables and all of their columns in
     * information_schema.columns, and the same list pg_class and pg_attribute are built from is
     * what keeps the three from disagreeing about which relations exist.
     */
    private static final List<String> PG_CATALOG_TABLES_FOR_IS =
            new ArrayList<>(PgCatalogRelations.ALL);

    private final Database database;
    private final OidSupplier oids;
    /** Session passed into {@link #build} – used for catalog name resolution. */
    private Session currentSession;

    public InfoSchemaBuilder(Database database, OidSupplier oids) {
        this.database = database;
        this.oids = oids;
    }

    /** Returns the catalog name derived from the session's database name, falling back to "memgres". */
    private String catalogName() {
        return currentSession != null ? currentSession.getDatabaseName() : "memgres";
    }

    /**
     * Build the information_schema table for the given table name.
     * Returns an empty table for unrecognized names.
     */
    public Table build(String tableName, Session session) {
        this.currentSession = session;
        switch (tableName) {
            case "tables":
                return buildIsTables();
            case "columns":
                return buildIsColumns();
            case "schemata":
                return buildIsSchemata();
            case "table_constraints":
                return buildIsTableConstraints();
            case "key_column_usage":
                return buildIsKeyColumnUsage();
            case "referential_constraints":
                return buildIsReferentialConstraints();
            case "routines":
                return buildIsRoutines();
            case "sequences":
                return buildIsSequences();
            case "views":
                return buildIsViews();
            case "domains":
                return buildIsDomains();
            case "check_constraints":
                return buildIsCheckConstraints();
            case "constraint_column_usage":
                return buildIsConstraintColumnUsage();
            case "constraint_table_usage":
                return buildIsConstraintTableUsage();
            case "parameters":
                return buildIsParameters();
            case "triggers":
                return buildIsTriggers();
            case "collations":
                return buildIsCollations();
            case "enabled_roles":
                return buildIsEnabledRoles();
            case "applicable_roles":
                return buildIsApplicableRoles();
            case "table_privileges":
            case "role_table_grants":
                return buildIsTablePrivileges(tableName);
            case "column_privileges":
            case "role_column_grants":
                return buildIsColumnPrivileges(tableName);
            case "routine_privileges":
            case "role_routine_grants":
                return buildIsRoutinePrivileges(tableName);
            case "udt_privileges":
            case "role_udt_grants":
                return buildIsUdtPrivileges(tableName);
            case "usage_privileges":
            case "role_usage_grants":
                return buildIsUsagePrivileges(tableName);
            case "data_type_privileges":
                return buildIsDataTypePrivileges();
            case "element_types":
                return buildIsElementTypes();
            case "attributes":
                return buildIsAttributes();
            case "user_defined_types":
                return buildIsUserDefinedTypes();
            case "column_domain_usage":
                return buildIsColumnDomainUsage();
            case "column_udt_usage":
                return buildIsColumnUdtUsage();
            case "domain_udt_usage":
                return buildIsDomainUdtUsage();
            case "view_column_usage":
                return buildIsViewColumnUsage();
            case "view_table_usage":
                return buildIsViewTableUsage();
            case "domain_constraints":
                return buildIsDomainConstraints();
            case "triggered_update_columns":
                return buildIsTriggeredUpdateColumns();
            case "sql_parts":
                return constantView("sql_parts", SQL_PARTS);
            case "sql_sizing":
                return constantView("sql_sizing", SQL_SIZING);
            case "sql_implementation_info":
                return constantView("sql_implementation_info", SQL_IMPLEMENTATION_INFO);
            case "foreign_tables": {
                // The standard's own list of foreign tables and the server behind each one.
                Table t = declaredView(tableName);
                for (Object[] entry : ForeignTables.live(database)) {
                    Database.FdwForeignTable ft = (Database.FdwForeignTable) entry[1];
                    t.insertRow(new Object[]{
                            catalogName(), (String) entry[0], ft.tableName,
                            catalogName(), ft.serverName
                    });
                }
                return t;
            }
            case "information_schema_catalog_name": {
                Table t = declaredView(tableName);
                t.insertRow(new Object[]{catalogName()});
                return t;
            }
            case "character_sets": {
                Table t = declaredView(tableName);
                // PG reports the database encoding as a single anonymous character set, and it
                // names no default collation for it: the collate catalog is the database, the
                // schema and the name are null. Naming one here made the view point at a
                // collation information_schema.collations does not list.
                t.insertRow(new Object[]{null, null, "UTF8", "UCS", "UTF8",
                        catalogName(), null, null});
                return t;
            }
            default: {
                // A view listed in information_schema.tables has to answer for the columns the
                // standard says it has: a name with nothing behind it is worse than a gap,
                // because a tool that finds a gap falls back and one that finds a false claim
                // proceeds and fails on the first column it reads.
                Table declared = declaredView(tableName);
                return declared != null ? declared : CatalogHelper.emptyTable(tableName);
            }
        }
    }

    /**
     * A view memgres lists but has no rows to put in yet, built with the columns PostgreSQL
     * declares for it so naming one resolves instead of failing with 42703.
     */
    private Table declaredView(String tableName) {
        String[] colNames = DECLARED_VIEW_COLUMNS.get(tableName);
        if (colNames == null) return null;
        List<Column> cols = new ArrayList<>();
        for (String spec : colNames) {
            // "name:type" — see DECLARED_VIEW_COLUMNS for what the suffixes stand for
            int colon = spec.indexOf(':');
            String name = colon < 0 ? spec : spec.substring(0, colon);
            cols.add(new Column(name, declaredType(colon < 0 ? "" : spec.substring(colon + 1)),
                    true, false, null));
        }
        return new Table(tableName, cols);
    }

    /** The type a {@link #DECLARED_VIEW_COLUMNS} suffix stands for. */
    private static DataType declaredType(String suffix) {
        if ("i".equals(suffix)) return DataType.INTEGER;
        if ("v".equals(suffix)) return DataType.VARCHAR;
        if ("o".equals(suffix)) return DataType.OID;
        // The helper views carry the option lists of the foreign-data catalogs, and those are
        // text arrays: information_schema.columns has to call them ARRAY of _text, not name.
        if ("a".equals(suffix)) return DataType.TEXT_ARRAY;
        return DataType.NAME;
    }

    /**
     * The columns PostgreSQL declares for the information_schema views memgres lists but does not
     * yet populate. Unsuffixed names are {@code name}, ":v" is {@code character varying}, ":i" is
     * {@code integer}, ":o" is {@code oid} and ":a" is {@code text[]} — what the
     * underscore-prefixed helper views hold their option lists in, and what
     * information_schema.columns has to call ARRAY.
     */
    private static final Map<String, String[]> DECLARED_VIEW_COLUMNS;

    static {
        Map<String, String[]> m = new LinkedHashMap<>();
        m.put("administrable_role_authorizations", new String[]{
                "grantee", "role_name", "is_grantable:v"});
        m.put("attributes", new String[]{
                "udt_catalog", "udt_schema", "udt_name", "attribute_name", "ordinal_position:i",
                "attribute_default:v", "is_nullable:v", "data_type:v", "character_maximum_length:i",
                "character_octet_length:i", "character_set_catalog", "character_set_schema",
                "character_set_name", "collation_catalog", "collation_schema", "collation_name",
                "numeric_precision:i", "numeric_precision_radix:i", "numeric_scale:i",
                "datetime_precision:i", "interval_type:v", "interval_precision:i",
                "attribute_udt_catalog", "attribute_udt_schema", "attribute_udt_name",
                "scope_catalog", "scope_schema", "scope_name", "maximum_cardinality:i",
                "dtd_identifier", "is_derived_reference_attribute:v"});
        m.put("character_sets", new String[]{
                "character_set_catalog", "character_set_schema", "character_set_name",
                "character_repertoire", "form_of_use", "default_collate_catalog",
                "default_collate_schema", "default_collate_name"});
        m.put("check_constraint_routine_usage", new String[]{
                "constraint_catalog", "constraint_schema", "constraint_name",
                "specific_catalog", "specific_schema", "specific_name"});
        m.put("collation_character_set_applicability", new String[]{
                "collation_catalog", "collation_schema", "collation_name",
                "character_set_catalog", "character_set_schema", "character_set_name"});
        m.put("column_column_usage", new String[]{
                "table_catalog", "table_schema", "table_name", "column_name", "dependent_column"});
        m.put("column_domain_usage", new String[]{
                "domain_catalog", "domain_schema", "domain_name",
                "table_catalog", "table_schema", "table_name", "column_name"});
        m.put("column_options", new String[]{
                "table_catalog", "table_schema", "table_name", "column_name",
                "option_name", "option_value:v"});
        m.put("column_privileges", new String[]{
                "grantor", "grantee", "table_catalog", "table_schema", "table_name",
                "column_name", "privilege_type:v", "is_grantable:v"});
        m.put("column_udt_usage", new String[]{
                "udt_catalog", "udt_schema", "udt_name",
                "table_catalog", "table_schema", "table_name", "column_name"});
        m.put("data_type_privileges", new String[]{
                "object_catalog", "object_schema", "object_name", "object_type:v",
                "dtd_identifier"});
        m.put("domain_constraints", new String[]{
                "constraint_catalog", "constraint_schema", "constraint_name",
                "domain_catalog", "domain_schema", "domain_name",
                "is_deferrable:v", "initially_deferred:v"});
        m.put("domain_udt_usage", new String[]{
                "udt_catalog", "udt_schema", "udt_name",
                "domain_catalog", "domain_schema", "domain_name"});
        m.put("element_types", new String[]{
                "object_catalog", "object_schema", "object_name", "object_type:v",
                "collection_type_identifier", "data_type:v", "character_maximum_length:i",
                "character_octet_length:i", "character_set_catalog", "character_set_schema",
                "character_set_name", "collation_catalog", "collation_schema", "collation_name",
                "numeric_precision:i", "numeric_precision_radix:i", "numeric_scale:i",
                "datetime_precision:i", "interval_type:v", "interval_precision:i",
                "udt_catalog", "udt_schema", "udt_name", "scope_catalog", "scope_schema",
                "scope_name", "maximum_cardinality:i", "dtd_identifier"});
        m.put("foreign_data_wrapper_options", new String[]{
                "foreign_data_wrapper_catalog", "foreign_data_wrapper_name",
                "option_name", "option_value:v"});
        m.put("foreign_data_wrappers", new String[]{
                "foreign_data_wrapper_catalog", "foreign_data_wrapper_name",
                "authorization_identifier", "library_name:v", "foreign_data_wrapper_language:v"});
        m.put("foreign_server_options", new String[]{
                "foreign_server_catalog", "foreign_server_name", "option_name", "option_value:v"});
        m.put("foreign_servers", new String[]{
                "foreign_server_catalog", "foreign_server_name", "foreign_data_wrapper_catalog",
                "foreign_data_wrapper_name", "foreign_server_type:v", "foreign_server_version:v",
                "authorization_identifier"});
        m.put("foreign_table_options", new String[]{
                "foreign_table_catalog", "foreign_table_schema", "foreign_table_name",
                "option_name", "option_value:v"});
        m.put("foreign_tables", new String[]{
                "foreign_table_catalog", "foreign_table_schema", "foreign_table_name",
                "foreign_server_catalog", "foreign_server_name"});
        m.put("information_schema_catalog_name", new String[]{"catalog_name"});
        m.put("role_column_grants", new String[]{
                "grantor", "grantee", "table_catalog", "table_schema", "table_name",
                "column_name", "privilege_type:v", "is_grantable:v"});
        m.put("role_table_grants", new String[]{
                "grantor", "grantee", "table_catalog", "table_schema", "table_name",
                "privilege_type:v", "is_grantable:v", "with_hierarchy:v"});
        m.put("role_routine_grants", new String[]{
                "grantor", "grantee", "specific_catalog", "specific_schema", "specific_name",
                "routine_catalog", "routine_schema", "routine_name",
                "privilege_type:v", "is_grantable:v"});
        m.put("role_udt_grants", new String[]{
                "grantor", "grantee", "udt_catalog", "udt_schema", "udt_name",
                "privilege_type:v", "is_grantable:v"});
        m.put("role_usage_grants", new String[]{
                "grantor", "grantee", "object_catalog", "object_schema", "object_name",
                "object_type:v", "privilege_type:v", "is_grantable:v"});
        m.put("routine_column_usage", new String[]{
                "specific_catalog", "specific_schema", "specific_name",
                "routine_catalog", "routine_schema", "routine_name",
                "table_catalog", "table_schema", "table_name", "column_name"});
        m.put("routine_privileges", new String[]{
                "grantor", "grantee", "specific_catalog", "specific_schema", "specific_name",
                "routine_catalog", "routine_schema", "routine_name",
                "privilege_type:v", "is_grantable:v"});
        m.put("routine_routine_usage", new String[]{
                "specific_catalog", "specific_schema", "specific_name",
                "routine_catalog", "routine_schema", "routine_name"});
        m.put("routine_sequence_usage", new String[]{
                "specific_catalog", "specific_schema", "specific_name",
                "routine_catalog", "routine_schema", "routine_name",
                "sequence_catalog", "sequence_schema", "sequence_name"});
        m.put("routine_table_usage", new String[]{
                "specific_catalog", "specific_schema", "specific_name",
                "routine_catalog", "routine_schema", "routine_name",
                "table_catalog", "table_schema", "table_name"});
        m.put("sql_features", new String[]{
                "feature_id:v", "feature_name:v", "sub_feature_id:v", "sub_feature_name:v",
                "is_supported:v", "is_verified_by:v", "comments:v"});
        m.put("sql_implementation_info", new String[]{
                "implementation_info_id:v", "implementation_info_name:v", "integer_value:i",
                "character_value:v", "comments:v"});
        m.put("sql_parts", new String[]{
                "feature_id:v", "feature_name:v", "is_supported:v", "is_verified_by:v",
                "comments:v"});
        m.put("sql_sizing", new String[]{
                "sizing_id:i", "sizing_name:v", "supported_value:i", "comments:v"});
        m.put("table_privileges", new String[]{
                "grantor", "grantee", "table_catalog", "table_schema", "table_name",
                "privilege_type:v", "is_grantable:v", "with_hierarchy:v"});
        m.put("transforms", new String[]{
                "udt_catalog", "udt_schema", "udt_name", "specific_catalog", "specific_schema",
                "specific_name", "group_name", "transform_type:v"});
        m.put("triggered_update_columns", new String[]{
                "trigger_catalog", "trigger_schema", "trigger_name", "event_object_catalog",
                "event_object_schema", "event_object_table", "event_object_column"});
        m.put("udt_privileges", new String[]{
                "grantor", "grantee", "udt_catalog", "udt_schema", "udt_name",
                "privilege_type:v", "is_grantable:v"});
        m.put("usage_privileges", new String[]{
                "grantor", "grantee", "object_catalog", "object_schema", "object_name",
                "object_type:v", "privilege_type:v", "is_grantable:v"});
        m.put("user_defined_types", new String[]{
                "user_defined_type_catalog", "user_defined_type_schema", "user_defined_type_name",
                "user_defined_type_category:v", "is_instantiable:v", "is_final:v",
                "ordering_form:v", "ordering_category:v", "ordering_routine_catalog",
                "ordering_routine_schema", "ordering_routine_name", "reference_type:v",
                "data_type:v", "character_maximum_length:i", "character_octet_length:i",
                "character_set_catalog", "character_set_schema", "character_set_name",
                "collation_catalog", "collation_schema", "collation_name", "numeric_precision:i",
                "numeric_precision_radix:i", "numeric_scale:i", "datetime_precision:i",
                "interval_type:v", "interval_precision:i", "source_dtd_identifier",
                "ref_dtd_identifier"});
        m.put("user_mapping_options", new String[]{
                "authorization_identifier", "foreign_server_catalog", "foreign_server_name",
                "option_name", "option_value:v"});
        m.put("user_mappings", new String[]{
                "authorization_identifier", "foreign_server_catalog", "foreign_server_name"});
        m.put("view_column_usage", new String[]{
                "view_catalog", "view_schema", "view_name",
                "table_catalog", "table_schema", "table_name", "column_name"});
        m.put("view_routine_usage", new String[]{
                "table_catalog", "table_schema", "table_name",
                "specific_catalog", "specific_schema", "specific_name"});
        m.put("view_table_usage", new String[]{
                "view_catalog", "view_schema", "view_name",
                "table_catalog", "table_schema", "table_name"});
        // The underscore-prefixed helper views PG's own information_schema is built on
        m.put("_pg_foreign_data_wrappers", new String[]{
                "oid:o", "fdwowner:o", "fdwoptions:a", "foreign_data_wrapper_catalog",
                "foreign_data_wrapper_name", "authorization_identifier",
                "foreign_data_wrapper_language:v"});
        m.put("_pg_foreign_servers", new String[]{
                "oid:o", "srvoptions:a", "foreign_server_catalog", "foreign_server_name",
                "foreign_data_wrapper_catalog", "foreign_data_wrapper_name",
                "foreign_server_type:v", "foreign_server_version:v", "authorization_identifier"});
        m.put("_pg_foreign_table_columns", new String[]{
                "nspname", "relname", "attname", "attfdwoptions:a"});
        m.put("_pg_foreign_tables", new String[]{
                "foreign_table_catalog", "foreign_table_schema", "foreign_table_name",
                "ftoptions:a", "foreign_server_catalog", "foreign_server_name",
                "authorization_identifier"});
        m.put("_pg_user_mappings", new String[]{
                "oid:o", "umoptions:a", "umuser:o", "authorization_identifier",
                "foreign_server_catalog", "foreign_server_name", "srvowner"});
        DECLARED_VIEW_COLUMNS = Collections.unmodifiableMap(m);
    }

    private Table buildIsTables() {
        List<Column> cols = Cols.listOf(
                new Column("table_catalog", DataType.TEXT, true, false, null),
                new Column("table_schema", DataType.TEXT, true, false, null),
                new Column("table_name", DataType.TEXT, true, false, null),
                new Column("table_type", DataType.TEXT, true, false, null),
                new Column("self_referencing_column_name", DataType.TEXT, true, false, null),
                new Column("reference_generation", DataType.TEXT, true, false, null),
                new Column("user_defined_type_catalog", DataType.TEXT, true, false, null),
                new Column("user_defined_type_schema", DataType.TEXT, true, false, null),
                new Column("user_defined_type_name", DataType.TEXT, true, false, null),
                new Column("is_insertable_into", DataType.TEXT, true, false, null),
                new Column("is_typed", DataType.TEXT, true, false, null),
                // What a temporary table does at COMMIT. PG only ever fills this in for a
                // local temporary table, so the column is null everywhere else — but a tool
                // that selects it has to find it there.
                new Column("commit_action", DataType.TEXT, true, false, null)
        );
        Table table = new Table("tables", cols);

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            // A table in a pg_temp namespace is temporary, and the standard has a table type of
            // its own for it — a tool that lists BASE TABLE only would otherwise take it for a
            // permanent table it can go on reading in the next session.
            boolean temp = schemaEntry.getKey().toLowerCase().startsWith("pg_temp");
            for (String tableName : schemaEntry.getValue().getTables().keySet()) {
                table.insertRow(new Object[]{
                        catalogName(), schemaEntry.getKey(), tableName,
                        temp ? "LOCAL TEMPORARY" : "BASE TABLE",
                        null, null, null, null, null, "YES", "NO", null
                });
            }
        }

        // A foreign table is listed here under a table type of its own. A tool that enumerates
        // this view to find out what it may read saw nothing at all for one, and then could not
        // explain the pg_class row it had already found.
        for (Object[] entry : ForeignTables.live(database)) {
            Database.FdwForeignTable ft = (Database.FdwForeignTable) entry[1];
            table.insertRow(new Object[]{
                    catalogName(), (String) entry[0], ft.tableName, "FOREIGN",
                    null, null, null, null, null, "YES", "NO", null
            });
        }

        for (Database.ViewDef vd : database.getViews().values()) {
            // M21: PG's information_schema excludes materialized views
            if (vd.materialized()) continue;
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            // A simple view can be inserted into, and information_schema.views says so for the
            // same view; the two views describe one object and have to agree about it.
            String insertable = isSimpleUpdatableView(vd) ? "YES" : "NO";
            table.insertRow(new Object[]{
                    catalogName(), vSchema, vd.name(), "VIEW",
                    null, null, null, null, null, insertable, "NO", null
            });
        }

        // The system catalogs are relations too, and PostgreSQL lists every one of them here.
        // A schema browser that enumerates through information_schema rather than pg_class saw
        // an empty system schema and never reached the columns pg_attribute describes — the two
        // halves of the introspection surface have to agree about what exists.
        for (String pgTable : PG_CATALOG_TABLES_FOR_IS) {
            boolean isTable = "r".equals(PgCatalogRelations.relkind(pgTable));
            table.insertRow(new Object[]{
                    catalogName(), "pg_catalog", pgTable, isTable ? "BASE TABLE" : "VIEW",
                    null, null, null, null, null, isTable ? "YES" : "NO", "NO"
            });
        }

        // information_schema describes itself in PG: its own views are listed here, and tools that
        // enumerate the schema to see which standard views they can rely on read exactly this.
        for (String isView : INFORMATION_SCHEMA_VIEWS) {
            table.insertRow(new Object[]{
                    catalogName(), "information_schema", isView, "VIEW",
                    null, null, null, null, null, "NO", "NO", null
            });
        }
        return table;
    }

    private Table buildIsColumns() {
        Session savedSession = currentSession;
        List<Column> cols = Cols.listOf(
                new Column("table_catalog", DataType.TEXT, true, false, null),
                new Column("table_schema", DataType.TEXT, true, false, null),
                new Column("table_name", DataType.TEXT, true, false, null),
                new Column("column_name", DataType.TEXT, true, false, null),
                new Column("ordinal_position", DataType.INTEGER, true, false, null),
                new Column("column_default", DataType.TEXT, true, false, null),
                new Column("is_nullable", DataType.TEXT, true, false, null),
                new Column("data_type", DataType.TEXT, true, false, null),
                new Column("character_maximum_length", DataType.INTEGER, true, false, null),
                new Column("character_octet_length", DataType.INTEGER, true, false, null),
                new Column("numeric_precision", DataType.INTEGER, true, false, null),
                new Column("numeric_precision_radix", DataType.INTEGER, true, false, null),
                new Column("numeric_scale", DataType.INTEGER, true, false, null),
                new Column("datetime_precision", DataType.INTEGER, true, false, null),
                new Column("interval_type", DataType.TEXT, true, false, null),
                new Column("interval_precision", DataType.INTEGER, true, false, null),
                new Column("character_set_catalog", DataType.TEXT, true, false, null),
                new Column("character_set_schema", DataType.TEXT, true, false, null),
                new Column("character_set_name", DataType.TEXT, true, false, null),
                new Column("collation_catalog", DataType.TEXT, true, false, null),
                new Column("collation_schema", DataType.TEXT, true, false, null),
                new Column("collation_name", DataType.TEXT, true, false, null),
                new Column("domain_catalog", DataType.TEXT, true, false, null),
                new Column("domain_schema", DataType.TEXT, true, false, null),
                new Column("domain_name", DataType.TEXT, true, false, null),
                new Column("udt_catalog", DataType.TEXT, true, false, null),
                new Column("udt_schema", DataType.TEXT, true, false, null),
                new Column("udt_name", DataType.TEXT, true, false, null),
                new Column("scope_catalog", DataType.TEXT, true, false, null),
                new Column("scope_schema", DataType.TEXT, true, false, null),
                new Column("scope_name", DataType.TEXT, true, false, null),
                new Column("maximum_cardinality", DataType.INTEGER, true, false, null),
                new Column("dtd_identifier", DataType.TEXT, true, false, null),
                new Column("is_self_referencing", DataType.TEXT, true, false, null),
                new Column("is_identity", DataType.TEXT, true, false, null),
                new Column("identity_generation", DataType.TEXT, true, false, null),
                new Column("identity_start", DataType.TEXT, true, false, null),
                new Column("identity_increment", DataType.TEXT, true, false, null),
                new Column("identity_maximum", DataType.TEXT, true, false, null),
                new Column("identity_minimum", DataType.TEXT, true, false, null),
                new Column("identity_cycle", DataType.TEXT, true, false, null),
                new Column("is_generated", DataType.TEXT, true, false, null),
                new Column("generation_expression", DataType.TEXT, true, false, null),
                new Column("is_updatable", DataType.TEXT, true, false, null)
        );
        Table table = new Table("columns", cols);

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                addColumnsForTable(table, schemaEntry.getKey(), t, true);
            }
        }

        // A foreign table's columns are described here exactly as a base table's are: the
        // relation is listed in information_schema.tables, and a view that names its columns
        // nowhere would leave a reader unable to write a query against a relation it can see.
        for (Object[] entry : ForeignTables.live(database)) {
            Database.FdwForeignTable ft = (Database.FdwForeignTable) entry[1];
            addColumnsForTable(table, (String) entry[0], foreignTableShape(ft), true);
        }

        // View columns: resolved output columns stored on the ViewDef. Materialized
        // views are excluded — PG's information_schema covers only SQL-standard
        // objects, so matviews appear in pg_attribute/pg_class but not here.
        for (Database.ViewDef vd : database.getViews().values()) {
            if (vd.materialized()) continue;
            if (vd.cachedColumns() == null || vd.cachedColumns().isEmpty()) continue;
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            addColumnsForTable(table, vSchema, new Table(vd.name(), vd.cachedColumns()), false);
        }

        // Also add pg_catalog virtual table columns so that queries like
        // SELECT ... FROM information_schema.columns WHERE table_schema = 'pg_catalog' work.
        PgCatalogBuilder pgBuilder = new PgCatalogBuilder(database, oids);
        for (String pgTable : PG_CATALOG_TABLES_FOR_IS) {
            Table pgCatTable;
            try {
                pgCatTable = pgBuilder.build(pgTable);
            } catch (MemgresException notAvailable) {
                // A relation the server declines to answer for at all (pg_stat_statements needs
                // shared_preload_libraries) has no columns to list. Anything else is a defect,
                // and swallowing it here would silently drop columns instead of reporting it.
                continue;
            }
            if (pgCatTable != null && !pgCatTable.getColumns().isEmpty()) {
                // Several names answer from one builder (pg_stat_all_tables and
                // pg_stat_user_tables are the same relation), so the listing is made under the
                // name that was asked for rather than the name the builder gave the table.
                addColumnsForTable(table, "pg_catalog", pgTable, pgCatTable, false);
            }
        }

        // information_schema describes its own views here too, so a tool that reads the listing
        // in information_schema.tables can go on to ask what columns those views have.
        if (!buildingIsColumns) {
            buildingIsColumns = true;
            try {
                for (String isView : INFORMATION_SCHEMA_VIEWS) {
                    // The declared column list answers this question without gathering the
                    // view's rows. information_schema.columns is rebuilt for every statement
                    // that reads it, so paying for the content of sixty views to describe
                    // their headings would make the catalog slow for no extra truth.
                    Table view = "columns".equals(isView) ? table : declaredView(isView);
                    if (view == null) view = build(isView, currentSession);
                    if (view != null && !view.getColumns().isEmpty()
                            && !"dummy".equals(view.getColumns().get(0).getName())) {
                        addColumnsForTable(table, "information_schema", view, false);
                    }
                }
            } finally {
                buildingIsColumns = false;
                this.currentSession = savedSession;
            }
        }

        return table;
    }

    /** Guards the self-description pass in {@link #buildIsColumns} against re-entering itself. */
    private boolean buildingIsColumns;

    /**
     * The SQL-standard description of a data type, as information_schema reports it in every
     * view that describes one — columns, domains, attributes, parameters. Which of the length,
     * numeric and datetime fields apply is a property of the type, so working it out once here
     * keeps all of those views saying the same thing about the same type.
     */
    static final class TypeFacts {
        final Integer charMaxLen;
        final Integer charOctetLen;
        final Integer numPrec;
        final Integer numPrecRadix;
        final Integer numScale;
        final Integer datetimePrec;
        final Integer intervalPrec;

        TypeFacts(DataType dt, Integer typmodPrecision, Integer typmodScale) {
            boolean isCharType = dt == DataType.VARCHAR || dt == DataType.CHAR
                    || dt == DataType.TEXT || dt == DataType.NAME;
            // character_maximum_length is the declared width of varchar(N)/char(N); text and an
            // undecorated varchar have none. The octet length is the UTF-8 worst case.
            // A bit string declares its width the same way, and PG reports it here too — but in
            // bits, so it has no octet length at all.
            boolean isBitType = dt == DataType.BIT || dt == DataType.VARBIT;
            charMaxLen = (dt == DataType.VARCHAR || dt == DataType.CHAR || isBitType)
                    ? typmodPrecision : null;
            if (isBitType) charOctetLen = null;
            else if (charMaxLen != null) charOctetLen = charMaxLen * 4;
            else charOctetLen = isCharType ? Integer.valueOf(1073741824) : null;

            Integer precision = CatalogHelper.numericPrecision(dt);
            if (dt == DataType.NUMERIC) precision = typmodPrecision;
            numPrec = precision;
            // numeric counts in decimal digits and reports radix 10 even undecorated; the binary
            // types report 2, and a type that is not numeric at all reports nothing.
            if (dt == DataType.NUMERIC) numPrecRadix = Integer.valueOf(10);
            else numPrecRadix = precision != null ? Integer.valueOf(2) : null;
            if (dt == DataType.NUMERIC) {
                // Only report scale when precision is declared (plain numeric has no limit)
                numScale = typmodPrecision != null
                        ? (typmodScale != null ? typmodScale : Integer.valueOf(0))
                        : null;
            } else {
                numScale = precision != null && dt != DataType.REAL
                        && dt != DataType.DOUBLE_PRECISION ? Integer.valueOf(0) : null;
            }

            // A temporal type declares how many fractional-second digits it keeps, and PG
            // reports that typmod here: timestamp(3) is precision 3, not the type's default 6.
            datetimePrec = datetimePrecision(dt, typmodPrecision);
            // interval_precision is the leading-field precision of "interval year to month" and
            // friends; a plain interval(3) carries its typmod in datetime_precision only.
            intervalPrec = null;
        }
    }

    /**
     * The datetime_precision information_schema reports for a temporal type: the declared
     * fractional-seconds typmod when the column has one, the type's own default otherwise.
     * A date keeps no fractional seconds at all, so it reports 0.
     */
    static Integer datetimePrecision(DataType dt, Integer typmod) {
        if (dt == DataType.DATE) return Integer.valueOf(0);
        boolean temporal = dt == DataType.TIMESTAMP || dt == DataType.TIMESTAMPTZ
                || dt == DataType.TIME || dt == DataType.TIMETZ || dt == DataType.INTERVAL;
        if (!temporal) return null;
        if (typmod != null && typmod >= 0 && typmod <= 6) return typmod;
        return Integer.valueOf(6);
    }

    /**
     * The interval_type information_schema reports: the field qualifier as written, upper case,
     * with the fractional-seconds precision it was declared with — "DAY TO SECOND(3)". A plain
     * interval and every other type report nothing.
     */
    static String intervalTypeOf(String qualifier, Integer precision) {
        if (qualifier == null) return null;
        String text = qualifier.toUpperCase();
        if (precision != null) text += "(" + precision + ")";
        return text;
    }

    /**
     * Add column entries for a table to the information_schema.columns table.
     *
     * <p>The rows are gathered before any of them is added, so a relation is either listed with
     * every column it has or not listed at all. A listing that stopped partway would contradict
     * pg_attribute about the same relation while still looking like an answer.
     */
    /**
     * A foreign table's declared columns as a relation shape, so that the views describing
     * columns can read it with the same code they read a stored table with. Its columns are held
     * as name/type text, and a type memgres does not know is described as text rather than
     * dropped — a column missing from the catalog is a column no client can ask about.
     */
    private static Table foreignTableShape(Database.FdwForeignTable ft) {
        List<Column> cols = new ArrayList<>();
        if (ft.columns != null) {
            for (String[] col : ft.columns) {
                if (col.length == 0 || col[0] == null) continue;
                DataType dt = null;
                if (col.length > 1 && col[1] != null) dt = DataType.fromPgName(col[1]);
                cols.add(new Column(col[0], dt != null ? dt : DataType.TEXT, true, false, null));
            }
        }
        return new Table(ft.tableName, cols);
    }

    private void addColumnsForTable(Table isTable, String schemaName, Table t, boolean isUserTable) {
        addColumnsForTable(isTable, schemaName, t.getName(), t, isUserTable);
    }

    /** @param relationName the name to list the columns under, which may be an alias of t */
    private void addColumnsForTable(Table isTable, String schemaName, String relationName,
                                    Table t, boolean isUserTable) {
        List<Object[]> pending = new ArrayList<Object[]>();
        int ordinal = 0;
        for (int i = 0; i < t.getColumns().size(); i++) {
            Column col = t.getColumns().get(i);
            // Asked of the catalog rather than by name alone: pg_replication_slots has a
            // genuine xmin column of type xid, and dropping it here would leave this view
            // one column short of what pg_class and pg_attribute say the relation has.
            if (!isUserTable && CatalogCoreBuilder.isSystemColumn(col)) continue;
            ordinal++;
            DataType dt = col.getType();
            // PG reports every array column as data_type 'ARRAY' and leaves the element type to
            // udt_name, so the test is the type's arrayness rather than a list of four of them.
            boolean isArrayType = DataType.isArrayType(dt) || dt == DataType.ACLITEM_ARRAY;
            // H14: data_type — arrays report "ARRAY", composite types report
            // "USER-DEFINED", but DOMAIN columns report their BASE type (PG puts
            // the domain name in domain_name and the base type in data_type).
            String dataType;
            if (isArrayType) {
                dataType = "ARRAY";
            } else if (isUserTable && col.getCompositeTypeName() != null) {
                dataType = "USER-DEFINED";
            } else {
                dataType = CatalogHelper.pgTypeName(dt);
            }
            TypeFacts facts = new TypeFacts(dt, col.getPrecision(), col.getScale());
            String intervalType = intervalTypeOf(col.getIntervalQualifier(), col.getPrecision());
            Integer charMaxLen = facts.charMaxLen;
            Integer charOctetLen = facts.charOctetLen;
            Integer numPrec = facts.numPrec;
            Integer numPrecRadix = facts.numPrecRadix;
            Integer numScale = facts.numScale;
            Integer datetimePrec = facts.datetimePrec;
            Integer intervalPrec = facts.intervalPrec;

            // H14: udt_name — serial/bigserial/smallserial report int4/int8/int2
            String udtSchema = "pg_catalog";
            String udtName;
            switch (dt) {
                case SERIAL: udtName = "int4"; break;
                case BIGSERIAL: udtName = "int8"; break;
                case SMALLSERIAL: udtName = "int2"; break;
                default: udtName = col.getType().getPgName(); break;
            }
            if (isUserTable) {
                // udt_schema and udt_name are two columns, so the type the column records is
                // split into the schema it lives in and the name it answers to there.
                if (dt == DataType.ENUM && col.getEnumTypeName() != null) {
                    udtSchema = TypeNamespace.schemaOfKey(col.getEnumTypeName());
                    udtName = TypeNamespace.nameOfKey(col.getEnumTypeName());
                } else if (col.getCompositeTypeName() != null) {
                    // H14: composite column — udt_name is the composite type name
                    udtSchema = TypeNamespace.schemaOfKey(col.getCompositeTypeName());
                    udtName = TypeNamespace.nameOfKey(col.getCompositeTypeName());
                }
                // H14: DOMAIN columns keep the BASE type udt_name (e.g. int4);
                // the domain identity is carried by the domain_* fields below.
            }

            // H14: is_identity — detect __identity__ marker in default value
            String defaultVal = col.getDefaultValue();
            boolean isIdentity = isUserTable && defaultVal != null && defaultVal.startsWith("__identity__");
            String identityGeneration = null;
            String identityStart = null, identityIncrement = null;
            String identityMaximum = null, identityMinimum = null;
            if (isIdentity) {
                identityGeneration = defaultVal.contains(":always:") ? "ALWAYS" : "BY DEFAULT";
                // The bounds of the sequence behind the identity column. A client that generates
                // keys itself reads these to know what values it may still use; leaving them
                // null made an IDENTITY column indistinguishable from an unbounded one.
                Sequence identitySeq = null;
                int seqAt = defaultVal.indexOf(":seq:");
                if (seqAt >= 0) {
                    identitySeq = database.getSequenceFor(schemaName, defaultVal.substring(seqAt + 5));
                }
                identityStart = identitySeq != null ? String.valueOf(identitySeq.getStartWith()) : "1";
                identityIncrement = identitySeq != null
                        ? String.valueOf(identitySeq.getIncrementBy()) : "1";
                identityMinimum = identitySeq != null ? String.valueOf(identitySeq.getMinValue()) : "1";
                // The ceiling is the column's own type, not the sequence counter's: an int
                // identity stops at 2147483647 whatever width the counter is kept in, and a
                // client that trusted the counter's ceiling would hand out unusable keys.
                identityMaximum = identitySeq != null
                        && identitySeq.getMaxValue() != Long.MAX_VALUE
                        ? String.valueOf(identitySeq.getMaxValue())
                        : String.valueOf(identityUpperBound(dt));
            }

            // H14: domain_* fields
            String domainCatalog = null, domainSchema = null, domainName = null;
            if (isUserTable && col.getDomainTypeName() != null) {
                domainCatalog = catalogName();
                // The domain lives where it was created, which need not be where the table is.
                DomainType colDomain = database.getDomain(col.getDomainTypeName());
                domainSchema = colDomain != null ? colDomain.getSchemaName()
                        : TypeNamespace.schemaOfKey(col.getDomainTypeName());
                domainName = TypeNamespace.nameOfKey(col.getDomainTypeName());
            }

            // H14: is_nullable — view columns are always YES (PG semantics)
            String isNullable = isUserTable ? (col.isNullable() ? "YES" : "NO") : "YES";

            pending.add(new Object[]{
                    catalogName(),                           // table_catalog
                    schemaName,                             // table_schema
                    relationName,                           // table_name
                    col.getName(),                          // column_name
                    ordinal,                                // ordinal_position
                    isUserTable ? CatalogHelper.formatColumnDefault(col) : null, // column_default
                    isNullable,                             // is_nullable
                    dataType,                               // data_type
                    charMaxLen,                             // character_maximum_length
                    charOctetLen,                           // character_octet_length
                    numPrec,                                // numeric_precision
                    numPrecRadix,                           // numeric_precision_radix
                    numScale,                               // numeric_scale
                    datetimePrec,                           // datetime_precision
                    intervalType,                           // interval_type
                    intervalPrec,                           // interval_precision
                    null, null, null,                       // character_set_*
                    null, null, null,                       // collation_*
                    domainCatalog, domainSchema, domainName, // domain_*
                    catalogName(),                           // udt_catalog
                    udtSchema,                              // udt_schema
                    udtName,                                // udt_name
                    null, null, null,                       // scope_*
                    null,                                   // maximum_cardinality
                    String.valueOf(ordinal),                // dtd_identifier
                    "NO",                                   // is_self_referencing
                    isIdentity ? "YES" : "NO",              // is_identity
                    identityGeneration,                     // identity_generation
                    identityStart,                          // identity_start
                    identityIncrement,                      // identity_increment
                    identityMaximum,                        // identity_maximum
                    identityMinimum,                        // identity_minimum
                    // PG answers identity_cycle for every column, identity or not, and answers
                    // NO unless the identity sequence was declared CYCLE.
                    "NO",                                   // identity_cycle
                    isUserTable && col.isGenerated() ? "ALWAYS" : "NEVER", // is_generated
                    isUserTable ? col.getGeneratedExpr() : null, // generation_expression
                    columnIsUpdatable(schemaName, relationName, col.getName(), isUserTable)
            });
        }
        for (Object[] row : pending) isTable.insertRow(row);
    }

    /**
     * What {@code information_schema.columns.is_updatable} reports: PG derives it from
     * {@code pg_column_is_updatable}, so it says YES for a table column, and for a view column
     * only when an UPDATE through that view could actually assign to it. Reporting a constant
     * YES told a migration tool it could write columns the engine refuses.
     */
    private String columnIsUpdatable(String schemaName, String relationName, String columnName,
                                     boolean isUserTable) {
        if (isUserTable) return "YES";
        Database.ViewDef vd = database.getView(schemaName, relationName);
        if (vd != null) {
            return ViewUpdatability.columnIsUpdatable(database, schemaName, relationName,
                    columnName, false) ? "YES" : "NO";
        }
        // A system relation: the catalog tables are tables, the catalog views are views, and a
        // view no write can reach reports NO the same way a user view does.
        if ("pg_catalog".equalsIgnoreCase(schemaName)) {
            return "r".equals(PgCatalogRelations.relkind(relationName)) ? "YES" : "NO";
        }
        if ("information_schema".equalsIgnoreCase(schemaName)) return "NO";
        return "YES";
    }

    private Table buildIsSchemata() {
        List<Column> cols = Cols.listOf(
                new Column("catalog_name", DataType.TEXT, true, false, null),
                new Column("schema_name", DataType.TEXT, true, false, null),
                new Column("schema_owner", DataType.TEXT, true, false, null),
                new Column("default_character_set_catalog", DataType.TEXT, true, false, null),
                new Column("default_character_set_schema", DataType.TEXT, true, false, null),
                new Column("default_character_set_name", DataType.TEXT, true, false, null),
                // The SQL path of the schema. PG has no per-schema path and always reports null.
                new Column("sql_path", DataType.TEXT, true, false, null)
        );
        Table table = new Table("schemata", cols);
        for (String schemaName : database.getSchemas().keySet()) {
            table.insertRow(new Object[]{catalogName(), schemaName, schemaOwner(schemaName),
                    null, null, null, null});
        }
        table.insertRow(new Object[]{catalogName(), "pg_catalog", schemaOwner("pg_catalog"), null, null, null, null});
        table.insertRow(new Object[]{catalogName(), "information_schema", schemaOwner("information_schema"), null, null, null, null});
        table.insertRow(new Object[]{catalogName(), "pg_toast", schemaOwner("pg_toast"), null, null, null, null});
        return table;
    }

    /**
     * Who owns a schema. PostgreSQL hands the public schema to the pg_database_owner
     * pseudo-role rather than to a named user, and a tool reads exactly that to decide whether
     * public is the database's shared schema or one particular user's.
     */
    private String schemaOwner(String schemaName) {
        if ("public".equalsIgnoreCase(schemaName)) return "pg_database_owner";
        String owner = database.getObjectOwner("schema:" + schemaName.toLowerCase());
        return owner != null ? owner : bootstrapUser();
    }

    private Table buildIsTableConstraints() {
        List<Column> cols = Cols.listOf(
                new Column("constraint_catalog", DataType.TEXT, true, false, null),
                new Column("constraint_schema", DataType.TEXT, true, false, null),
                new Column("constraint_name", DataType.TEXT, true, false, null),
                new Column("table_catalog", DataType.TEXT, true, false, null),
                new Column("table_schema", DataType.TEXT, true, false, null),
                new Column("table_name", DataType.TEXT, true, false, null),
                new Column("constraint_type", DataType.TEXT, true, false, null),
                new Column("is_deferrable", DataType.TEXT, true, false, null),
                new Column("initially_deferred", DataType.TEXT, true, false, null),
                new Column("enforced", DataType.TEXT, true, false, null),
                // Whether a UNIQUE constraint treats two nulls as different values. Only a
                // uniqueness constraint has an answer; PG leaves the rest null.
                new Column("nulls_distinct", DataType.TEXT, true, false, null)
        );
        Table table = new Table("table_constraints", cols);

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                for (StoredConstraint sc : t.getConstraints()) {
                    // UNIQUE constraints from CREATE UNIQUE INDEX (not ADD CONSTRAINT) are not in information_schema.table_constraints
                    if (sc.getType() == StoredConstraint.Type.UNIQUE && sc.isFromIndex()) continue;
                    // M21: EXCLUDE constraints are not exposed in the SQL-standard
                    // information_schema.table_constraints (PG omits them).
                    if (sc.getType() == StoredConstraint.Type.EXCLUDE) continue;
                    String type;
                    switch (sc.getType()) {
                        case PRIMARY_KEY:
                            type = "PRIMARY KEY";
                            break;
                        case UNIQUE:
                            type = "UNIQUE";
                            break;
                        case CHECK:
                            type = "CHECK";
                            break;
                        case FOREIGN_KEY:
                            type = "FOREIGN KEY";
                            break;
                        case EXCLUDE:
                            type = "EXCLUDE";
                            break;
                        default:
                            throw new IllegalStateException("Unknown constraint type: " + sc.getType());
                    }
                    String nullsDistinct = sc.getType() == StoredConstraint.Type.UNIQUE
                            ? (sc.isNullsNotDistinct() ? "NO" : "YES") : null;
                    table.insertRow(new Object[]{
                            catalogName(), schemaEntry.getKey(), sc.getName(),
                            catalogName(), schemaEntry.getKey(), t.getName(),
                            type,
                            sc.isDeferrable() ? "YES" : "NO",
                            sc.isInitiallyDeferred() ? "YES" : "NO",
                            sc.isNotEnforced() ? "NO" : "YES", nullsDistinct
                    });
                }
                // PG 18: NOT NULL constraints appear in information_schema.table_constraints
                java.util.Set<String> isPromotedUniqueCols = new java.util.HashSet<>();
                for (StoredConstraint usc : t.getConstraints()) {
                    if (usc.getType() == StoredConstraint.Type.UNIQUE && usc.isPromotedFromIndex()) {
                        for (String c : usc.getColumns()) isPromotedUniqueCols.add(c.toLowerCase());
                    }
                }
                for (Column col : t.getColumns()) {
                    boolean isPromotedUnique = isPromotedUniqueCols.contains(col.getName().toLowerCase());
                    // Emit NOT NULL for all NOT NULL columns (including PK columns),
                    // but skip columns covered by UNIQUE constraints promoted from index
                    if (!col.isNullable() && !isPromotedUnique) {
                        String conname = notNullConstraintName(t, col);
                        table.insertRow(new Object[]{
                                catalogName(), schemaEntry.getKey(), conname,
                                catalogName(), schemaEntry.getKey(), t.getName(),
                                "CHECK", "NO", "NO", "YES", null
                        });
                    }
                }
            }
        }
        return table;
    }

    private Table buildIsKeyColumnUsage() {
        List<Column> cols = Cols.listOf(
                new Column("constraint_catalog", DataType.TEXT, true, false, null),
                new Column("constraint_schema", DataType.TEXT, true, false, null),
                new Column("constraint_name", DataType.TEXT, true, false, null),
                new Column("table_catalog", DataType.TEXT, true, false, null),
                new Column("table_schema", DataType.TEXT, true, false, null),
                new Column("table_name", DataType.TEXT, true, false, null),
                new Column("column_name", DataType.TEXT, true, false, null),
                new Column("ordinal_position", DataType.INTEGER, true, false, null),
                new Column("position_in_unique_constraint", DataType.INTEGER, true, false, null)
        );
        Table table = new Table("key_column_usage", cols);

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.CHECK) continue;
                    // UNIQUE constraints from CREATE UNIQUE INDEX (not ADD CONSTRAINT) are not in key_column_usage
                    if (sc.getType() == StoredConstraint.Type.UNIQUE && sc.isFromIndex()) continue;
                    for (int i = 0; i < sc.getColumns().size(); i++) {
                        Integer posInUnique = null;
                        if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY) {
                            posInUnique = i + 1;
                        }
                        table.insertRow(new Object[]{
                                catalogName(), schemaEntry.getKey(), sc.getName(),
                                catalogName(), schemaEntry.getKey(), t.getName(),
                                sc.getColumns().get(i), i + 1, posInUnique
                        });
                    }
                }
            }
        }
        return table;
    }

    private Table buildIsReferentialConstraints() {
        List<Column> cols = Cols.listOf(
                new Column("constraint_catalog", DataType.TEXT, true, false, null),
                new Column("constraint_schema", DataType.TEXT, true, false, null),
                new Column("constraint_name", DataType.TEXT, true, false, null),
                new Column("unique_constraint_catalog", DataType.TEXT, true, false, null),
                new Column("unique_constraint_schema", DataType.TEXT, true, false, null),
                new Column("unique_constraint_name", DataType.TEXT, true, false, null),
                new Column("match_option", DataType.TEXT, true, false, null),
                new Column("update_rule", DataType.TEXT, true, false, null),
                new Column("delete_rule", DataType.TEXT, true, false, null)
        );
        Table table = new Table("referential_constraints", cols);

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                    // Find the referenced PK/unique constraint name
                    String refConstraintName = CatalogHelper.findReferencedConstraintName(CatalogHelper.findTable(database, sc.getReferencesTable()));
                    table.insertRow(new Object[]{
                            catalogName(), schemaEntry.getKey(), sc.getName(),
                            catalogName(), schemaEntry.getKey(), refConstraintName,
                            "NONE",
                            CatalogHelper.fkActionToString(sc.getOnUpdate()),
                            CatalogHelper.fkActionToString(sc.getOnDelete())
                    });
                }
            }
        }
        return table;
    }

    private Table buildIsRoutines() {
        List<Column> cols = Cols.listOf(
                new Column("specific_catalog", DataType.TEXT, true, false, null),
                new Column("specific_schema", DataType.TEXT, true, false, null),
                new Column("specific_name", DataType.TEXT, true, false, null),
                new Column("routine_catalog", DataType.TEXT, true, false, null),
                new Column("routine_schema", DataType.TEXT, true, false, null),
                new Column("routine_name", DataType.TEXT, true, false, null),
                new Column("routine_type", DataType.TEXT, true, false, null),
                new Column("module_catalog", DataType.TEXT, true, false, null),
                new Column("module_schema", DataType.TEXT, true, false, null),
                new Column("module_name", DataType.TEXT, true, false, null),
                new Column("udt_catalog", DataType.TEXT, true, false, null),
                new Column("udt_schema", DataType.TEXT, true, false, null),
                new Column("udt_name", DataType.TEXT, true, false, null),
                new Column("data_type", DataType.TEXT, true, false, null),
                new Column("character_maximum_length", DataType.INTEGER, true, false, null),
                new Column("character_octet_length", DataType.INTEGER, true, false, null),
                new Column("character_set_catalog", DataType.TEXT, true, false, null),
                new Column("character_set_schema", DataType.TEXT, true, false, null),
                new Column("character_set_name", DataType.TEXT, true, false, null),
                new Column("collation_catalog", DataType.TEXT, true, false, null),
                new Column("collation_schema", DataType.TEXT, true, false, null),
                new Column("collation_name", DataType.TEXT, true, false, null),
                new Column("numeric_precision", DataType.INTEGER, true, false, null),
                new Column("numeric_precision_radix", DataType.INTEGER, true, false, null),
                new Column("numeric_scale", DataType.INTEGER, true, false, null),
                new Column("datetime_precision", DataType.INTEGER, true, false, null),
                new Column("interval_type", DataType.TEXT, true, false, null),
                new Column("interval_precision", DataType.INTEGER, true, false, null),
                new Column("type_udt_catalog", DataType.TEXT, true, false, null),
                new Column("type_udt_schema", DataType.TEXT, true, false, null),
                new Column("type_udt_name", DataType.TEXT, true, false, null),
                new Column("scope_catalog", DataType.TEXT, true, false, null),
                new Column("scope_schema", DataType.TEXT, true, false, null),
                new Column("scope_name", DataType.TEXT, true, false, null),
                new Column("maximum_cardinality", DataType.INTEGER, true, false, null),
                new Column("dtd_identifier", DataType.TEXT, true, false, null),
                new Column("routine_body", DataType.TEXT, true, false, null),
                new Column("routine_definition", DataType.TEXT, true, false, null),
                new Column("external_name", DataType.TEXT, true, false, null),
                new Column("external_language", DataType.TEXT, true, false, null),
                new Column("parameter_style", DataType.TEXT, true, false, null),
                new Column("is_deterministic", DataType.TEXT, true, false, null),
                new Column("sql_data_access", DataType.TEXT, true, false, null),
                new Column("is_null_call", DataType.TEXT, true, false, null),
                new Column("sql_path", DataType.TEXT, true, false, null),
                new Column("schema_level_routine", DataType.TEXT, true, false, null),
                new Column("max_dynamic_result_sets", DataType.INTEGER, true, false, null),
                new Column("is_user_defined_cast", DataType.TEXT, true, false, null),
                new Column("is_implicitly_invocable", DataType.TEXT, true, false, null),
                new Column("security_type", DataType.TEXT, true, false, null),
                new Column("to_sql_specific_catalog", DataType.TEXT, true, false, null),
                new Column("to_sql_specific_schema", DataType.TEXT, true, false, null),
                new Column("to_sql_specific_name", DataType.TEXT, true, false, null),
                new Column("as_locator", DataType.TEXT, true, false, null),
                new Column("created", DataType.TIMESTAMPTZ, true, false, null),
                new Column("last_altered", DataType.TIMESTAMPTZ, true, false, null),
                new Column("new_savepoint_level", DataType.TEXT, true, false, null),
                new Column("is_udt_dependent", DataType.TEXT, true, false, null),
                new Column("result_cast_from_data_type", DataType.TEXT, true, false, null),
                new Column("result_cast_as_locator", DataType.TEXT, true, false, null),
                new Column("result_cast_char_max_length", DataType.INTEGER, true, false, null),
                new Column("result_cast_char_octet_length", DataType.INTEGER, true, false, null),
                new Column("result_cast_char_set_catalog", DataType.TEXT, true, false, null),
                new Column("result_cast_char_set_schema", DataType.TEXT, true, false, null),
                new Column("result_cast_char_set_name", DataType.TEXT, true, false, null),
                new Column("result_cast_collation_catalog", DataType.TEXT, true, false, null),
                new Column("result_cast_collation_schema", DataType.TEXT, true, false, null),
                new Column("result_cast_collation_name", DataType.TEXT, true, false, null),
                new Column("result_cast_numeric_precision", DataType.INTEGER, true, false, null),
                new Column("result_cast_numeric_precision_radix", DataType.INTEGER, true, false, null),
                new Column("result_cast_numeric_scale", DataType.INTEGER, true, false, null),
                new Column("result_cast_datetime_precision", DataType.INTEGER, true, false, null),
                new Column("result_cast_interval_type", DataType.TEXT, true, false, null),
                new Column("result_cast_interval_precision", DataType.INTEGER, true, false, null),
                new Column("result_cast_type_udt_catalog", DataType.TEXT, true, false, null),
                new Column("result_cast_type_udt_schema", DataType.TEXT, true, false, null),
                new Column("result_cast_type_udt_name", DataType.TEXT, true, false, null),
                new Column("result_cast_scope_catalog", DataType.TEXT, true, false, null),
                new Column("result_cast_scope_schema", DataType.TEXT, true, false, null),
                new Column("result_cast_scope_name", DataType.TEXT, true, false, null),
                new Column("result_cast_maximum_cardinality", DataType.INTEGER, true, false, null),
                new Column("result_cast_dtd_identifier", DataType.TEXT, true, false, null)
        );
        Table table = new Table("routines", cols);
        int specificSeq = 1;
        for (Map.Entry<String, PgFunction> entry : database.getFunctions().entrySet()) {
            PgFunction fn = entry.getValue();
            String schema = fn.getSchemaName() != null ? fn.getSchemaName() : "public";
            String routineType = fn.isProcedure() ? "PROCEDURE" : "FUNCTION";
            String returnType = fn.getReturnType() != null ? fn.getReturnType() : "void";
            String language = fn.getLanguage() != null ? fn.getLanguage() : "plpgsql";
            String specificName = fn.getName() + "_" + (specificSeq++);
            // routine_body is SQL for a function whose body is SQL the server understands, and
            // EXTERNAL for every other language. Calling a LANGUAGE sql function EXTERNAL told
            // a reader the definition was not SQL it could parse.
            String routineBody = "sql".equalsIgnoreCase(language) ? "SQL" : "EXTERNAL";
            String routineDefinition = fn.getBody();
            // data_type is the SQL name of the return type and type_udt_name its internal one:
            // "integer" and "int4", not the same word twice.
            TypeSpec retSpec = parseTypeSpec(returnType);
            String retDataType = retSpec.type != null
                    ? CatalogHelper.pgTypeName(retSpec.type) : returnType;
            String retUdtName = retSpec.type != null ? retSpec.type.getPgName() : returnType;

            table.insertRow(new Object[]{
                    catalogName(),       // specific_catalog
                    schema,              // specific_schema
                    specificName,        // specific_name
                    catalogName(),       // routine_catalog
                    schema,              // routine_schema
                    fn.getName(),        // routine_name
                    routineType,         // routine_type
                    null,                // module_catalog
                    null,                // module_schema
                    null,                // module_name
                    null,                // udt_catalog
                    null,                // udt_schema
                    null,                // udt_name
                    retDataType,         // data_type
                    null,                // character_maximum_length
                    null,                // character_octet_length
                    null,                // character_set_catalog
                    null,                // character_set_schema
                    null,                // character_set_name
                    null,                // collation_catalog
                    null,                // collation_schema
                    null,                // collation_name
                    null,                // numeric_precision
                    null,                // numeric_precision_radix
                    null,                // numeric_scale
                    null,                // datetime_precision
                    null,                // interval_type
                    null,                // interval_precision
                    catalogName(),       // type_udt_catalog
                    "pg_catalog",        // type_udt_schema
                    retUdtName,          // type_udt_name
                    null,                // scope_catalog
                    null,                // scope_schema
                    null,                // scope_name
                    null,                // maximum_cardinality
                    "0",                 // dtd_identifier
                    routineBody,         // routine_body
                    routineDefinition,   // routine_definition
                    null,                // external_name
                    language.toUpperCase(), // external_language (M21: PG uppercases: SQL/PLPGSQL)
                    "GENERAL",           // parameter_style
                    "NO",                // is_deterministic
                    "MODIFIES",          // sql_data_access
                    // PG reports is_null_call from the routine's strictness, and answers NO for
                    // the ordinary non-strict function. Answering YES for every function said
                    // something about strictness that was not so.
                    fn.isProcedure() ? null : (fn.isStrict() ? "YES" : "NO"), // is_null_call
                    null,                // sql_path
                    "YES",               // schema_level_routine
                    0,                   // max_dynamic_result_sets
                    // PG has no answer for these three and reports null. Writing NO/NO/YES
                    // turned "not applicable" into a definite claim about the routine.
                    null,                // is_user_defined_cast
                    null,                // is_implicitly_invocable
                    fn.isSecurityDefiner() ? "DEFINER" : "INVOKER", // security_type
                    null,                // to_sql_specific_catalog
                    null,                // to_sql_specific_schema
                    null,                // to_sql_specific_name
                    "NO",                // as_locator
                    null,                // created
                    null,                // last_altered
                    null,                // new_savepoint_level (PG reports none)
                    "NO",                // is_udt_dependent
                    null,                // result_cast_from_data_type
                    null,                // result_cast_as_locator
                    null,                // result_cast_char_max_length
                    null,                // result_cast_char_octet_length
                    null,                // result_cast_char_set_catalog
                    null,                // result_cast_char_set_schema
                    null,                // result_cast_char_set_name
                    null,                // result_cast_collation_catalog
                    null,                // result_cast_collation_schema
                    null,                // result_cast_collation_name
                    null,                // result_cast_numeric_precision
                    null,                // result_cast_numeric_precision_radix
                    null,                // result_cast_numeric_scale
                    null,                // result_cast_datetime_precision
                    null,                // result_cast_interval_type
                    null,                // result_cast_interval_precision
                    null,                // result_cast_type_udt_catalog
                    null,                // result_cast_type_udt_schema
                    null,                // result_cast_type_udt_name
                    null,                // result_cast_scope_catalog
                    null,                // result_cast_scope_schema
                    null,                // result_cast_scope_name
                    null,                // result_cast_maximum_cardinality
                    null                 // result_cast_dtd_identifier
            });
        }
        return table;
    }

    private Table buildIsSequences() {
        List<Column> cols = Cols.listOf(
                new Column("sequence_catalog", DataType.TEXT, true, false, null),
                new Column("sequence_schema", DataType.TEXT, true, false, null),
                new Column("sequence_name", DataType.TEXT, true, false, null),
                new Column("data_type", DataType.TEXT, true, false, null),
                new Column("numeric_precision", DataType.INTEGER, true, false, null),
                // A sequence counts in binary, and its scale is fixed at whole numbers; the
                // standard still expects both alongside the precision.
                new Column("numeric_precision_radix", DataType.INTEGER, true, false, null),
                new Column("numeric_scale", DataType.INTEGER, true, false, null),
                new Column("start_value", DataType.TEXT, true, false, null),
                new Column("minimum_value", DataType.TEXT, true, false, null),
                new Column("maximum_value", DataType.TEXT, true, false, null),
                new Column("increment", DataType.TEXT, true, false, null),
                new Column("cycle_option", DataType.TEXT, true, false, null)
        );
        Table table = new Table("sequences", cols);
        // M14: collect identity-owned sequence names to exclude from information_schema
        java.util.Set<String> identitySeqs = new java.util.HashSet<>();
        for (Schema schema : database.getSchemas().values()) {
            for (Table tbl : schema.getTables().values()) {
                for (Column col : tbl.getColumns()) {
                    String def = col.getDefaultValue();
                    if (def != null && def.contains(":seq:")) {
                        identitySeqs.add(Database.seqKey(schema.getName(),
                                def.substring(def.indexOf(":seq:") + 5)));
                    }
                }
            }
        }
        for (String qualified : CatalogHelper.getSequenceNames(database)) {
            String seqName = CatalogHelper.nameOf(qualified);
            if (identitySeqs.contains(Database.seqKey(CatalogHelper.schemaOf(qualified), seqName))) {
                continue; // M14: exclude identity sequences
            }
            Sequence seq = database.getSequence(qualified);
            if (seq != null) {
                String dataType = seq.getDataType() != null ? seq.getDataType() : "bigint";
                int precision = "smallint".equals(dataType) ? 16 : "integer".equals(dataType) ? 32 : 64;
                String cycleOption = seq.isCycle() ? "YES" : "NO";
                table.insertRow(new Object[]{
                        catalogName(), CatalogHelper.schemaOf(qualified), seqName, dataType, precision, 2, 0,
                        String.valueOf(seq.getStartWith()),
                        String.valueOf(seq.getMinValue()),
                        String.valueOf(seq.getMaxValue()),
                        String.valueOf(seq.getIncrementBy()),
                        cycleOption
                });
            }
        }
        return table;
    }

    private Table buildIsViews() {
        List<Column> cols = Cols.listOf(
                new Column("table_catalog", DataType.TEXT, true, false, null),
                new Column("table_schema", DataType.TEXT, true, false, null),
                new Column("table_name", DataType.TEXT, true, false, null),
                new Column("view_definition", DataType.TEXT, true, false, null),
                new Column("check_option", DataType.TEXT, true, false, null),
                new Column("is_updatable", DataType.TEXT, true, false, null),
                new Column("is_insertable_into", DataType.TEXT, true, false, null),
                new Column("is_trigger_updatable", DataType.TEXT, true, false, null),
                new Column("is_trigger_deletable", DataType.TEXT, true, false, null),
                new Column("is_trigger_insertable_into", DataType.TEXT, true, false, null)
        );
        Table table = new Table("views", cols);
        for (Database.ViewDef vd : database.getViews().values()) {
            // M21: PG's information_schema.views excludes materialized views
            if (vd.materialized()) continue;
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            String viewDef = "";
            if (vd.query() != null) {
                String raw = vd.sourceSQL() != null ? vd.sourceSQL() : SqlUnparser.toSql(vd.query());
                // M19: information_schema.views.view_definition uses the pretty form (with trailing ;).
                viewDef = SqlUnparser.prettyViewDef(raw) + ";";
            }
            // PG derives these from pg_relation_is_updatable: is_updatable wants UPDATE and
            // DELETE both, is_insertable_into wants INSERT. Deriving them from the same place
            // the executor decides from is what keeps the catalog from promising a write the
            // engine then refuses.
            int events = ViewUpdatability.relationEvents(database, vSchema, vd.name(), false);
            String isUpdatable =
                    (events & (ViewUpdatability.UPDATE | ViewUpdatability.DELETE))
                            == (ViewUpdatability.UPDATE | ViewUpdatability.DELETE) ? "YES" : "NO";
            String isInsertable = (events & ViewUpdatability.INSERT) != 0 ? "YES" : "NO";
            // An INSTEAD OF trigger is reported separately from auto-updatability: a view can be
            // read-only and still take the write, and a client has to be able to tell which.
            int trig = ViewUpdatability.insteadOfEvents(database, vd.name());
            // WITH CHECK OPTION is what stops an UPDATE through the view from writing a row the
            // view cannot see. Reporting NONE for a view that has one told a client the write
            // would go through unchecked.
            String checkOption = vd.checkOption() == null ? "NONE"
                    : vd.checkOption().toUpperCase();
            table.insertRow(new Object[]{
                    catalogName(), vSchema, vd.name(), viewDef, checkOption,
                    isUpdatable, isInsertable,
                    (trig & ViewUpdatability.UPDATE) != 0 ? "YES" : "NO",
                    (trig & ViewUpdatability.DELETE) != 0 ? "YES" : "NO",
                    (trig & ViewUpdatability.INSERT) != 0 ? "YES" : "NO"
            });
        }
        return table;
    }

    /**
     * Whether a view accepts INSERT, in the sense information_schema.tables reports it — the same
     * answer information_schema.views gives for is_insertable_into. Both come from the rules the
     * executor writes through, so the two views and the write itself cannot disagree.
     */
    private boolean isSimpleUpdatableView(Database.ViewDef vd) {
        if (vd.materialized()) return false;
        String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
        int events = ViewUpdatability.relationEvents(database, vSchema, vd.name(), false);
        return (events & ViewUpdatability.INSERT) != 0;
    }

    private Table buildIsDomains() {
        // A domain is a data type, so the view describes it with the same fields that describe a
        // column's type — the length, numeric and datetime facts, and the base type's identity
        // in udt_*. A tool reading a domain to size an input needs all of them, not just a name.
        List<Column> cols = Cols.listOf(
                new Column("domain_catalog", DataType.TEXT, true, false, null),
                new Column("domain_schema", DataType.TEXT, true, false, null),
                new Column("domain_name", DataType.TEXT, true, false, null),
                new Column("data_type", DataType.TEXT, true, false, null),
                new Column("character_maximum_length", DataType.INTEGER, true, false, null),
                new Column("character_octet_length", DataType.INTEGER, true, false, null),
                new Column("character_set_catalog", DataType.TEXT, true, false, null),
                new Column("character_set_schema", DataType.TEXT, true, false, null),
                new Column("character_set_name", DataType.TEXT, true, false, null),
                new Column("collation_catalog", DataType.TEXT, true, false, null),
                new Column("collation_schema", DataType.TEXT, true, false, null),
                new Column("collation_name", DataType.TEXT, true, false, null),
                new Column("numeric_precision", DataType.INTEGER, true, false, null),
                new Column("numeric_precision_radix", DataType.INTEGER, true, false, null),
                new Column("numeric_scale", DataType.INTEGER, true, false, null),
                new Column("datetime_precision", DataType.INTEGER, true, false, null),
                new Column("interval_type", DataType.TEXT, true, false, null),
                new Column("interval_precision", DataType.INTEGER, true, false, null),
                new Column("domain_default", DataType.TEXT, true, false, null),
                new Column("udt_catalog", DataType.TEXT, true, false, null),
                new Column("udt_schema", DataType.TEXT, true, false, null),
                new Column("udt_name", DataType.TEXT, true, false, null),
                new Column("scope_catalog", DataType.TEXT, true, false, null),
                new Column("scope_schema", DataType.TEXT, true, false, null),
                new Column("scope_name", DataType.TEXT, true, false, null),
                new Column("maximum_cardinality", DataType.INTEGER, true, false, null),
                new Column("dtd_identifier", DataType.TEXT, true, false, null)
        );
        Table table = new Table("domains", cols);
        for (Map.Entry<String, DomainType> entry : database.getDomains().entrySet()) {
            DomainType d = entry.getValue();
            DataType base = d.getBaseType();
            // A domain over an array is described as the array: its element's width is not the
            // domain's, so none of the length or numeric facts apply to it.
            boolean isArray = d.isArray();
            TypeFacts facts = isArray ? null : new TypeFacts(base, d.getPrecision(), d.getScale());
            String dataType = isArray
                    ? CatalogHelper.pgTypeName(d.getArrayElementType()) + "[]"
                    : CatalogHelper.pgTypeName(base);
            table.insertRow(new Object[]{
                    catalogName(), d.getSchemaName(), d.getName(),
                    dataType,
                    facts == null ? null : facts.charMaxLen, facts == null ? null : facts.charOctetLen,
                    null, null, null,                       // character_set_*
                    null, null, null,                       // collation_*
                    facts == null ? null : facts.numPrec,
                    facts == null ? null : facts.numPrecRadix,
                    facts == null ? null : facts.numScale,
                    facts == null ? null : facts.datetimePrec,
                    intervalTypeOf(d.getIntervalQualifier(), d.getPrecision()),
                    facts == null ? null : facts.intervalPrec,
                    CatalogHelper.formatDomainDefault(d),
                    catalogName(), "pg_catalog", base.getPgName(),
                    null, null, null,                       // scope_*
                    null,                                   // maximum_cardinality
                    "1"                                     // dtd_identifier
            });
        }
        return table;
    }

    private Table buildIsCheckConstraints() {
        List<Column> cols = Cols.listOf(
                new Column("constraint_catalog", DataType.TEXT, true, false, null),
                new Column("constraint_schema", DataType.TEXT, true, false, null),
                new Column("constraint_name", DataType.TEXT, true, false, null),
                new Column("check_clause", DataType.TEXT, true, false, null)
        );
        Table table = new Table("check_constraints", cols);
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.CHECK && sc.getName() != null) {
                        table.insertRow(new Object[]{
                                catalogName(), schemaEntry.getKey(), sc.getName(),
                                sc.getCheckExpr() != null
                                        ? RuleDeparser.deparse(sc.getCheckExpr(), RuleDeparser.forTable(t)) : ""
                        });
                    }
                }
                // H15: PG 18 lists NOT NULL constraints in check_constraints too,
                // with a "<col> IS NOT NULL" clause.
                for (Column col : t.getColumns()) {
                    if (col.isNullable()) continue;
                    String conname = notNullConstraintName(t, col);
                    table.insertRow(new Object[]{
                            catalogName(), schemaEntry.getKey(), conname,
                            col.getName() + " IS NOT NULL"
                    });
                }
            }
        }
        // H15: domain CHECK constraints also appear in check_constraints.
        for (Map.Entry<String, DomainType> entry : database.getDomains().entrySet()) {
            DomainType d = entry.getValue();
            // A domain constraint belongs to the schema the domain was created in. Filing every
            // one of them under public hid it from the query that names the domain's own schema.
            String dSchema = domainSchema(d);
            if (d.getParsedCheck() != null) {
                table.insertRow(new Object[]{
                        catalogName(), dSchema, d.getName() + "_check",
                        "(" + stripOuterParensLocal(CatalogHelper.renderDomainCheck(d, d.getParsedCheck())) + ")"
                });
            }
            for (DomainType.NamedConstraint nc : d.getNamedConstraints()) {
                table.insertRow(new Object[]{
                        catalogName(), dSchema, nc.name(),
                        "(" + stripOuterParensLocal(CatalogHelper.renderDomainCheck(d, nc.parsedCheck)) + ")"
                });
            }
        }
        return table;
    }

    /**
     * Constraint name for a column's NOT NULL. Partition children inherit the
     * name from the partition parent that first declared the column NOT NULL (L13).
     */
    private static String notNullConstraintName(Table t, Column col) {
        Table owner = t;
        Table parent = t.getPartitionParent();
        while (parent != null) {
            int idx = parent.getColumnIndex(col.getName());
            if (idx < 0 || parent.getColumns().get(idx).isNullable()) break;
            owner = parent;
            parent = parent.getPartitionParent();
        }
        String named = owner.notNullConstraintName(col.getName());
        return named != null ? named : owner.getName() + "_" + col.getName() + "_not_null";
    }

    /** Strip a single pair of balanced outer parentheses, if present. */
    private static String stripOuterParensLocal(String s) {
        if (s == null) return "";
        String str = s.trim();
        while (str.startsWith("(") && str.endsWith(")")) {
            int depth = 0;
            boolean wraps = true;
            for (int i = 0; i < str.length(); i++) {
                char ch = str.charAt(i);
                if (ch == '(') depth++;
                else if (ch == ')') { depth--; if (depth == 0 && i < str.length() - 1) { wraps = false; break; } }
            }
            if (wraps) str = str.substring(1, str.length() - 1).trim();
            else break;
        }
        return str;
    }

    private Table buildIsConstraintColumnUsage() {
        List<Column> cols = Cols.listOf(
                new Column("table_catalog", DataType.TEXT, true, false, null),
                new Column("table_schema", DataType.TEXT, true, false, null),
                new Column("table_name", DataType.TEXT, true, false, null),
                new Column("column_name", DataType.TEXT, true, false, null),
                new Column("constraint_catalog", DataType.TEXT, true, false, null),
                new Column("constraint_schema", DataType.TEXT, true, false, null),
                new Column("constraint_name", DataType.TEXT, true, false, null)
        );
        Table table = new Table("constraint_column_usage", cols);
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                for (StoredConstraint sc : tableEntry.getValue().getConstraints()) {
                    if (sc.getName() == null) continue;
                    // M21: EXCLUDE constraints not shown in information_schema
                    if (sc.getType() == StoredConstraint.Type.EXCLUDE) continue;
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY) {
                        // M21: FK shows referenced table/column, not the FK's own
                        String refSchema = sc.getReferencesSchema() != null ? sc.getReferencesSchema() : schemaEntry.getKey();
                        String refTable = sc.getReferencesTable();
                        if (refTable != null && sc.getReferencesColumns() != null) {
                            for (String refCol : sc.getReferencesColumns()) {
                                table.insertRow(new Object[]{
                                        catalogName(), refSchema, refTable, refCol,
                                        catalogName(), schemaEntry.getKey(), sc.getName()
                                });
                            }
                        }
                    } else if (sc.getType() == StoredConstraint.Type.CHECK) {
                        // A CHECK is used by the columns its expression reads.
                        for (String col : checkedColumnNames(tableEntry.getValue(), sc)) {
                            table.insertRow(new Object[]{
                                    catalogName(), schemaEntry.getKey(), tableEntry.getKey(), col,
                                    catalogName(), schemaEntry.getKey(), sc.getName()
                            });
                        }
                    } else if (sc.getColumns() != null) {
                        for (String col : sc.getColumns()) {
                            table.insertRow(new Object[]{
                                    catalogName(), schemaEntry.getKey(), tableEntry.getKey(), col,
                                    catalogName(), schemaEntry.getKey(), sc.getName()
                            });
                        }
                    }
                }
                // A NOT NULL constraint is a check over the column that carries it, and PG
                // lists it here under the name it gave it.
                for (Column col : tableEntry.getValue().getColumns()) {
                    if (col.isNullable()) continue;
                    table.insertRow(new Object[]{
                            catalogName(), schemaEntry.getKey(), tableEntry.getKey(), col.getName(),
                            catalogName(), schemaEntry.getKey(),
                            notNullConstraintName(tableEntry.getValue(), col)
                    });
                }
            }
        }
        return table;
    }

    /** The columns a CHECK expression reads, in the relation's own column order. */
    private static List<String> checkedColumnNames(Table t, StoredConstraint sc) {
        List<String> out = new ArrayList<>();
        if (sc.getCheckExpr() == null) return out;
        java.util.Set<String> named = new java.util.LinkedHashSet<>();
        for (String name : DdlExecutor.referencedColumnNames(sc.getCheckExpr())) {
            named.add(name.toLowerCase());
        }
        for (Column c : t.getColumns()) {
            if (named.contains(c.getName().toLowerCase())) out.add(c.getName());
        }
        return out;
    }

    private Table buildIsConstraintTableUsage() {
        List<Column> cols = Cols.listOf(
                new Column("table_catalog", DataType.TEXT, true, false, null),
                new Column("table_schema", DataType.TEXT, true, false, null),
                new Column("table_name", DataType.TEXT, true, false, null),
                new Column("constraint_catalog", DataType.TEXT, true, false, null),
                new Column("constraint_schema", DataType.TEXT, true, false, null),
                new Column("constraint_name", DataType.TEXT, true, false, null)
        );
        Table table = new Table("constraint_table_usage", cols);
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                for (StoredConstraint sc : tableEntry.getValue().getConstraints()) {
                    if (sc.getName() == null) continue;
                    // The view names the table a constraint USES, which for a foreign key is the
                    // table it points AT, not the one that carries it. Naming the referencing
                    // table made the question "which constraints depend on this table" answer
                    // with nothing, which is how a tool decides it is safe to drop it.
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY) {
                        String refTable = sc.getReferencesTable();
                        if (refTable == null) continue;
                        String refSchema = sc.getReferencesSchema() != null
                                ? sc.getReferencesSchema() : schemaEntry.getKey();
                        table.insertRow(new Object[]{
                                catalogName(), refSchema, refTable,
                                catalogName(), schemaEntry.getKey(), sc.getName()
                        });
                        continue;
                    }
                    // Only the key constraints appear here. PG leaves CHECK (and the NOT NULL
                    // constraints it synthesizes) to constraint_column_usage, and listing them
                    // made the view claim dependencies PostgreSQL does not report.
                    if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY
                            && sc.getType() != StoredConstraint.Type.UNIQUE) continue;
                    if (sc.getType() == StoredConstraint.Type.UNIQUE && sc.isFromIndex()) continue;
                    table.insertRow(new Object[]{
                            catalogName(), schemaEntry.getKey(), tableEntry.getKey(),
                            catalogName(), schemaEntry.getKey(), sc.getName()
                    });
                }
            }
        }
        return table;
    }

    private Table buildIsParameters() {
        List<Column> cols = Cols.listOf(
                new Column("specific_catalog", DataType.NAME, true, false, null),
                new Column("specific_schema", DataType.NAME, true, false, null),
                new Column("specific_name", DataType.NAME, true, false, null),
                new Column("ordinal_position", DataType.INTEGER, true, false, null),
                new Column("parameter_mode", DataType.VARCHAR, true, false, null),
                new Column("is_result", DataType.VARCHAR, true, false, null),
                new Column("as_locator", DataType.VARCHAR, true, false, null),
                new Column("parameter_name", DataType.NAME, true, false, null),
                new Column("data_type", DataType.VARCHAR, true, false, null),
                new Column("character_maximum_length", DataType.INTEGER, true, false, null),
                new Column("character_octet_length", DataType.INTEGER, true, false, null),
                new Column("character_set_catalog", DataType.NAME, true, false, null),
                new Column("character_set_schema", DataType.NAME, true, false, null),
                new Column("character_set_name", DataType.NAME, true, false, null),
                new Column("collation_catalog", DataType.NAME, true, false, null),
                new Column("collation_schema", DataType.NAME, true, false, null),
                new Column("collation_name", DataType.NAME, true, false, null),
                new Column("numeric_precision", DataType.INTEGER, true, false, null),
                new Column("numeric_precision_radix", DataType.INTEGER, true, false, null),
                new Column("numeric_scale", DataType.INTEGER, true, false, null),
                new Column("datetime_precision", DataType.INTEGER, true, false, null),
                new Column("interval_type", DataType.VARCHAR, true, false, null),
                new Column("interval_precision", DataType.INTEGER, true, false, null),
                new Column("udt_catalog", DataType.NAME, true, false, null),
                new Column("udt_schema", DataType.NAME, true, false, null),
                new Column("udt_name", DataType.NAME, true, false, null),
                new Column("scope_catalog", DataType.NAME, true, false, null),
                new Column("scope_schema", DataType.NAME, true, false, null),
                new Column("scope_name", DataType.NAME, true, false, null),
                new Column("maximum_cardinality", DataType.INTEGER, true, false, null),
                new Column("dtd_identifier", DataType.NAME, true, false, null),
                new Column("parameter_default", DataType.VARCHAR, true, false, null)
        );
        Table table = new Table("parameters", cols);
        // The specific_name sequence must line up with information_schema.routines, so the
        // two views join; both walk database.getFunctions() in insertion order.
        int specificSeq = 1;
        for (Map.Entry<String, PgFunction> entry : database.getFunctions().entrySet()) {
            PgFunction fn = entry.getValue();
            String schema = fn.getSchemaName() != null ? fn.getSchemaName() : "public";
            String specificName = fn.getName() + "_" + (specificSeq++);
            List<PgFunction.Param> params = fn.getParams();
            if (params == null) continue;
            for (int i = 0; i < params.size(); i++) {
                PgFunction.Param p = params.get(i);
                String mode = p.mode() == null || p.mode().isEmpty() ? "IN" : p.mode().toUpperCase();
                DataType dt = null;
                String udtName = p.typeName();
                if (udtName != null) {
                    try {
                        dt = DataType.fromPgName(udtName);
                    } catch (RuntimeException ignored) {
                        // A user-defined type: keep the written name as udt_name.
                    }
                }
                table.insertRow(new Object[]{
                        catalogName(),                                  // specific_catalog
                        schema,                                         // specific_schema
                        specificName,                                   // specific_name
                        i + 1,                                          // ordinal_position
                        mode,                                           // parameter_mode
                        "NO",                                           // is_result
                        "NO",                                           // as_locator
                        p.name(),                                       // parameter_name
                        dt != null ? CatalogHelper.pgTypeName(dt) : "USER-DEFINED", // data_type
                        null, null, null, null, null, null, null, null, // length / charset / collation
                        null, null, null, null, null, null,             // numeric / datetime / interval
                        catalogName(),                                  // udt_catalog
                        dt != null ? "pg_catalog" : schema,             // udt_schema
                        dt != null ? dt.getPgName() : udtName,          // udt_name
                        null, null, null, null,                         // scope / maximum_cardinality
                        String.valueOf(i + 1),                          // dtd_identifier
                        parameterDefault(p.defaultExpr(), dt)           // parameter_default
                });
            }
        }
        return table;
    }

    private Table buildIsTriggers() {
        List<Column> cols = Cols.listOf(
                new Column("trigger_catalog", DataType.TEXT, true, false, null),
                new Column("trigger_schema", DataType.TEXT, true, false, null),
                new Column("trigger_name", DataType.TEXT, true, false, null),
                new Column("event_manipulation", DataType.TEXT, true, false, null),
                new Column("event_object_catalog", DataType.TEXT, true, false, null),
                new Column("event_object_schema", DataType.TEXT, true, false, null),
                new Column("event_object_table", DataType.TEXT, true, false, null),
                new Column("action_order", DataType.INTEGER, true, false, null),
                new Column("action_condition", DataType.TEXT, true, false, null),
                new Column("action_statement", DataType.TEXT, true, false, null),
                new Column("action_orientation", DataType.TEXT, true, false, null),
                new Column("action_timing", DataType.TEXT, true, false, null),
                new Column("action_reference_old_table", DataType.TEXT, true, false, null),
                new Column("action_reference_new_table", DataType.TEXT, true, false, null),
                // The standard lets a trigger name its row variables; PG's are always OLD and
                // NEW, so it reports nothing here — but the columns are part of the view.
                new Column("action_reference_old_row", DataType.TEXT, true, false, null),
                new Column("action_reference_new_row", DataType.TEXT, true, false, null),
                new Column("created", DataType.TIMESTAMPTZ, true, false, null)
        );
        Table table = new Table("triggers", cols);
        // Group triggers by name to combine multiple events
        Map<String, List<PgTrigger>> byName = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, List<PgTrigger>> entry : database.getAllTriggers().entrySet()) {
            for (PgTrigger trigger : entry.getValue()) {
                byName.computeIfAbsent(trigger.getName(), k -> new java.util.ArrayList<>()).add(trigger);
            }
        }
        for (Map.Entry<String, List<PgTrigger>> entry : byName.entrySet()) {
            PgTrigger first = entry.getValue().get(0);
            String trigSchema = first.getSchemaName() != null ? first.getSchemaName() : "public";
            String timing;
            switch (first.getTiming()) {
                case BEFORE: timing = "BEFORE"; break;
                case AFTER: timing = "AFTER"; break;
                case INSTEAD_OF: timing = "INSTEAD OF"; break;
                default: timing = "AFTER"; break;
            }
            String orientation = first.isForEachStatement() ? "STATEMENT" : "ROW";
            // PG writes the trigger function schema-qualified. Replaying an unqualified name
            // depends on the reader's search_path and can land on a different function.
            String actionStmt = "EXECUTE FUNCTION "
                    + qualifiedFunctionName(first.getFunctionName(), trigSchema) + "()";
            // Emit one row per event manipulation (PG standard). Each trigger names its own
            // table and schema: two triggers of one name on two tables were both reported
            // against the first one's table, so a client saw one of them twice and the other
            // not at all.
            for (PgTrigger trig : entry.getValue()) {
                String event = trig.getEvent().name(); // INSERT, UPDATE, DELETE, TRUNCATE
                String ownSchema = trig.getSchemaName() != null ? trig.getSchemaName() : trigSchema;
                table.insertRow(new Object[]{
                        catalogName(),          // trigger_catalog
                        ownSchema,              // trigger_schema
                        trig.getName(),         // trigger_name
                        event,                  // event_manipulation
                        catalogName(),          // event_object_catalog
                        ownSchema,              // event_object_schema
                        trig.getTableName(),    // event_object_table
                        1,                      // action_order
                        null,                   // action_condition
                        actionStmt,             // action_statement
                        orientation,            // action_orientation
                        timing,                 // action_timing
                        null,                   // action_reference_old_table
                        null,                   // action_reference_new_table
                        null,                   // action_reference_old_row
                        null,                   // action_reference_new_row
                        null                    // created
                });
            }
        }
        return table;
    }

    private Table buildIsCollations() {
        List<Column> cols = Cols.listOf(
                new Column("collation_catalog", DataType.TEXT, true, false, null),
                new Column("collation_schema", DataType.TEXT, true, false, null),
                new Column("collation_name", DataType.TEXT, true, false, null),
                new Column("pad_attribute", DataType.TEXT, true, false, null)
        );
        Table table = new Table("collations", cols);
        String cat = catalogName();
        // Builtin collations that PG always provides
        table.insertRow(new Object[]{cat, "pg_catalog", "default", "NO PAD"});
        table.insertRow(new Object[]{cat, "pg_catalog", "C", "NO PAD"});
        table.insertRow(new Object[]{cat, "pg_catalog", "POSIX", "NO PAD"});
        table.insertRow(new Object[]{cat, "pg_catalog", "ucs_basic", "NO PAD"});
        // PG 17 added a built-in "unicode" collation alongside C and POSIX; a client that asks
        // for it by name has to find it listed before it will use it.
        table.insertRow(new Object[]{cat, "pg_catalog", "unicode", "NO PAD"});
        return table;
    }

    private Table buildIsEnabledRoles() {
        List<Column> cols = Cols.listOf(
                new Column("role_name", DataType.TEXT, true, false, null)
        );
        Table table = new Table("enabled_roles", cols);
        // Current user is always an enabled role
        String currentUser = currentSession != null && currentSession.getConnectingUser() != null
                ? currentSession.getConnectingUser() : "memgres";
        table.insertRow(new Object[]{currentUser});
        // Also include all roles the current user is a member of
        if (database.getRoleMemberships() != null) {
            for (Map.Entry<String, java.util.Set<String>> entry : database.getRoleMemberships().entrySet()) {
                if (entry.getValue().contains(currentUser)) {
                    table.insertRow(new Object[]{entry.getKey()});
                }
            }
        }
        return table;
    }

    private Table buildIsApplicableRoles() {
        List<Column> cols = Cols.listOf(
                new Column("grantee", DataType.TEXT, true, false, null),
                new Column("role_name", DataType.TEXT, true, false, null),
                new Column("is_grantable", DataType.TEXT, true, false, null)
        );
        Table table = new Table("applicable_roles", cols);
        String currentUser = currentSession != null && currentSession.getConnectingUser() != null
                ? currentSession.getConnectingUser() : "memgres";
        if (database.getRoleMemberships() != null) {
            for (Map.Entry<String, java.util.Set<String>> entry : database.getRoleMemberships().entrySet()) {
                if (entry.getValue().contains(currentUser)) {
                    table.insertRow(new Object[]{currentUser, entry.getKey(), "NO"});
                }
            }
        }
        return table;
    }

    // =====================================================================================
    // Privilege views
    //
    // PostgreSQL does not need a GRANT to have something to report here: creating a relation
    // grants its owner every privilege on it, and those implicit grants are what these views
    // are mostly made of. Reporting nothing until someone ran a GRANT told a tool that checks
    // "may I read this table" that the answer was no.
    // =====================================================================================

    /** The privileges an owner holds on a table or view, in the order PG lists them. */
    private static final String[] OWNER_TABLE_PRIVILEGES = {
            "INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"};

    /** The privileges that can be held on a single column rather than a whole relation. */
    private static final String[] OWNER_COLUMN_PRIVILEGES = {
            "INSERT", "SELECT", "UPDATE", "REFERENCES"};

    /** The user this session speaks as, which is also the owner of anything it creates. */
    private String currentUser() {
        return currentSession != null && currentSession.getConnectingUser() != null
                ? currentSession.getConnectingUser() : "memgres";
    }

    /** The role the server was bootstrapped with, which owns the system schemas. */
    private String bootstrapUser() {
        return "memgres";
    }

    /** The recorded owner of an object, falling back to the user of this session. */
    private String ownerOf(String objectKey) {
        String owner = database.getObjectOwner(objectKey);
        return owner != null ? owner : currentUser();
    }

    /** A relation memgres publishes to the SQL-standard views: a table or a non-materialized view. */
    private static final class Relation {
        final String schema;
        final String name;
        final List<Column> columns;
        final boolean isView;

        Relation(String schema, String name, List<Column> columns, boolean isView) {
            this.schema = schema;
            this.name = name;
            this.columns = columns;
            this.isView = isView;
        }

        String ownerKey() {
            return (isView ? "view:" : "table:") + schema.toLowerCase() + "." + name.toLowerCase();
        }
    }

    /**
     * Every user relation, tables first and then views, which is the set the SQL-standard views
     * describe. The system catalogs are deliberately not here: PostgreSQL grants those to PUBLIC
     * rather than to the connected user, and inventing owner grants for two hundred catalog
     * relations would add rows no client acts on to a view rebuilt for every statement.
     */
    private List<Relation> userRelations() {
        List<Relation> out = new ArrayList<Relation>();
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                out.add(new Relation(schemaEntry.getKey(), tableEntry.getValue().getName(),
                        tableEntry.getValue().getColumns(), false));
            }
        }
        for (Database.ViewDef vd : database.getViews().values()) {
            if (vd.materialized()) continue;
            if (vd.cachedColumns() == null) continue;
            out.add(new Relation(vd.schemaName() != null ? vd.schemaName() : "public",
                    vd.name(), vd.cachedColumns(), true));
        }
        return out;
    }

    /**
     * information_schema.table_privileges, and information_schema.role_table_grants — the same
     * rows. PG's role_* views keep every grant whose grantor or grantee is a role the session may
     * act as, and every grant to PUBLIC; with only owner and PUBLIC grants in play that is the
     * whole of the base view.
     */
    private Table buildIsTablePrivileges(String viewName) {
        Table table = declaredView(viewName);
        String catalog = catalogName();
        for (Relation rel : userRelations()) {
            String owner = ownerOf(rel.ownerKey());
            for (String priv : OWNER_TABLE_PRIVILEGES) {
                table.insertRow(new Object[]{owner, owner, catalog, rel.schema, rel.name,
                        priv, "YES", "SELECT".equals(priv) ? "YES" : "NO"});
            }
        }
        // Grants somebody actually issued, on top of the owner's own.
        for (Map.Entry<String, java.util.Set<String>> entry : database.getAllRolePrivileges().entrySet()) {
            String grantee = entry.getKey();
            for (String privEntry : entry.getValue()) {
                String[] parts = privEntry.split(":", 3);
                if (parts.length != 3 || !"TABLE".equalsIgnoreCase(parts[1])) continue;
                String privilege = parts[0];
                String objectName = parts[2];
                String schema = "public";
                String tableName = objectName;
                int dot = objectName.indexOf('.');
                if (dot >= 0) {
                    schema = objectName.substring(0, dot);
                    tableName = objectName.substring(dot + 1);
                }
                table.insertRow(new Object[]{currentUser(), grantee, catalog, schema, tableName,
                        privilege, "NO", "SELECT".equalsIgnoreCase(privilege) ? "YES" : "NO"});
            }
        }
        return table;
    }

    /** information_schema.column_privileges / role_column_grants. */
    private Table buildIsColumnPrivileges(String viewName) {
        Table table = declaredView(viewName);
        String catalog = catalogName();
        for (Relation rel : userRelations()) {
            String owner = ownerOf(rel.ownerKey());
            for (Column col : rel.columns) {
                if (CatalogCoreBuilder.isSystemColumn(col)) continue;
                for (String priv : OWNER_COLUMN_PRIVILEGES) {
                    table.insertRow(new Object[]{owner, owner, catalog, rel.schema, rel.name,
                            col.getName(), priv, "YES"});
                }
            }
        }
        return table;
    }

    /** information_schema.routine_privileges / role_routine_grants. */
    private Table buildIsRoutinePrivileges(String viewName) {
        Table table = declaredView(viewName);
        String catalog = catalogName();
        int specificSeq = 1;
        for (Map.Entry<String, PgFunction> entry : database.getFunctions().entrySet()) {
            PgFunction fn = entry.getValue();
            String schema = fn.getSchemaName() != null ? fn.getSchemaName() : "public";
            String specificName = fn.getName() + "_" + (specificSeq++);
            String owner = fn.getOwner() != null ? fn.getOwner() : currentUser();
            // PG grants EXECUTE on a new function to PUBLIC as well, and that is why a
            // non-owner may call it at all.
            table.insertRow(new Object[]{owner, "PUBLIC", catalog, schema, specificName,
                    catalog, schema, fn.getName(), "EXECUTE", "NO"});
            table.insertRow(new Object[]{owner, owner, catalog, schema, specificName,
                    catalog, schema, fn.getName(), "EXECUTE", "YES"});
        }
        return table;
    }

    /** information_schema.udt_privileges / role_udt_grants — USAGE on the composite types. */
    private Table buildIsUdtPrivileges(String viewName) {
        Table table = declaredView(viewName);
        String catalog = catalogName();
        // A relation has a composite type of its own, and PG reports USAGE on that type
        // alongside USAGE on the types CREATE TYPE made.
        for (Relation rel : userRelations()) {
            addUdtGrant(table, catalog, rel.schema, rel.name, ownerOf(rel.ownerKey()));
        }
        for (String ctKey : database.getCompositeTypes().keySet()) {
            addUdtGrant(table, catalog, compositeTypeSchema(ctKey), typeNameOf(ctKey),
                    ownerOf("type:" + ctKey));
        }
        return table;
    }

    private void addUdtGrant(Table table, String catalog, String schema, String name, String owner) {
        table.insertRow(new Object[]{owner, "PUBLIC", catalog, schema, name, "TYPE USAGE", "NO"});
        table.insertRow(new Object[]{owner, owner, catalog, schema, name, "TYPE USAGE", "YES"});
    }

    /** information_schema.usage_privileges / role_usage_grants — USAGE on the domains. */
    private Table buildIsUsagePrivileges(String viewName) {
        Table table = declaredView(viewName);
        String catalog = catalogName();
        for (Map.Entry<String, DomainType> entry : database.getDomains().entrySet()) {
            DomainType d = entry.getValue();
            String owner = ownerOf("domain:" + entry.getKey());
            table.insertRow(new Object[]{owner, "PUBLIC", catalog, domainSchema(d), d.getName(),
                    "DOMAIN", "USAGE", "NO"});
            table.insertRow(new Object[]{owner, owner, catalog, domainSchema(d), d.getName(),
                    "DOMAIN", "USAGE", "YES"});
        }
        return table;
    }

    /**
     * information_schema.data_type_privileges — one row per type descriptor in the database,
     * which is what the dtd_identifier of every other view points back at.
     */
    private Table buildIsDataTypePrivileges() {
        Table table = declaredView("data_type_privileges");
        String catalog = catalogName();
        for (Relation rel : userRelations()) {
            int ordinal = 0;
            for (Column col : rel.columns) {
                if (CatalogCoreBuilder.isSystemColumn(col)) continue;
                ordinal++;
                table.insertRow(new Object[]{catalog, rel.schema, rel.name, "TABLE",
                        String.valueOf(ordinal)});
            }
        }
        for (Map.Entry<String, DomainType> entry : database.getDomains().entrySet()) {
            table.insertRow(new Object[]{catalog, domainSchema(entry.getValue()),
                    entry.getValue().getName(), "DOMAIN", "1"});
        }
        for (Map.Entry<String, List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField>> ct
                : database.getCompositeTypes().entrySet()) {
            String schema = compositeTypeSchema(ct.getKey());
            for (int i = 0; i < ct.getValue().size(); i++) {
                table.insertRow(new Object[]{catalog, schema, typeNameOf(ct.getKey()),
                        "USER-DEFINED TYPE", String.valueOf(i + 1)});
            }
        }
        int specificSeq = 1;
        for (Map.Entry<String, PgFunction> entry : database.getFunctions().entrySet()) {
            PgFunction fn = entry.getValue();
            String schema = fn.getSchemaName() != null ? fn.getSchemaName() : "public";
            String specificName = fn.getName() + "_" + (specificSeq++);
            // Descriptor 0 is the result; the parameters follow it in order.
            table.insertRow(new Object[]{catalog, schema, specificName, "ROUTINE", "0"});
            List<PgFunction.Param> params = fn.getParams();
            if (params == null) continue;
            for (int i = 0; i < params.size(); i++) {
                table.insertRow(new Object[]{catalog, schema, specificName, "ROUTINE",
                        String.valueOf(i + 1)});
            }
        }
        return table;
    }

    /**
     * information_schema.element_types — what each array-typed descriptor holds. Nothing else in
     * information_schema answers that question, so without these rows an array column's element
     * type is not discoverable through the standard views at all.
     *
     * <p>One row per descriptor whose type is an array — a column, a composite attribute or a
     * domain — keyed by the collection's own identifier. A column typed by a domain over an array
     * is not one of them: the column's type is the domain, and it is the domain that carries the
     * array, so PostgreSQL lists the domain and not the column.
     */
    private Table buildIsElementTypes() {
        Table table = declaredView("element_types");
        String catalog = catalogName();
        for (Map.Entry<String, DomainType> entry : database.getDomains().entrySet()) {
            DomainType d = entry.getValue();
            if (!d.isArray()) continue;
            addElementTypeRow(table, catalog, domainSchema(d), d.getName(), "DOMAIN",
                    "1", d.getArrayElementType());
        }
        for (Relation rel : userRelations()) {
            int ordinal = 0;
            for (Column col : rel.columns) {
                if (CatalogCoreBuilder.isSystemColumn(col)) continue;
                ordinal++;
                // The column's type is written as a domain: the domain is the collection, and it
                // has a row of its own above. Listing the column here as well would name the same
                // array twice under two identifiers.
                if (col.getDomainTypeName() != null) continue;
                DataType element = DataType.elementOf(col.getType());
                if (element == null) continue;
                addElementTypeRow(table, catalog, rel.schema, rel.name, "TABLE",
                        String.valueOf(ordinal), element);
            }
        }
        for (Map.Entry<String, List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField>> ct
                : database.getCompositeTypes().entrySet()) {
            String schema = compositeTypeSchema(ct.getKey());
            List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField> fields = ct.getValue();
            for (int i = 0; i < fields.size(); i++) {
                TypeSpec spec = parseTypeSpec(fields.get(i).typeName());
                if (!spec.isArray || spec.type == null) continue;
                addElementTypeRow(table, catalog, schema, typeNameOf(ct.getKey()),
                        "USER-DEFINED TYPE", String.valueOf(i + 1), spec.type);
            }
        }
        return table;
    }

    private void addElementTypeRow(Table table, String catalog, String schema, String objectName,
                                   String objectType, String collectionId, DataType element) {
        // PG leaves the length, numeric and datetime facts of the element unfilled here — the
        // element carries no typmod of its own — and names the type in data_type and udt_name.
        table.insertRow(new Object[]{
                catalog, schema, objectName, objectType, collectionId,
                CatalogHelper.pgTypeName(element),
                null, null,                         // character_maximum_length / octet_length
                null, null, null,                   // character_set_*
                null, null, null,                   // collation_*
                null, null, null,                   // numeric_*
                null, null, null,                   // datetime_precision / interval_*
                catalog, "pg_catalog", element.getPgName(),
                null, null, null,                   // scope_*
                null,                               // maximum_cardinality
                "a" + collectionId                  // dtd_identifier
        });
    }

    /**
     * information_schema.attributes — the fields of a composite type. A composite type with no
     * rows here is a type a client can name but cannot take apart.
     */
    private Table buildIsAttributes() {
        Table table = declaredView("attributes");
        String catalog = catalogName();
        for (Map.Entry<String, List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField>> ct
                : database.getCompositeTypes().entrySet()) {
            String schema = compositeTypeSchema(ct.getKey());
            List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField> fields = ct.getValue();
            for (int i = 0; i < fields.size(); i++) {
                com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField f = fields.get(i);
                TypeSpec spec = parseTypeSpec(f.typeName());
                TypeFacts facts = spec.type == null || spec.isArray
                        ? null : new TypeFacts(spec.type, spec.precision, spec.scale);
                String dataType;
                if (spec.isArray) dataType = "ARRAY";
                else if (spec.type == null) dataType = "USER-DEFINED";
                else dataType = CatalogHelper.pgTypeName(spec.type);
                // An attribute's type has a schema of its own: a composite in b whose attribute
                // is of a.e reports a here, not b. The two are separate columns, so the recorded
                // type is split rather than printed whole.
                String udtName = spec.type != null
                        ? spec.type.getPgName() : TypeNamespace.nameOfKey(spec.baseName);
                String udtSchema = spec.type != null
                        ? "pg_catalog" : TypeNamespace.schemaOfKey(spec.baseName);
                table.insertRow(new Object[]{
                        catalog, schema, typeNameOf(ct.getKey()), f.name(), Integer.valueOf(i + 1),
                        null,                                   // attribute_default
                        "YES",                                  // is_nullable
                        dataType,
                        facts == null ? null : facts.charMaxLen,
                        facts == null ? null : facts.charOctetLen,
                        null, null, null,                       // character_set_*
                        null, null, null,                       // collation_*
                        facts == null ? null : facts.numPrec,
                        facts == null ? null : facts.numPrecRadix,
                        facts == null ? null : facts.numScale,
                        facts == null ? null : facts.datetimePrec,
                        null,                                   // interval_type
                        null,                                   // interval_precision
                        catalog, udtSchema, udtName,
                        null, null, null,                       // scope_*
                        null,                                   // maximum_cardinality
                        String.valueOf(i + 1),                  // dtd_identifier
                        "NO"                                    // is_derived_reference_attribute
                });
            }
        }
        return table;
    }

    /** information_schema.user_defined_types — the types CREATE TYPE ... AS (...) made. */
    private Table buildIsUserDefinedTypes() {
        Table table = declaredView("user_defined_types");
        String catalog = catalogName();
        for (String ctKey : database.getCompositeTypes().keySet()) {
            // PG answers only the first five columns for a composite type and leaves the rest
            // null: the remaining fields describe a DISTINCT type, which PG does not have.
            Object[] row = new Object[29];
            row[0] = catalog;
            row[1] = compositeTypeSchema(ctKey);
            row[2] = typeNameOf(ctKey);
            row[3] = "STRUCTURED";
            row[4] = "YES";
            table.insertRow(row);
        }
        return table;
    }

    /** information_schema.column_domain_usage — which columns are typed by a domain. */
    private Table buildIsColumnDomainUsage() {
        Table table = declaredView("column_domain_usage");
        String catalog = catalogName();
        for (Relation rel : userRelations()) {
            for (Column col : rel.columns) {
                if (col.getDomainTypeName() == null) continue;
                // The schema and the name are two columns here, so the recorded key is split
                // rather than printed whole.
                DomainType d = database.getDomain(col.getDomainTypeName());
                table.insertRow(new Object[]{catalog,
                        d != null ? domainSchema(d)
                                : TypeNamespace.schemaOfKey(col.getDomainTypeName()),
                        TypeNamespace.nameOfKey(col.getDomainTypeName()),
                        catalog, rel.schema, rel.name, col.getName()});
            }
        }
        return table;
    }

    /** information_schema.column_udt_usage — the underlying type of every column, by name. */
    private Table buildIsColumnUdtUsage() {
        Table table = declaredView("column_udt_usage");
        String catalog = catalogName();
        for (Relation rel : userRelations()) {
            for (Column col : rel.columns) {
                if (CatalogCoreBuilder.isSystemColumn(col)) continue;
                String udtSchema = "pg_catalog";
                String udtName;
                switch (col.getType()) {
                    case SERIAL: udtName = "int4"; break;
                    case BIGSERIAL: udtName = "int8"; break;
                    case SMALLSERIAL: udtName = "int2"; break;
                    default: udtName = col.getType().getPgName(); break;
                }
                if (col.getType() == DataType.ENUM && col.getEnumTypeName() != null) {
                    udtSchema = TypeNamespace.schemaOfKey(col.getEnumTypeName());
                    udtName = TypeNamespace.nameOfKey(col.getEnumTypeName());
                } else if (col.getCompositeTypeName() != null) {
                    udtSchema = TypeNamespace.schemaOfKey(col.getCompositeTypeName());
                    udtName = TypeNamespace.nameOfKey(col.getCompositeTypeName());
                }
                table.insertRow(new Object[]{catalog, udtSchema, udtName,
                        catalog, rel.schema, rel.name, col.getName()});
            }
        }
        return table;
    }

    /** information_schema.domain_udt_usage — the base type each domain is built on. */
    private Table buildIsDomainUdtUsage() {
        Table table = declaredView("domain_udt_usage");
        String catalog = catalogName();
        for (Map.Entry<String, DomainType> entry : database.getDomains().entrySet()) {
            DomainType d = entry.getValue();
            table.insertRow(new Object[]{catalog, "pg_catalog", d.getBaseType().getPgName(),
                    catalog, domainSchema(d), d.getName()});
        }
        return table;
    }

    /**
     * A relation a view's query names, under the name the query calls it by.
     */
    private static final class ViewSource {
        final String schema;
        final String name;
        final List<Column> columns;
        /** The names the query may qualify a column of this relation with: its alias, or its name. */
        final String key;

        ViewSource(String schema, String name, List<Column> columns, String key) {
            this.schema = schema;
            this.name = name;
            this.columns = columns;
            this.key = key;
        }

        boolean has(String column) {
            for (Column c : columns) {
                if (c.getName().equalsIgnoreCase(column)) return true;
            }
            return false;
        }
    }

    /** information_schema.view_table_usage — which relations each view's query names. */
    private Table buildIsViewTableUsage() {
        Table table = declaredView("view_table_usage");
        String catalog = catalogName();
        for (Database.ViewDef vd : database.getViews().values()) {
            if (vd.materialized()) continue;
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            Set<String> seen = new LinkedHashSet<String>();
            for (ViewSource src : viewSources(vd)) {
                if (!seen.add(src.schema + "." + src.name)) continue;
                table.insertRow(new Object[]{catalog, vSchema, vd.name(),
                        catalog, src.schema, src.name});
            }
        }
        return table;
    }

    /**
     * information_schema.view_column_usage — which columns of those relations the view reads.
     *
     * <p>PostgreSQL records these through the dependencies its rewriter leaves behind, so what is
     * listed is what the view's query <em>refers to</em> — not every column of every relation it
     * reads from. A tool that renames a column consults this view to find out which views it is
     * about to break, and one that lists the untouched columns of a joined table sends it
     * rebuilding views that never mentioned them.
     */
    private Table buildIsViewColumnUsage() {
        Table table = declaredView("view_column_usage");
        String catalog = catalogName();
        for (Database.ViewDef vd : database.getViews().values()) {
            if (vd.materialized()) continue;
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            for (String[] use : viewColumnUses(vd)) {
                table.insertRow(new Object[]{catalog, vSchema, vd.name(),
                        catalog, use[0], use[1], use[2]});
            }
        }
        return table;
    }

    /**
     * The relations a view's query names, in the order it names them. A subquery alias and a CTE
     * name are left out: they are names the query invents for itself, and a column read through
     * one is not a column of any relation.
     */
    private List<ViewSource> viewSources(Database.ViewDef vd) {
        final List<ViewSource> sources = new ArrayList<ViewSource>();
        if (vd.query() == null) return sources;
        final String viewSchema = vd.schemaName() != null ? vd.schemaName() : "public";
        final Set<String> invented = new LinkedHashSet<String>();
        AstWalk.forEach(vd.query(), new java.util.function.Consumer<Object>() {
            @Override public void accept(Object node) {
                if (node instanceof com.memgres.engine.parser.ast.SelectStmt.CommonTableExpr) {
                    String n = ((com.memgres.engine.parser.ast.SelectStmt.CommonTableExpr) node).name();
                    if (n != null) invented.add(n.toLowerCase());
                } else if (node instanceof com.memgres.engine.parser.ast.SelectStmt.SubqueryFrom) {
                    String a = ((com.memgres.engine.parser.ast.SelectStmt.SubqueryFrom) node).alias();
                    if (a != null) invented.add(a.toLowerCase());
                }
            }
        });
        AstWalk.forEach(vd.query(), new java.util.function.Consumer<Object>() {
            @Override public void accept(Object node) {
                if (!(node instanceof com.memgres.engine.parser.ast.SelectStmt.TableRef)) return;
                com.memgres.engine.parser.ast.SelectStmt.TableRef ref =
                        (com.memgres.engine.parser.ast.SelectStmt.TableRef) node;
                if (ref.table() == null) return;
                if (ref.schema() == null && invented.contains(ref.table().toLowerCase())) return;
                ViewSource src = resolveViewSource(ref.schema(), ref.table(), viewSchema,
                        ref.alias() != null ? ref.alias() : ref.table());
                if (src != null) sources.add(src);
            }
        });
        return sources;
    }

    /** The relation a name in a view's FROM clause denotes, or null when nothing answers to it. */
    private ViewSource resolveViewSource(String schema, String name, String viewSchema, String key) {
        if (schema != null) {
            return lookupViewSource(schema, name, key);
        }
        // No schema written: the view was resolved through a search path memgres does not keep,
        // so the schema it lives in and then public are the two that can plausibly hold it.
        ViewSource src = lookupViewSource(viewSchema, name, key);
        if (src != null) return src;
        if (!"public".equalsIgnoreCase(viewSchema)) {
            src = lookupViewSource("public", name, key);
            if (src != null) return src;
        }
        for (String schemaName : database.getSchemas().keySet()) {
            src = lookupViewSource(schemaName, name, key);
            if (src != null) return src;
        }
        return null;
    }

    /** The table or view of that name in that schema, described by its columns. */
    private ViewSource lookupViewSource(String schema, String name, String key) {
        Schema s = null;
        for (Map.Entry<String, Schema> e : database.getSchemas().entrySet()) {
            if (e.getKey().equalsIgnoreCase(schema)) { s = e.getValue(); break; }
        }
        if (s != null) {
            for (Map.Entry<String, Table> e : s.getTables().entrySet()) {
                if (e.getKey().equalsIgnoreCase(name)) {
                    return new ViewSource(s.getName(), e.getValue().getName(),
                            e.getValue().getColumns(), key);
                }
            }
        }
        for (Database.ViewDef other : database.getViews().values()) {
            String otherSchema = other.schemaName() != null ? other.schemaName() : "public";
            if (!otherSchema.equalsIgnoreCase(schema) || !other.name().equalsIgnoreCase(name)) continue;
            List<Column> cols = other.cachedColumns() != null ? other.cachedColumns()
                    : Collections.<Column>emptyList();
            return new ViewSource(otherSchema, other.name(), cols, key);
        }
        return null;
    }

    /** The (schema, table, column) triples a view's query refers to, without repeats. */
    private List<String[]> viewColumnUses(Database.ViewDef vd) {
        final List<ViewSource> sources = viewSources(vd);
        final Set<String> emitted = new LinkedHashSet<String>();
        final List<String[]> out = new ArrayList<String[]>();
        if (sources.isEmpty()) return out;
        final Set<String> starred = new LinkedHashSet<String>();
        final List<Object[]> refs = new ArrayList<Object[]>();  // {qualifier, column}
        AstWalk.forEach(vd.query(), new java.util.function.Consumer<Object>() {
            @Override public void accept(Object node) {
                if (node instanceof com.memgres.engine.parser.ast.ColumnRef) {
                    com.memgres.engine.parser.ast.ColumnRef cr =
                            (com.memgres.engine.parser.ast.ColumnRef) node;
                    if (cr.column() != null) refs.add(new Object[]{cr.table(), cr.column()});
                } else if (node instanceof com.memgres.engine.parser.ast.WildcardExpr) {
                    com.memgres.engine.parser.ast.WildcardExpr w =
                            (com.memgres.engine.parser.ast.WildcardExpr) node;
                    starred.add(w.table() == null ? "*" : w.table().toLowerCase());
                } else if (node instanceof com.memgres.engine.parser.ast.SelectStmt.JoinFrom) {
                    // USING (a) reads a from both sides even though neither is written down
                    List<String> using =
                            ((com.memgres.engine.parser.ast.SelectStmt.JoinFrom) node).using();
                    if (using != null) {
                        for (String u : using) refs.add(new Object[]{null, u});
                    }
                }
            }
        });
        // A star stands for every column of the relations it covers.
        for (ViewSource src : sources) {
            if (starred.contains("*") || starred.contains(src.key.toLowerCase())
                    || starred.contains(src.name.toLowerCase())) {
                for (Column c : src.columns) {
                    addColumnUse(out, emitted, src, c.getName());
                }
            }
        }
        for (Object[] ref : refs) {
            String qualifier = (String) ref[0];
            String column = (String) ref[1];
            if (qualifier != null) {
                ViewSource match = null;
                for (ViewSource src : sources) {
                    if (src.key.equalsIgnoreCase(qualifier)) { match = src; break; }
                }
                if (match == null) {
                    for (ViewSource src : sources) {
                        if (src.name.equalsIgnoreCase(qualifier)) { match = src; break; }
                    }
                }
                if (match != null && match.has(column)) addColumnUse(out, emitted, match, column);
                continue;
            }
            // Unqualified: the relation that has such a column. A name that two of them carry
            // would have made the view's own query ambiguous unless it was joined USING it, and
            // then both relations really are read.
            int carriers = 0;
            for (ViewSource src : sources) if (src.has(column)) carriers++;
            for (ViewSource src : sources) {
                if (!src.has(column)) continue;
                addColumnUse(out, emitted, src, column);
                if (carriers == 1) break;
            }
        }
        return out;
    }

    private void addColumnUse(List<String[]> out, Set<String> emitted, ViewSource src, String column) {
        String actual = column;
        for (Column c : src.columns) {
            if (c.getName().equalsIgnoreCase(column)) { actual = c.getName(); break; }
        }
        if (emitted.add(src.schema + "." + src.name + "." + actual.toLowerCase())) {
            out.add(new String[]{src.schema, src.name, actual});
        }
    }

    /**
     * information_schema.domain_constraints — the CHECK constraints a domain carries. The clause
     * itself is in check_constraints; this view is what names the domain they belong to.
     */
    private Table buildIsDomainConstraints() {
        Table table = declaredView("domain_constraints");
        String catalog = catalogName();
        for (Map.Entry<String, DomainType> entry : database.getDomains().entrySet()) {
            DomainType d = entry.getValue();
            String schema = domainSchema(d);
            if (d.getParsedCheck() != null) {
                table.insertRow(new Object[]{catalog, schema, d.getName() + "_check",
                        catalog, schema, d.getName(), "NO", "NO"});
            }
            for (DomainType.NamedConstraint nc : d.getNamedConstraints()) {
                table.insertRow(new Object[]{catalog, schema, nc.name(),
                        catalog, schema, d.getName(), "NO", "NO"});
            }
        }
        return table;
    }

    /**
     * information_schema.triggered_update_columns — the column list of an UPDATE OF trigger.
     * Without it, a trigger that fires on one column looks like one that fires on every update.
     */
    private Table buildIsTriggeredUpdateColumns() {
        Table table = declaredView("triggered_update_columns");
        String catalog = catalogName();
        for (Map.Entry<String, List<PgTrigger>> entry : database.getAllTriggers().entrySet()) {
            for (PgTrigger trig : entry.getValue()) {
                if (trig.getEvent() != PgTrigger.Event.UPDATE) continue;
                List<String> updateColumns = trig.getUpdateColumns();
                if (updateColumns == null || updateColumns.isEmpty()) continue;
                String schema = trig.getSchemaName() != null ? trig.getSchemaName() : "public";
                for (String col : updateColumns) {
                    table.insertRow(new Object[]{catalog, schema, trig.getName(),
                            catalog, schema, trig.getTableName(), col});
                }
            }
        }
        return table;
    }

    /** A view whose content PostgreSQL ships as a constant table. */
    private Table constantView(String name, Object[][] rows) {
        Table table = declaredView(name);
        for (Object[] row : rows) table.insertRow(row.clone());
        return table;
    }

    /** information_schema.sql_parts — the parts of the SQL standard, as PG reports them. */
    private static final Object[][] SQL_PARTS = {
        {"1", "Framework (SQL/Framework)", "NO", null, ""},
        {"2", "Foundation (SQL/Foundation)", "NO", null, ""},
        {"3", "Call-Level Interface (SQL/CLI)", "NO", null, ""},
        {"4", "Persistent Stored Modules (SQL/PSM)", "NO", null, ""},
        {"9", "Management of External Data (SQL/MED)", "NO", null, ""},
        {"10", "Object Language Bindings (SQL/OLB)", "NO", null, ""},
        {"11", "Information and Definition Schema (SQL/Schemata)", "NO", null, ""},
        {"13", "Routines and Types Using the Java Programming Language (SQL/JRT)", "NO", null, ""},
        {"14", "XML-Related Specifications (SQL/XML)", "NO", null, ""},
        {"15", "Multi-Dimensional Arrays (SQL/MDA)", "NO", null, ""},
        {"16", "Property Graph Queries (SQL/PGQ)", "NO", null, ""},
    };

    private static final String SIZING_CHARSET_NOTE = "Might be less, depending on character set.";

    /** information_schema.sql_sizing — the implementation limits PostgreSQL publishes. */
    private static final Object[][] SQL_SIZING = {
        {Integer.valueOf(0), "MAXIMUM DRIVER CONNECTIONS", null, null},
        {Integer.valueOf(1), "MAXIMUM CONCURRENT ACTIVITIES", Integer.valueOf(0), null},
        {Integer.valueOf(30), "MAXIMUM COLUMN NAME LENGTH", Integer.valueOf(63), SIZING_CHARSET_NOTE},
        {Integer.valueOf(31), "MAXIMUM CURSOR NAME LENGTH", Integer.valueOf(63), SIZING_CHARSET_NOTE},
        {Integer.valueOf(32), "MAXIMUM SCHEMA NAME LENGTH", Integer.valueOf(63), SIZING_CHARSET_NOTE},
        {Integer.valueOf(34), "MAXIMUM CATALOG NAME LENGTH", Integer.valueOf(63), SIZING_CHARSET_NOTE},
        {Integer.valueOf(35), "MAXIMUM TABLE NAME LENGTH", Integer.valueOf(63), SIZING_CHARSET_NOTE},
        {Integer.valueOf(97), "MAXIMUM COLUMNS IN GROUP BY", Integer.valueOf(0), null},
        {Integer.valueOf(99), "MAXIMUM COLUMNS IN ORDER BY", Integer.valueOf(0), null},
        {Integer.valueOf(100), "MAXIMUM COLUMNS IN SELECT", Integer.valueOf(1664), null},
        {Integer.valueOf(101), "MAXIMUM COLUMNS IN TABLE", Integer.valueOf(1600), null},
        {Integer.valueOf(106), "MAXIMUM TABLES IN SELECT", Integer.valueOf(0), null},
        {Integer.valueOf(107), "MAXIMUM USER NAME LENGTH", Integer.valueOf(63), SIZING_CHARSET_NOTE},
        {Integer.valueOf(10005), "MAXIMUM IDENTIFIER LENGTH", Integer.valueOf(63), SIZING_CHARSET_NOTE},
        {Integer.valueOf(20000), "MAXIMUM STATEMENT OCTETS", Integer.valueOf(0), null},
        {Integer.valueOf(20001), "MAXIMUM STATEMENT OCTETS DATA", Integer.valueOf(0), null},
        {Integer.valueOf(20002), "MAXIMUM STATEMENT OCTETS SCHEMA", Integer.valueOf(0), null},
        {Integer.valueOf(25000), "MAXIMUM CURRENT DEFAULT TRANSFORM GROUP LENGTH", null, null},
        {Integer.valueOf(25001), "MAXIMUM CURRENT TRANSFORM GROUP LENGTH", null, null},
        {Integer.valueOf(25002), "MAXIMUM CURRENT PATH LENGTH", Integer.valueOf(0), null},
        {Integer.valueOf(25003), "MAXIMUM CURRENT ROLE LENGTH", null, null},
        {Integer.valueOf(25004), "MAXIMUM SESSION USER LENGTH", Integer.valueOf(63), SIZING_CHARSET_NOTE},
        {Integer.valueOf(25005), "MAXIMUM SYSTEM USER LENGTH", Integer.valueOf(63), SIZING_CHARSET_NOTE},
    };

    /** information_schema.sql_implementation_info — what an ODBC-style client asks about first. */
    private static final Object[][] SQL_IMPLEMENTATION_INFO = {
        {"10003", "CATALOG NAME", null, "Y", null},
        {"10004", "COLLATING SEQUENCE", null, null, null},
        {"13", "SERVER NAME", null, "", null},
        {"17", "DBMS NAME", null, "PostgreSQL", null},
        {"18", "DBMS VERSION", null, "18.00.0000", null},
        {"2", "DATA SOURCE NAME", null, "", null},
        {"23", "CURSOR COMMIT BEHAVIOR", Integer.valueOf(1), null,
                "close cursors and retain prepared statements"},
        {"26", "DEFAULT TRANSACTION ISOLATION", Integer.valueOf(2), null,
                "READ COMMITTED; user-settable"},
        {"28", "IDENTIFIER CASE", Integer.valueOf(3), null, "stored in mixed case - case sensitive"},
        {"46", "TRANSACTION CAPABLE", Integer.valueOf(2), null, "both DML and DDL"},
        {"85", "NULL COLLATION", Integer.valueOf(0), null, "nulls higher than non-nulls"},
        {"94", "SPECIAL CHARACTERS", null, "", "all non-ASCII characters allowed"},
    };

    // =====================================================================================
    // Small shared lookups
    // =====================================================================================

    /**
     * A parameter default the way PG's deparser writes it. A string literal carries the cast that
     * gives it its type — {@code 'x'::text} — because the same three characters mean something
     * different at every character type, and a client that replays the default has to know which.
     */
    private static String parameterDefault(String written, DataType dt) {
        if (written == null || dt == null) return written;
        boolean isText = dt == DataType.TEXT || dt == DataType.VARCHAR
                || dt == DataType.CHAR || dt == DataType.NAME;
        if (!isText) return written;
        String s = written.trim();
        if (s.length() < 2 || s.charAt(0) != '\'' || s.charAt(s.length() - 1) != '\'') return written;
        return s + "::" + CatalogHelper.pgTypeName(dt);
    }

    /** The schema a domain was created in. */
    private static String domainSchema(DomainType d) {
        return d.getSchemaName() != null ? d.getSchemaName() : "public";
    }

    /** The schema a composite type was created in, as the schema object registry recorded it. */
    private String compositeTypeSchema(String typeKey) {
        return TypeNamespace.schemaOfKey(typeKey);
    }

    /** The bare name of the type a namespace key stands for. */
    private static String typeNameOf(String typeKey) {
        return TypeNamespace.nameOfKey(typeKey);
    }

    /**
     * The schema a user-defined type lives in — which is not the schema of whatever is declared as
     * it. {@code udt_schema} names the type, so a table in one schema whose column is of a type in
     * another has to report the type's, and a name no type answers to falls back to the caller's.
     */
    private String userTypeSchema(String typeName, String fallback) {
        if (typeName == null) return fallback;
        String owner = TypeNamespace.schemaOf(database, typeName);
        return owner != null ? owner : fallback;
    }

    /** A trigger function written the way PG writes it in triggers.action_statement. */
    private String qualifiedFunctionName(String functionName, String fallbackSchema) {
        if (functionName == null || functionName.indexOf('.') >= 0) return functionName;
        for (PgFunction fn : database.getFunctions().values()) {
            if (fn.getName().equalsIgnoreCase(functionName)) {
                String schema = fn.getSchemaName() != null ? fn.getSchemaName() : "public";
                return schema + "." + functionName;
            }
        }
        return fallbackSchema + "." + functionName;
    }

    /** The largest value an identity column of this type can reach. */
    private static long identityUpperBound(DataType dt) {
        switch (dt) {
            case SMALLINT: case SMALLSERIAL: return 32767L;
            case BIGINT: case BIGSERIAL: return 9223372036854775807L;
            default: return 2147483647L;
        }
    }

    /** A type as it was written down: the type itself, plus the typmod it was declared with. */
    static final class TypeSpec {
        DataType type;
        String baseName;
        Integer precision;
        Integer scale;
        boolean isArray;
    }

    /**
     * Read a written type name — "varchar(3)", "numeric(8,2)", "text[]" — back into the type and
     * the typmod it carries. Composite attributes and routine results are stored as the text the
     * user wrote, and the SQL-standard views have to describe them the same way a column is.
     */
    static TypeSpec parseTypeSpec(String written) {
        TypeSpec spec = new TypeSpec();
        if (written == null) return spec;
        String s = written.trim();
        while (s.endsWith("[]")) {
            spec.isArray = true;
            s = s.substring(0, s.length() - 2).trim();
        }
        int lp = s.indexOf('(');
        if (lp >= 0 && s.endsWith(")")) {
            String args = s.substring(lp + 1, s.length() - 1);
            s = s.substring(0, lp).trim();
            String[] parts = args.split(",");
            try {
                spec.precision = Integer.valueOf(parts[0].trim());
            } catch (NumberFormatException ignored) {
                // A qualifier that is not a number, such as interval's field list
            }
            if (parts.length > 1) {
                try {
                    spec.scale = Integer.valueOf(parts[1].trim());
                } catch (NumberFormatException ignored) {
                    // as above
                }
            }
        }
        spec.baseName = s;
        try {
            spec.type = DataType.fromPgName(s);
        } catch (RuntimeException notABuiltin) {
            // A user-defined type: the caller reports it as USER-DEFINED under its own name.
        }
        return spec;
    }
}
