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
        "view_routine_usage", "view_table_usage", "views"
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
            case "role_table_grants":
                return buildIsRoleTableGrants();
            case "information_schema_catalog_name": {
                Table t = declaredView(tableName);
                t.insertRow(new Object[]{catalogName()});
                return t;
            }
            case "character_sets": {
                Table t = declaredView(tableName);
                // PG reports the database encoding as a single anonymous character set
                t.insertRow(new Object[]{null, null, "UTF8", "UCS", "UTF8",
                        catalogName(), "pg_catalog", "en_US.UTF-8"});
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
            // "name:type" — the information_schema types are name, character varying and integer
            int colon = spec.indexOf(':');
            String name = colon < 0 ? spec : spec.substring(0, colon);
            DataType type = colon < 0 ? DataType.NAME
                    : "i".equals(spec.substring(colon + 1)) ? DataType.INTEGER : DataType.VARCHAR;
            cols.add(new Column(name, type, true, false, null));
        }
        return new Table(tableName, cols);
    }

    /**
     * The columns PostgreSQL declares for the information_schema views memgres lists but does not
     * yet populate. Unsuffixed names are {@code name}, ":v" is {@code character varying} and
     * ":i" is {@code integer} — the only three types these views use.
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
                "oid", "fdwowner", "fdwoptions", "foreign_data_wrapper_catalog",
                "foreign_data_wrapper_name", "authorization_identifier",
                "foreign_data_wrapper_language:v"});
        m.put("_pg_foreign_servers", new String[]{
                "oid", "srvoptions", "foreign_server_catalog", "foreign_server_name",
                "foreign_data_wrapper_catalog", "foreign_data_wrapper_name",
                "foreign_server_type:v", "foreign_server_version:v", "authorization_identifier"});
        m.put("_pg_foreign_table_columns", new String[]{
                "nspname", "relname", "attname", "attfdwoptions"});
        m.put("_pg_foreign_tables", new String[]{
                "foreign_table_catalog", "foreign_table_schema", "foreign_table_name",
                "ftoptions", "foreign_server_catalog", "foreign_server_name",
                "authorization_identifier"});
        m.put("_pg_user_mappings", new String[]{
                "oid", "umoptions", "umuser", "authorization_identifier",
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
                    Table view = "columns".equals(isView) ? table : build(isView, currentSession);
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
            charMaxLen = (dt == DataType.VARCHAR || dt == DataType.CHAR) ? typmodPrecision : null;
            if (charMaxLen != null) charOctetLen = charMaxLen * 4;
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
                if (dt == DataType.ENUM && col.getEnumTypeName() != null) {
                    udtSchema = schemaName;
                    udtName = col.getEnumTypeName();
                } else if (col.getCompositeTypeName() != null) {
                    // H14: composite column — udt_name is the composite type name
                    udtSchema = schemaName;
                    udtName = col.getCompositeTypeName();
                }
                // H14: DOMAIN columns keep the BASE type udt_name (e.g. int4);
                // the domain identity is carried by the domain_* fields below.
            }

            // H14: is_identity — detect __identity__ marker in default value
            String defaultVal = col.getDefaultValue();
            boolean isIdentity = isUserTable && defaultVal != null && defaultVal.startsWith("__identity__");
            String identityGeneration = null;
            if (isIdentity) {
                identityGeneration = defaultVal.contains(":always:") ? "ALWAYS" : "BY DEFAULT";
            }

            // H14: domain_* fields
            String domainCatalog = null, domainSchema = null, domainName = null;
            if (isUserTable && col.getDomainTypeName() != null) {
                domainCatalog = catalogName();
                // The domain lives where it was created, which need not be where the table is.
                DomainType colDomain = database.getDomain(col.getDomainTypeName());
                domainSchema = colDomain != null ? colDomain.getSchemaName() : schemaName;
                domainName = col.getDomainTypeName();
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
                    null, null, null, null, null,           // identity_start..identity_cycle
                    isUserTable && col.isGenerated() ? "ALWAYS" : "NEVER", // is_generated
                    isUserTable ? col.getGeneratedExpr() : null, // generation_expression
                    "YES"                                   // is_updatable
            });
        }
        for (Object[] row : pending) isTable.insertRow(row);
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
            table.insertRow(new Object[]{catalogName(), schemaName, "memgres", null, null, null, null});
        }
        table.insertRow(new Object[]{catalogName(), "pg_catalog", "memgres", null, null, null, null});
        table.insertRow(new Object[]{catalogName(), "information_schema", "memgres", null, null, null, null});
        table.insertRow(new Object[]{catalogName(), "pg_toast", "memgres", null, null, null, null});
        return table;
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
                            type, "NO", "NO", sc.isNotEnforced() ? "NO" : "YES", nullsDistinct
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
            String routineBody = "EXTERNAL"; // PG uses EXTERNAL for non-SQL
            String routineDefinition = fn.getBody();

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
                    returnType,          // data_type
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
                    returnType,          // type_udt_name
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
                    fn.isProcedure() ? null : "YES",  // is_null_call
                    null,                // sql_path
                    "YES",               // schema_level_routine
                    0,                   // max_dynamic_result_sets
                    "NO",                // is_user_defined_cast
                    "NO",                // is_implicitly_invocable
                    "INVOKER",           // security_type
                    null,                // to_sql_specific_catalog
                    null,                // to_sql_specific_schema
                    null,                // to_sql_specific_name
                    "NO",                // as_locator
                    null,                // created
                    null,                // last_altered
                    "YES",               // new_savepoint_level
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
                        identitySeqs.add(def.substring(def.indexOf(":seq:") + 5));
                    }
                }
            }
        }
        for (String seqName : CatalogHelper.getSequenceNames(database)) {
            if (identitySeqs.contains(seqName)) continue; // M14: exclude identity sequences
            Sequence seq = database.getSequence(seqName);
            if (seq != null) {
                String dataType = seq.getDataType() != null ? seq.getDataType() : "bigint";
                int precision = "smallint".equals(dataType) ? 16 : "integer".equals(dataType) ? 32 : 64;
                String cycleOption = seq.isCycle() ? "YES" : "NO";
                table.insertRow(new Object[]{
                        catalogName(), sequenceSchema(seqName), seqName, dataType, precision, 2, 0,
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

    /** The schema a sequence was created in, as the schema object registry recorded it. */
    private String sequenceSchema(String seqName) {
        for (String schemaName : database.getSchemas().keySet()) {
            if (database.getSchemaObjects(schemaName).contains("sequence:" + seqName.toLowerCase())) {
                return schemaName;
            }
        }
        return "public";
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
            String isUpdatable = isSimpleUpdatableView(vd) ? "YES" : "NO";
            table.insertRow(new Object[]{
                    catalogName(), vSchema, vd.name(), viewDef, "NONE", isUpdatable, isUpdatable, "NO", "NO", "NO"
            });
        }
        return table;
    }

    /**
     * Determine if a view is a simple updatable view (single-table, no aggregates/grouping/distinct/unions).
     */
    private boolean isSimpleUpdatableView(Database.ViewDef vd) {
        if (vd.materialized()) return false;
        if (!(vd.query() instanceof com.memgres.engine.parser.ast.SelectStmt)) return false;
        com.memgres.engine.parser.ast.SelectStmt sel = (com.memgres.engine.parser.ast.SelectStmt) vd.query();
        if (sel.distinct) return false;
        if (sel.groupBy != null && !sel.groupBy.isEmpty()) return false;
        if (sel.having != null) return false;
        if (sel.from == null || sel.from.size() != 1) return false;
        // Must be a simple table reference (no subquery, no join)
        com.memgres.engine.parser.ast.SelectStmt.FromItem from = sel.from.get(0);
        return from instanceof com.memgres.engine.parser.ast.SelectStmt.TableRef;
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
                    catalogName(), d.getSchemaName(), entry.getKey(),
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
            if (d.getParsedCheck() != null) {
                table.insertRow(new Object[]{
                        catalogName(), "public", d.getName() + "_check",
                        "(" + stripOuterParensLocal(CatalogHelper.renderDomainCheck(d, d.getParsedCheck())) + ")"
                });
            }
            for (DomainType.NamedConstraint nc : d.getNamedConstraints()) {
                table.insertRow(new Object[]{
                        catalogName(), "public", nc.name(),
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
                    if (sc.getName() != null) {
                        table.insertRow(new Object[]{
                                catalogName(), schemaEntry.getKey(), tableEntry.getKey(),
                                catalogName(), schemaEntry.getKey(), sc.getName()
                        });
                    }
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
                        p.defaultExpr()                                 // parameter_default
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
            String actionStmt = "EXECUTE FUNCTION " + first.getFunctionName() + "()";
            // Emit one row per event manipulation (PG standard)
            for (PgTrigger trig : entry.getValue()) {
                String event = trig.getEvent().name(); // INSERT, UPDATE, DELETE, TRUNCATE
                table.insertRow(new Object[]{
                        catalogName(),          // trigger_catalog
                        trigSchema,             // trigger_schema
                        first.getName(),        // trigger_name
                        event,                  // event_manipulation
                        catalogName(),          // event_object_catalog
                        trigSchema,             // event_object_schema
                        first.getTableName(),   // event_object_table
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

    private Table buildIsRoleTableGrants() {
        List<Column> cols = Cols.listOf(
                new Column("grantor", DataType.TEXT, true, false, null),
                new Column("grantee", DataType.TEXT, true, false, null),
                new Column("table_catalog", DataType.TEXT, true, false, null),
                new Column("table_schema", DataType.TEXT, true, false, null),
                new Column("table_name", DataType.TEXT, true, false, null),
                new Column("privilege_type", DataType.TEXT, true, false, null),
                new Column("is_grantable", DataType.TEXT, true, false, null),
                new Column("with_hierarchy", DataType.TEXT, true, false, null)
        );
        Table table = new Table("role_table_grants", cols);
        String catalog = catalogName();
        // Iterate all role privileges and extract TABLE grants
        for (Map.Entry<String, java.util.Set<String>> entry : database.getAllRolePrivileges().entrySet()) {
            String grantee = entry.getKey();
            for (String privEntry : entry.getValue()) {
                // Format: "privilege:objectType:objectName"
                String[] parts = privEntry.split(":", 3);
                if (parts.length == 3 && "TABLE".equalsIgnoreCase(parts[1])) {
                    String privilege = parts[0];
                    String objectName = parts[2];
                    // Determine schema — objectName might be schema-qualified
                    String schema = "public";
                    String tableName = objectName;
                    if (objectName.contains(".")) {
                        int dot = objectName.indexOf('.');
                        schema = objectName.substring(0, dot);
                        tableName = objectName.substring(dot + 1);
                    }
                    String grantor = currentSession != null && currentSession.getConnectingUser() != null
                            ? currentSession.getConnectingUser() : "memgres";
                    table.insertRow(new Object[]{grantor, grantee, catalog, schema, tableName,
                            privilege, "NO", "NO"});
                }
            }
        }
        return table;
    }
}
