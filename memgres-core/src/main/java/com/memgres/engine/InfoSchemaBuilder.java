package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.*;

/**
 * Builds information_schema virtual tables from the current database metadata.
 * Extracted from SystemCatalog to keep that class focused on pg_catalog tables.
 */
public class InfoSchemaBuilder {

    /** pg_catalog tables whose columns should be listed in information_schema.columns. */
    private static final List<String> PG_CATALOG_TABLES_FOR_IS = Cols.listOf(
        "pg_stat_user_tables", "pg_stat_all_tables",
        "pg_stat_user_indexes", "pg_stat_all_indexes",
        "pg_stat_database", "pg_statio_user_tables",
        "pg_stat_activity", "pg_stat_replication",
        "pg_am", "pg_prepared_statements", "pg_cursors",
        "pg_class", "pg_attribute", "pg_namespace", "pg_type",
        "pg_proc", "pg_index", "pg_description",
        "pg_constraint", "pg_depend", "pg_roles", "pg_collation",
        "pg_database", "pg_tablespace", "pg_settings",
        "pg_extension", "pg_enum", "pg_operator",
        "pg_aggregate", "pg_trigger", "pg_rewrite",
        "pg_event_trigger",
        "pg_stat_bgwriter", "pg_stat_wal",
        "pg_replication_slots",
        "pg_seclabel", "pg_shseclabel",
        "pg_policies",
        "pg_authid"
    );

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
            default:
                return CatalogHelper.emptyTable(tableName);
        }
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
                new Column("is_typed", DataType.TEXT, true, false, null)
        );
        Table table = new Table("tables", cols);

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (String tableName : schemaEntry.getValue().getTables().keySet()) {
                table.insertRow(new Object[]{
                        catalogName(), schemaEntry.getKey(), tableName, "BASE TABLE",
                        null, null, null, null, null, "YES", "NO"
                });
            }
        }

        for (Database.ViewDef vd : database.getViews().values()) {
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            table.insertRow(new Object[]{
                    catalogName(), vSchema, vd.name(), "VIEW",
                    null, null, null, null, null, "NO", "NO"
            });
        }
        return table;
    }

    private Table buildIsColumns() {
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

        // Also add pg_catalog virtual table columns so that queries like
        // SELECT ... FROM information_schema.columns WHERE table_schema = 'pg_catalog' work.
        PgCatalogBuilder pgBuilder = new PgCatalogBuilder(database, oids);
        for (String pgTable : PG_CATALOG_TABLES_FOR_IS) {
            try {
                Table pgCatTable = pgBuilder.build(pgTable);
                if (pgCatTable != null && !pgCatTable.getColumns().isEmpty()) {
                    addColumnsForTable(table, "pg_catalog", pgCatTable, false);
                }
            } catch (Exception ignored) {
                // Skip tables that can't be built without session context
            }
        }

        return table;
    }

    /** Add column entries for a table to the information_schema.columns table. */
    private void addColumnsForTable(Table isTable, String schemaName, Table t, boolean isUserTable) {
        for (int i = 0; i < t.getColumns().size(); i++) {
            Column col = t.getColumns().get(i);
            DataType dt = col.getType();
            String dataType = CatalogHelper.pgTypeName(dt);
            boolean isCharType = dt == DataType.VARCHAR || dt == DataType.CHAR || dt == DataType.TEXT || dt == DataType.NAME;
            boolean isNumericType = dt == DataType.SMALLINT || dt == DataType.INTEGER || dt == DataType.BIGINT
                    || dt == DataType.SERIAL || dt == DataType.BIGSERIAL || dt == DataType.SMALLSERIAL
                    || dt == DataType.REAL || dt == DataType.DOUBLE_PRECISION || dt == DataType.NUMERIC;
            boolean isDateTimeType = dt == DataType.TIMESTAMP || dt == DataType.TIMESTAMPTZ
                    || dt == DataType.TIME || dt == DataType.DATE || dt == DataType.INTERVAL;

            Integer charMaxLen = null;
            Integer charOctetLen = isCharType ? Integer.valueOf(1073741824) : null;
            Integer numPrec = CatalogHelper.numericPrecision(dt);
            // For NUMERIC columns, use the column's declared precision/scale if available
            if (dt == DataType.NUMERIC && col.getPrecision() != null) {
                numPrec = col.getPrecision();
            }
            Integer numPrecRadix = numPrec != null ? Integer.valueOf(dt == DataType.NUMERIC ? 10 : 2) : null;
            Integer numScale;
            if (dt == DataType.NUMERIC) {
                // Only report scale when precision is declared (plain numeric has no limit)
                numScale = col.getPrecision() != null
                        ? (col.getScale() != null ? col.getScale() : Integer.valueOf(0))
                        : null;
            } else {
                numScale = numPrec != null && dt != DataType.REAL && dt != DataType.DOUBLE_PRECISION ? Integer.valueOf(0) : null;
            }
            Integer datetimePrec = isDateTimeType ? Integer.valueOf(6) : null;
            if (dt == DataType.DATE) datetimePrec = Integer.valueOf(0);
            Integer intervalPrec = dt == DataType.INTERVAL ? Integer.valueOf(6) : null;

            String udtSchema = "pg_catalog";
            String udtName = col.getType().getPgName();
            if (isUserTable) {
                udtSchema = isCharType || isNumericType || isDateTimeType || dt == DataType.BOOLEAN
                        || dt == DataType.BYTEA || dt == DataType.UUID || dt == DataType.JSON || dt == DataType.JSONB
                        || dt == DataType.XML ? "pg_catalog" : (dt == DataType.ENUM ? schemaName : "pg_catalog");
                if (dt == DataType.ENUM && col.getEnumTypeName() != null) {
                    udtName = col.getEnumTypeName();
                }
            }

            isTable.insertRow(new Object[]{
                    catalogName(),                           // table_catalog
                    schemaName,                             // table_schema
                    t.getName(),                            // table_name
                    col.getName(),                          // column_name
                    i + 1,                                  // ordinal_position
                    isUserTable ? CatalogHelper.formatColumnDefault(col) : null, // column_default
                    col.isNullable() ? "YES" : "NO",       // is_nullable
                    dataType,                               // data_type
                    charMaxLen,                             // character_maximum_length
                    charOctetLen,                           // character_octet_length
                    numPrec,                                // numeric_precision
                    numPrecRadix,                           // numeric_precision_radix
                    numScale,                               // numeric_scale
                    datetimePrec,                           // datetime_precision
                    null,                                   // interval_type
                    intervalPrec,                           // interval_precision
                    null, null, null,                       // character_set_*
                    null, null, null,                       // collation_*
                    null, null, null,                       // domain_*
                    catalogName(),                           // udt_catalog
                    udtSchema,                              // udt_schema
                    udtName,                                // udt_name
                    null, null, null,                       // scope_*
                    null,                                   // maximum_cardinality
                    String.valueOf(i + 1),                  // dtd_identifier
                    "NO",                                   // is_self_referencing
                    "NO",                                   // is_identity
                    null, null, null, null, null, null,     // identity_*
                    isUserTable && col.isGenerated() ? "ALWAYS" : "NEVER", // is_generated
                    isUserTable ? col.getGeneratedExpr() : null, // generation_expression
                    "YES"                                   // is_updatable
            });
        }
    }

    private Table buildIsSchemata() {
        List<Column> cols = Cols.listOf(
                new Column("catalog_name", DataType.TEXT, true, false, null),
                new Column("schema_name", DataType.TEXT, true, false, null),
                new Column("schema_owner", DataType.TEXT, true, false, null),
                new Column("default_character_set_catalog", DataType.TEXT, true, false, null),
                new Column("default_character_set_schema", DataType.TEXT, true, false, null),
                new Column("default_character_set_name", DataType.TEXT, true, false, null)
        );
        Table table = new Table("schemata", cols);
        for (String schemaName : database.getSchemas().keySet()) {
            table.insertRow(new Object[]{catalogName(), schemaName, "memgres", null, null, null});
        }
        table.insertRow(new Object[]{catalogName(), "pg_catalog", "memgres", null, null, null});
        table.insertRow(new Object[]{catalogName(), "information_schema", "memgres", null, null, null});
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
                new Column("enforced", DataType.TEXT, true, false, null)
        );
        Table table = new Table("table_constraints", cols);

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                for (StoredConstraint sc : t.getConstraints()) {
                    // UNIQUE constraints from CREATE UNIQUE INDEX (not ADD CONSTRAINT) are not in information_schema.table_constraints
                    if (sc.getType() == StoredConstraint.Type.UNIQUE && sc.isFromIndex()) continue;
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
                    table.insertRow(new Object[]{
                            catalogName(), schemaEntry.getKey(), sc.getName(),
                            catalogName(), schemaEntry.getKey(), t.getName(),
                            type, "NO", "NO", sc.isNotEnforced() ? "NO" : "YES"
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
                        String conname = t.getName() + "_" + col.getName() + "_not_null";
                        table.insertRow(new Object[]{
                                catalogName(), schemaEntry.getKey(), conname,
                                catalogName(), schemaEntry.getKey(), t.getName(),
                                "CHECK", "NO", "NO", "YES"
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
                    language,            // external_language
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
                new Column("start_value", DataType.TEXT, true, false, null),
                new Column("minimum_value", DataType.TEXT, true, false, null),
                new Column("maximum_value", DataType.TEXT, true, false, null),
                new Column("increment", DataType.TEXT, true, false, null),
                new Column("cycle_option", DataType.TEXT, true, false, null)
        );
        Table table = new Table("sequences", cols);
        for (String seqName : CatalogHelper.getSequenceNames(database)) {
            Sequence seq = database.getSequence(seqName);
            if (seq != null) {
                String dataType = seq.getDataType() != null ? seq.getDataType() : "bigint";
                int precision = "smallint".equals(dataType) ? 16 : "integer".equals(dataType) ? 32 : 64;
                String cycleOption = seq.isCycle() ? "YES" : "NO";
                table.insertRow(new Object[]{
                        catalogName(), "public", seqName, dataType, precision,
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
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            String viewDef = "";
            if (vd.query() != null) {
                viewDef = vd.sourceSQL() != null ? vd.sourceSQL() : SqlUnparser.toSql(vd.query());
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
        List<Column> cols = Cols.listOf(
                new Column("domain_catalog", DataType.TEXT, true, false, null),
                new Column("domain_schema", DataType.TEXT, true, false, null),
                new Column("domain_name", DataType.TEXT, true, false, null),
                new Column("data_type", DataType.TEXT, true, false, null),
                new Column("character_maximum_length", DataType.INTEGER, true, false, null),
                new Column("numeric_precision", DataType.INTEGER, true, false, null),
                new Column("domain_default", DataType.TEXT, true, false, null)
        );
        Table table = new Table("domains", cols);
        for (Map.Entry<String, DomainType> entry : database.getDomains().entrySet()) {
            DomainType d = entry.getValue();
            table.insertRow(new Object[]{
                    catalogName(), "public", entry.getKey(),
                    CatalogHelper.pgTypeName(d.getBaseType()), null, null, null
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
                for (StoredConstraint sc : tableEntry.getValue().getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.CHECK && sc.getName() != null) {
                        table.insertRow(new Object[]{
                                catalogName(), schemaEntry.getKey(), sc.getName(),
                                sc.getCheckExpr() != null ? sc.getCheckExpr().toString() : ""
                        });
                    }
                }
            }
        }
        return table;
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
                    if (sc.getName() != null && sc.getColumns() != null) {
                        for (String col : sc.getColumns()) {
                            table.insertRow(new Object[]{
                                    catalogName(), schemaEntry.getKey(), tableEntry.getKey(), col,
                                    catalogName(), schemaEntry.getKey(), sc.getName()
                            });
                        }
                    }
                }
            }
        }
        return table;
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
                new Column("specific_catalog", DataType.TEXT, true, false, null),
                new Column("specific_schema", DataType.TEXT, true, false, null),
                new Column("specific_name", DataType.TEXT, true, false, null),
                new Column("ordinal_position", DataType.INTEGER, true, false, null),
                new Column("parameter_mode", DataType.TEXT, true, false, null),
                new Column("parameter_name", DataType.TEXT, true, false, null),
                new Column("data_type", DataType.TEXT, true, false, null),
                new Column("parameter_default", DataType.TEXT, true, false, null)
        );
        return new Table("parameters", cols); // empty, function params not tracked in detail
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
