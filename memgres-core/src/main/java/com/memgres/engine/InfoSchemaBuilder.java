package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.*;

/**
 * Builds information_schema virtual tables from the current database metadata.
 * Extracted from SystemCatalog to keep that class focused on pg_catalog tables.
 */
public class InfoSchemaBuilder {

    private final Database database;
    private final OidSupplier oids;

    public InfoSchemaBuilder(Database database, OidSupplier oids) {
        this.database = database;
        this.oids = oids;
    }

    /**
     * Build the information_schema table for the given table name.
     * Returns an empty table for unrecognized names.
     */
    public Table build(String tableName) {
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
                        "memgres", schemaEntry.getKey(), tableName, "BASE TABLE",
                        null, null, null, null, null, "YES", "NO"
                });
            }
        }

        for (Database.ViewDef vd : database.getViews().values()) {
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            table.insertRow(new Object[]{
                    "memgres", vSchema, vd.name(), "VIEW",
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

                    Integer charMaxLen = null; // could be populated if we tracked varchar(N) lengths
                    Integer charOctetLen = isCharType ? Integer.valueOf(1073741824) : null;
                    Integer numPrec = CatalogHelper.numericPrecision(dt);
                    Integer numPrecRadix = numPrec != null ? Integer.valueOf(dt == DataType.NUMERIC ? 10 : 2) : null;
                    Integer numScale = dt == DataType.NUMERIC ? Integer.valueOf(0) : (numPrec != null && dt != DataType.REAL && dt != DataType.DOUBLE_PRECISION ? Integer.valueOf(0) : null);
                    Integer datetimePrec = isDateTimeType ? Integer.valueOf(6) : null;
                    if (dt == DataType.DATE) datetimePrec = Integer.valueOf(0);
                    Integer intervalPrec = dt == DataType.INTERVAL ? Integer.valueOf(6) : null;

                    String udtSchema = isCharType || isNumericType || isDateTimeType || dt == DataType.BOOLEAN
                            || dt == DataType.BYTEA || dt == DataType.UUID || dt == DataType.JSON || dt == DataType.JSONB
                            || dt == DataType.XML ? "pg_catalog" : (dt == DataType.ENUM ? schemaEntry.getKey() : "pg_catalog");
                    String udtName = (dt == DataType.ENUM && col.getEnumTypeName() != null)
                            ? col.getEnumTypeName() : col.getType().getPgName();

                    table.insertRow(new Object[]{
                            "memgres",                              // table_catalog
                            schemaEntry.getKey(),                   // table_schema
                            t.getName(),                            // table_name
                            col.getName(),                          // column_name
                            i + 1,                                  // ordinal_position
                            CatalogHelper.formatColumnDefault(col), // column_default
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
                            null,                                   // character_set_catalog
                            null,                                   // character_set_schema
                            null,                                   // character_set_name
                            null,                                   // collation_catalog
                            null,                                   // collation_schema
                            null,                                   // collation_name
                            null,                                   // domain_catalog
                            null,                                   // domain_schema
                            null,                                   // domain_name
                            "memgres",                              // udt_catalog
                            udtSchema,                              // udt_schema
                            udtName,                                // udt_name
                            null,                                   // scope_catalog
                            null,                                   // scope_schema
                            null,                                   // scope_name
                            null,                                   // maximum_cardinality
                            String.valueOf(i + 1),                  // dtd_identifier
                            "NO",                                   // is_self_referencing
                            "NO",                                   // is_identity (not yet supported)
                            null,                                   // identity_generation
                            null,                                   // identity_start
                            null,                                   // identity_increment
                            null,                                   // identity_maximum
                            null,                                   // identity_minimum
                            null,                                   // identity_cycle
                            col.isGenerated() ? "ALWAYS" : "NEVER", // is_generated
                            col.getGeneratedExpr(),                 // generation_expression
                            "YES"                                   // is_updatable
                    });
                }
            }
        }
        return table;
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
            table.insertRow(new Object[]{"memgres", schemaName, "memgres", null, null, null});
        }
        table.insertRow(new Object[]{"memgres", "pg_catalog", "memgres", null, null, null});
        table.insertRow(new Object[]{"memgres", "information_schema", "memgres", null, null, null});
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
                            "memgres", schemaEntry.getKey(), sc.getName(),
                            "memgres", schemaEntry.getKey(), t.getName(),
                            type, "NO", "NO", "YES"
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
                                "memgres", schemaEntry.getKey(), conname,
                                "memgres", schemaEntry.getKey(), t.getName(),
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
                                "memgres", schemaEntry.getKey(), sc.getName(),
                                "memgres", schemaEntry.getKey(), t.getName(),
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
                            "memgres", schemaEntry.getKey(), sc.getName(),
                            "memgres", schemaEntry.getKey(), refConstraintName,
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
                    "memgres",           // specific_catalog
                    schema,              // specific_schema
                    specificName,        // specific_name
                    "memgres",           // routine_catalog
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
                    "memgres",           // type_udt_catalog
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
                table.insertRow(new Object[]{
                        "memgres", "public", seqName, "bigint", 64,
                        String.valueOf(seq.getStartWith()),
                        String.valueOf(seq.getMinValue()),
                        String.valueOf(seq.getMaxValue()),
                        String.valueOf(seq.getIncrementBy()),
                        "NO"
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
            table.insertRow(new Object[]{
                    "memgres", vSchema, vd.name(), viewDef, "NONE", "NO", "NO", "NO", "NO", "NO"
            });
        }
        return table;
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
                    "memgres", "public", entry.getKey(),
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
                                "memgres", schemaEntry.getKey(), sc.getName(),
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
                                    "memgres", schemaEntry.getKey(), tableEntry.getKey(), col,
                                    "memgres", schemaEntry.getKey(), sc.getName()
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
                                "memgres", schemaEntry.getKey(), tableEntry.getKey(),
                                "memgres", schemaEntry.getKey(), sc.getName()
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
}
