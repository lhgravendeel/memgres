package com.memgres.engine;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The types PostgreSQL gives the columns of information_schema.
 *
 * <p>information_schema is defined by the SQL standard over three domains: an identifier is a
 * {@code sql_identifier}, which is a {@code name}; descriptive text and the yes/no columns are
 * {@code character_data} and {@code yes_or_no}, which are {@code character varying}; a count or
 * a length is a {@code cardinal_number}, which is an {@code integer}. Built with every column
 * declared text, every one of these views described itself to a client as text, so a driver
 * reading column metadata was told the wrong type for 222 columns across 17 views -- and a
 * tool that reads information_schema to decide how to render a value read text for all of them.
 *
 * <p>Measured from PostgreSQL 18; regenerate with tools/replay/annotate.sh if it changes.
 */
final class InfoSchemaColumnTypes {
    private InfoSchemaColumnTypes() {}

    private static final Map<String, Map<String, DataType>> BY_VIEW = build();

    /** The type this information_schema column has, or null where it is already right. */
    static DataType typeOf(String view, String column) {
        Map<String, DataType> cols = BY_VIEW.get(view.toLowerCase(java.util.Locale.ROOT));
        return cols == null ? null : cols.get(column.toLowerCase(java.util.Locale.ROOT));
    }

    private static Map<String, Map<String, DataType>> build() {
        Map<String, Map<String, DataType>> m = new HashMap<String, Map<String, DataType>>();
        m.put("applicable_roles", cols(new String[][]{
                {"grantee", "NAME"}, {"role_name", "NAME"}, {"is_grantable", "VARCHAR"},
        }));
        m.put("check_constraints", cols(new String[][]{
                {"constraint_catalog", "NAME"}, {"constraint_schema", "NAME"}, {"constraint_name", "NAME"},
                {"check_clause", "VARCHAR"},
        }));
        m.put("collations", cols(new String[][]{
                {"collation_catalog", "NAME"}, {"collation_schema", "NAME"}, {"collation_name", "NAME"},
                {"pad_attribute", "VARCHAR"},
        }));
        m.put("columns", cols(new String[][]{
                {"table_catalog", "NAME"}, {"table_schema", "NAME"}, {"table_name", "NAME"},
                {"column_name", "NAME"}, {"column_default", "VARCHAR"}, {"is_nullable", "VARCHAR"},
                {"data_type", "VARCHAR"}, {"interval_type", "VARCHAR"}, {"character_set_catalog", "NAME"},
                {"character_set_schema", "NAME"}, {"character_set_name", "NAME"}, {"collation_catalog", "NAME"},
                {"collation_schema", "NAME"}, {"collation_name", "NAME"}, {"domain_catalog", "NAME"},
                {"domain_schema", "NAME"}, {"domain_name", "NAME"}, {"udt_catalog", "NAME"},
                {"udt_schema", "NAME"}, {"udt_name", "NAME"}, {"scope_catalog", "NAME"},
                {"scope_schema", "NAME"}, {"scope_name", "NAME"}, {"dtd_identifier", "NAME"},
                {"is_self_referencing", "VARCHAR"}, {"is_identity", "VARCHAR"}, {"identity_generation", "VARCHAR"},
                {"identity_start", "VARCHAR"}, {"identity_increment", "VARCHAR"}, {"identity_maximum", "VARCHAR"},
                {"identity_minimum", "VARCHAR"}, {"identity_cycle", "VARCHAR"}, {"is_generated", "VARCHAR"},
                {"generation_expression", "VARCHAR"}, {"is_updatable", "VARCHAR"},
        }));
        m.put("constraint_column_usage", cols(new String[][]{
                {"table_catalog", "NAME"}, {"table_schema", "NAME"}, {"table_name", "NAME"},
                {"column_name", "NAME"}, {"constraint_catalog", "NAME"}, {"constraint_schema", "NAME"},
                {"constraint_name", "NAME"},
        }));
        m.put("constraint_table_usage", cols(new String[][]{
                {"table_catalog", "NAME"}, {"table_schema", "NAME"}, {"table_name", "NAME"},
                {"constraint_catalog", "NAME"}, {"constraint_schema", "NAME"}, {"constraint_name", "NAME"},
        }));
        m.put("domains", cols(new String[][]{
                {"domain_catalog", "NAME"}, {"domain_schema", "NAME"}, {"domain_name", "NAME"},
                {"data_type", "VARCHAR"}, {"character_set_catalog", "NAME"}, {"character_set_schema", "NAME"},
                {"character_set_name", "NAME"}, {"collation_catalog", "NAME"}, {"collation_schema", "NAME"},
                {"collation_name", "NAME"}, {"interval_type", "VARCHAR"}, {"domain_default", "VARCHAR"},
                {"udt_catalog", "NAME"}, {"udt_schema", "NAME"}, {"udt_name", "NAME"},
                {"scope_catalog", "NAME"}, {"scope_schema", "NAME"}, {"scope_name", "NAME"},
                {"dtd_identifier", "NAME"},
        }));
        m.put("enabled_roles", cols(new String[][]{
                {"role_name", "NAME"},
        }));
        m.put("key_column_usage", cols(new String[][]{
                {"constraint_catalog", "NAME"}, {"constraint_schema", "NAME"}, {"constraint_name", "NAME"},
                {"table_catalog", "NAME"}, {"table_schema", "NAME"}, {"table_name", "NAME"},
                {"column_name", "NAME"},
        }));
        m.put("referential_constraints", cols(new String[][]{
                {"constraint_catalog", "NAME"}, {"constraint_schema", "NAME"}, {"constraint_name", "NAME"},
                {"unique_constraint_catalog", "NAME"}, {"unique_constraint_schema", "NAME"}, {"unique_constraint_name", "NAME"},
                {"match_option", "VARCHAR"}, {"update_rule", "VARCHAR"}, {"delete_rule", "VARCHAR"},
        }));
        m.put("routines", cols(new String[][]{
                {"specific_catalog", "NAME"}, {"specific_schema", "NAME"}, {"specific_name", "NAME"},
                {"routine_catalog", "NAME"}, {"routine_schema", "NAME"}, {"routine_name", "NAME"},
                {"routine_type", "VARCHAR"}, {"module_catalog", "NAME"}, {"module_schema", "NAME"},
                {"module_name", "NAME"}, {"udt_catalog", "NAME"}, {"udt_schema", "NAME"},
                {"udt_name", "NAME"}, {"data_type", "VARCHAR"}, {"character_set_catalog", "NAME"},
                {"character_set_schema", "NAME"}, {"character_set_name", "NAME"}, {"collation_catalog", "NAME"},
                {"collation_schema", "NAME"}, {"collation_name", "NAME"}, {"interval_type", "VARCHAR"},
                {"type_udt_catalog", "NAME"}, {"type_udt_schema", "NAME"}, {"type_udt_name", "NAME"},
                {"scope_catalog", "NAME"}, {"scope_schema", "NAME"}, {"scope_name", "NAME"},
                {"dtd_identifier", "NAME"}, {"routine_body", "VARCHAR"}, {"routine_definition", "VARCHAR"},
                {"external_name", "VARCHAR"}, {"external_language", "VARCHAR"}, {"parameter_style", "VARCHAR"},
                {"is_deterministic", "VARCHAR"}, {"sql_data_access", "VARCHAR"}, {"is_null_call", "VARCHAR"},
                {"sql_path", "VARCHAR"}, {"schema_level_routine", "VARCHAR"}, {"is_user_defined_cast", "VARCHAR"},
                {"is_implicitly_invocable", "VARCHAR"}, {"security_type", "VARCHAR"}, {"to_sql_specific_catalog", "NAME"},
                {"to_sql_specific_schema", "NAME"}, {"to_sql_specific_name", "NAME"}, {"as_locator", "VARCHAR"},
                {"new_savepoint_level", "VARCHAR"}, {"is_udt_dependent", "VARCHAR"}, {"result_cast_from_data_type", "VARCHAR"},
                {"result_cast_as_locator", "VARCHAR"}, {"result_cast_char_set_catalog", "NAME"}, {"result_cast_char_set_schema", "NAME"},
                {"result_cast_char_set_name", "NAME"}, {"result_cast_collation_catalog", "NAME"}, {"result_cast_collation_schema", "NAME"},
                {"result_cast_collation_name", "NAME"}, {"result_cast_interval_type", "VARCHAR"}, {"result_cast_type_udt_catalog", "NAME"},
                {"result_cast_type_udt_schema", "NAME"}, {"result_cast_type_udt_name", "NAME"}, {"result_cast_scope_catalog", "NAME"},
                {"result_cast_scope_schema", "NAME"}, {"result_cast_scope_name", "NAME"}, {"result_cast_dtd_identifier", "NAME"},
        }));
        m.put("schemata", cols(new String[][]{
                {"catalog_name", "NAME"}, {"schema_name", "NAME"}, {"schema_owner", "NAME"},
                {"default_character_set_catalog", "NAME"}, {"default_character_set_schema", "NAME"}, {"default_character_set_name", "NAME"},
                {"sql_path", "VARCHAR"},
        }));
        m.put("sequences", cols(new String[][]{
                {"sequence_catalog", "NAME"}, {"sequence_schema", "NAME"}, {"sequence_name", "NAME"},
                {"data_type", "VARCHAR"}, {"start_value", "VARCHAR"}, {"minimum_value", "VARCHAR"},
                {"maximum_value", "VARCHAR"}, {"increment", "VARCHAR"}, {"cycle_option", "VARCHAR"},
        }));
        m.put("table_constraints", cols(new String[][]{
                {"constraint_catalog", "NAME"}, {"constraint_schema", "NAME"}, {"constraint_name", "NAME"},
                {"table_catalog", "NAME"}, {"table_schema", "NAME"}, {"table_name", "NAME"},
                {"constraint_type", "VARCHAR"}, {"is_deferrable", "VARCHAR"}, {"initially_deferred", "VARCHAR"},
                {"enforced", "VARCHAR"}, {"nulls_distinct", "VARCHAR"},
        }));
        m.put("tables", cols(new String[][]{
                {"table_catalog", "NAME"}, {"table_schema", "NAME"}, {"table_name", "NAME"},
                {"table_type", "VARCHAR"}, {"self_referencing_column_name", "NAME"}, {"reference_generation", "VARCHAR"},
                {"user_defined_type_catalog", "NAME"}, {"user_defined_type_schema", "NAME"}, {"user_defined_type_name", "NAME"},
                {"is_insertable_into", "VARCHAR"}, {"is_typed", "VARCHAR"}, {"commit_action", "VARCHAR"},
        }));
        m.put("triggers", cols(new String[][]{
                {"trigger_catalog", "NAME"}, {"trigger_schema", "NAME"}, {"trigger_name", "NAME"},
                {"event_manipulation", "VARCHAR"}, {"event_object_catalog", "NAME"}, {"event_object_schema", "NAME"},
                {"event_object_table", "NAME"}, {"action_condition", "VARCHAR"}, {"action_statement", "VARCHAR"},
                {"action_orientation", "VARCHAR"}, {"action_timing", "VARCHAR"}, {"action_reference_old_table", "NAME"},
                {"action_reference_new_table", "NAME"}, {"action_reference_old_row", "NAME"}, {"action_reference_new_row", "NAME"},
        }));
        m.put("views", cols(new String[][]{
                {"table_catalog", "NAME"}, {"table_schema", "NAME"}, {"table_name", "NAME"},
                {"view_definition", "VARCHAR"}, {"check_option", "VARCHAR"}, {"is_updatable", "VARCHAR"},
                {"is_insertable_into", "VARCHAR"}, {"is_trigger_updatable", "VARCHAR"}, {"is_trigger_deletable", "VARCHAR"},
                {"is_trigger_insertable_into", "VARCHAR"},
        }));
        return Collections.unmodifiableMap(m);
    }

    private static Map<String, DataType> cols(String[][] pairs) {
        Map<String, DataType> m = new HashMap<String, DataType>();
        for (String[] p : pairs) m.put(p[0], DataType.valueOf(p[1]));
        return m;
    }
}
