package com.memgres.engine;

/**
 * The names of the functions memgres implements as built-ins.
 *
 * <p>PostgreSQL lists every built-in in {@code pg_catalog.pg_proc}, and tools read that list
 * to decide what the server can do: the JDBC driver answers
 * {@code DatabaseMetaData.getFunctions} from it, {@code ::regproc} resolves through it, and
 * psql's \\df reads it. Evaluating a function without recording it there leaves the function
 * working but invisible, so this list is what makes the catalog agree with the executor.
 *
 * <p>What is deliberately <em>not</em> here is as important. memgres's lexer knows a good many
 * names that are not functions at all — type names ({@code integer}, {@code jsonb}), the fields
 * {@code EXTRACT} accepts ({@code epoch}, {@code century}), the suffixes {@code pg_size_bytes}
 * parses ({@code kb}, {@code mb}), the system columns ({@code ctid}, {@code xmin}) and the
 * syntax PostgreSQL evaluates without a pg_proc row ({@code coalesce}, {@code trim},
 * {@code current_date}). Listing one of those as a function is the same mistake as omitting a
 * real one: a client that resolves it writes a call the server has never accepted.
 */
final class BuiltinFunctionNames {

    private BuiltinFunctionNames() {
    }

    static final String[] NAMES = {
            "_pg_expandarray", "abs", "acldefault", "aclexplode", "age", "akeys", "area", "array_agg",
            "array_append", "array_cat", "array_dims", "array_fill", "array_length", "array_lower",
            "array_ndims", "array_position", "array_positions", "array_prepend", "array_remove",
            "array_replace", "array_reverse", "array_sample", "array_shuffle", "array_sort",
            "array_to_string", "array_to_tsvector", "array_upper", "ascii", "avals", "avg",
            "bit", "bit_and", "bit_length", "bit_or", "bit_xor",
            "bool_and", "bool_or", "bound_box", "box", "btrim", "bytea",
            "cardinality", "ceil", "ceiling", "center",
            "char", "char_length", "character_length", "chr", "cidr", "circle",
            "clock_timestamp", "closest_point", "col_description", "concat",
            "concat_ws", "convert", "count", "crc32", "crc32c",
            "current_database", "current_query",
            "current_schema", "current_schemas", "current_setting",
            "current_user", "currval", "date", "date_bin", "date_part",
            "date_trunc", "decode", "defined", "delete",
            "delete_key", "dense_rank", "diagonal", "diameter", "div", "each",
            "encode", "enum_cmp", "enum_first", "enum_last", "enum_range", "exist", "exp",
            "extract", "first_value", "float4", "float8", "floor", "format", "format_type",
            "gen_random_bytes", "generate_series", "generate_subscripts", "get_current_ts_config",
            "has_any_column_privilege", "has_column_privilege", "has_database_privilege",
            "has_foreign_data_wrapper_privilege", "has_function_privilege", "has_language_privilege",
            "has_parameter_privilege", "has_schema_privilege", "has_sequence_privilege",
            "has_server_privilege", "has_table_privilege", "has_tablespace_privilege",
            "has_type_privilege", "height", "hstore", "hstore_to_array",
            "hstore_to_json", "hstore_to_json_loose", "hstore_to_jsonb", "hstore_to_jsonb_loose",
            "hstore_to_matrix", "icu_unicode_version", "inet_client_addr", "inet_client_port",
            "inet_server_addr", "inet_server_port", "initcap", "int2", "int4", "int8",
            "intersects", "interval", "is_horizontal", "is_parallel", "is_perpendicular",
            "is_vertical", "isclosed", "isdefined", "isexists", "isfinite", "isopen",
            "json_agg", "json_array_elements",
            "json_array_elements_text", "json_array_length", "json_build_array",
            "json_build_object", "json_each", "json_extract_path", "json_extract_path_text",
            "json_object_keys",
            "json_typeof", "jsonb_agg", "jsonb_array_elements", "jsonb_array_elements_text",
            "jsonb_array_length", "jsonb_build_array", "jsonb_build_object", "jsonb_each",
            "jsonb_each_text", "jsonb_exists", "jsonb_extract_path", "jsonb_extract_path_text",
            "jsonb_insert", "jsonb_object_keys", "jsonb_path_exists", "jsonb_path_exists_tz",
            "jsonb_path_query", "jsonb_path_query_array", "jsonb_path_query_array_tz",
            "jsonb_path_query_first", "jsonb_path_query_first_tz", "jsonb_path_query_tz",
            "jsonb_pretty", "jsonb_set", "jsonb_set_lax", "jsonb_typeof", "justify_days",
            "justify_hours", "justify_interval", "lag", "last_value", "lastval", "lead",
            "left", "length", "line", "ln", "lo_close", "lo_creat", "lo_create", "lo_export",
            "lo_from_bytea", "lo_get", "lo_import", "lo_lseek", "lo_open", "lo_put", "lo_tell",
            "lo_truncate", "lo_unlink", "log", "log10", "loread", "lower",
            "lowrite", "lpad", "lseg", "ltrim", "macaddr", "macaddr8", "make_date", "make_interval",
            "make_time", "make_timestamp", "make_timestamptz", "max", "md5", "merge_action",
            "min", "mod", "name", "nextval",
            "normalize", "now", "npoints", "nth_value", "ntile", "num_nonnulls", "num_nulls",
            "numeric", "numnode", "obj_description", "octet_length", "oid", "overlaps", "overlay",
            "path", "pclose", "pg_advisory_lock", "pg_advisory_unlock", "pg_advisory_xact_lock",
            "pg_advisory_xact_unlock", "pg_backend_pid", "pg_blocking_pids", "pg_cancel_backend",
            "pg_client_encoding", "pg_collation_is_visible", "pg_column_size", "pg_conf_load_time",
            "pg_conversion_is_visible", "pg_current_logfile", "pg_current_snapshot",
            "pg_current_wal_flush_lsn", "pg_current_wal_insert_lsn", "pg_current_wal_lsn",
            "pg_current_xact_id", "pg_current_xact_id_if_assigned", "pg_database_size",
            "pg_describe_object", "pg_drop_replication_slot", "pg_encoding_to_char",
            "pg_export_snapshot", "pg_function_is_visible", "pg_get_constraintdef", "pg_get_expr",
            "pg_get_function_arguments", "pg_get_function_identity_arguments",
            "pg_get_function_result", "pg_get_function_sqlbody", "pg_get_functiondef",
            "pg_get_indexdef", "pg_get_keywords", "pg_get_partkeydef", "pg_get_ruledef",
            "pg_get_serial_sequence", "pg_get_triggerdef", "pg_get_userbyid", "pg_get_viewdef",
            "pg_has_role", "pg_indexam_has_property", "pg_indexes_size", "pg_is_in_recovery",
            "pg_is_other_temp_schema", "pg_is_wal_replay_paused", "pg_last_wal_receive_lsn",
            "pg_last_wal_replay_lsn", "pg_last_xact_replay_timestamp", "pg_listening_channels",
            "pg_log_backend_memory_contexts", "pg_ls_archive_statusdir", "pg_ls_dir", "pg_ls_logdir",
            "pg_ls_tmpdir", "pg_ls_waldir", "pg_my_temp_schema", "pg_notification_queue_usage",
            "pg_notify", "pg_opclass_is_visible", "pg_operator_is_visible", "pg_partition_ancestors",
            "pg_partition_root", "pg_partition_tree", "pg_postmaster_start_time", "pg_promote",
            "pg_read_binary_file", "pg_read_file", "pg_relation_filepath", "pg_relation_size",
            "pg_reload_conf", "pg_rotate_logfile", "pg_safe_snapshot_blocking_pids",
            "pg_sequence_last_value", "pg_size_bytes", "pg_size_pretty", "pg_sleep", "pg_sleep_for",
            "pg_sleep_until", "pg_snapshot_xip", "pg_snapshot_xmax", "pg_snapshot_xmin",
            "pg_stat_clear_snapshot", "pg_stat_file", "pg_stat_reset", "pg_stat_reset_shared",
            "pg_stat_reset_single_function_counters", "pg_stat_reset_single_table_counters",
            "pg_stat_statements_reset", "pg_table_is_visible", "pg_table_size",
            "pg_tablespace_location", "pg_tablespace_size", "pg_terminate_backend",
            "pg_total_relation_size", "pg_ts_config_is_visible", "pg_ts_dict_is_visible",
            "pg_ts_parser_is_visible", "pg_ts_template_is_visible", "pg_type_is_visible", "pg_typeof",
            "pg_visible_in_snapshot", "pg_wal_lsn_diff", "pg_xact_status", "phraseto_tsquery", "pi",
            "plainto_tsquery", "point", "polygon", "popen", "populate_record", "position", "power",
            "querytree", "quote_ident", "quote_literal", "quote_nullable", "radius",
            "random", "rank", "regclass", "regexp_count", "regexp_instr",
            "regexp_like", "regexp_match", "regexp_matches", "regexp_replace", "regexp_split_to_array",
            "regexp_substr", "repeat", "replace",
            "reverse", "right", "round", "row_number", "rpad", "rtrim",
            "session_user", "set_config", "setseed", "setval",
            "setweight", "sha1", "sha224", "sha256", "sha384", "sha512", "shobj_description", "sign",
            "skeys", "slice", "slope", "split_part", "sqrt", "starts_with",
            "statement_timestamp", "string_agg", "string_to_array", "strip", "strpos", "substr",
            "substring", "sum", "svals", "text", "time",
            "timeofday", "timestamp", "timestamptz", "timetz", "timezone",
            "to_char", "to_date", "to_hex", "to_number", "to_regclass",
            "to_regproc", "to_regprocedure", "to_regtype", "to_timestamp", "to_tsquery", "to_tsvector",
            "transaction_timestamp", "translate", "trim_array", "trunc", "ts_debug",
            "ts_delete", "ts_filter", "ts_headline", "ts_lexize", "ts_parse", "ts_rank", "ts_rank_cd",
            "ts_rewrite", "ts_stat", "ts_token_type", "tsquery_phrase",
            "tsvector_to_array", "txid_current", "txid_current_if_assigned", "txid_current_snapshot",
            "txid_snapshot_xip", "txid_snapshot_xmax", "txid_snapshot_xmin", "txid_status",
            "unistr", "unnest", "upper", "uuidv7", "varbit", "varchar",
            "version", "websearch_to_tsquery", "width", "xml"
    };
}
