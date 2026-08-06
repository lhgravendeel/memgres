package com.memgres.engine;

/**
 * The names of the functions memgres implements as built-ins, and the register of every name it
 * can call.
 *
 * <p>Two lists, because they answer two questions. {@link #NAMES} is what the catalog reports;
 * {@link #isCallable} is what the engine can dispatch, which is a larger thing and the one a
 * refusal has to be decided from. The second is {@link #NAMES} together with
 * {@link #ALSO_CALLABLE}, and the difference between them is explained there.
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
public final class BuiltinFunctionNames {

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
            "clock_timestamp", "col_description", "concat",
            "concat_ws", "convert", "count", "crc32", "crc32c",
            "current_database", "current_query",
            "current_schema", "current_schemas", "current_setting",
            "current_user", "currval", "date", "date_bin", "date_part",
            "date_trunc", "decode", "defined", "delete",
            "dense_rank", "diagonal", "diameter", "div", "each",
            "encode", "enum_cmp", "enum_first", "enum_last", "enum_range", "exist", "exp",
            "extract", "first_value", "float4", "float8", "floor", "format", "format_type",
            "gamma", "gen_random_bytes", "generate_series", "generate_subscripts",
            "get_current_ts_config",
            "has_any_column_privilege", "has_column_privilege", "has_database_privilege",
            "has_foreign_data_wrapper_privilege", "has_function_privilege", "has_language_privilege",
            "has_largeobject_privilege",
            "has_parameter_privilege", "has_schema_privilege", "has_sequence_privilege",
            "has_server_privilege", "has_table_privilege", "has_tablespace_privilege",
            "has_type_privilege", "height", "hstore", "hstore_to_array",
            "hstore_to_json", "hstore_to_json_loose", "hstore_to_jsonb", "hstore_to_jsonb_loose",
            "hstore_to_matrix", "icu_unicode_version", "inet_client_addr", "inet_client_port",
            "inet_server_addr", "inet_server_port", "initcap", "int2", "int4", "int8",
            "interval", "isclosed", "isdefined", "isexists", "isfinite", "isopen",
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
            "left", "lgamma", "length", "line", "ln", "lo_close", "lo_creat", "lo_create", "lo_export",
            "lo_from_bytea", "lo_get", "lo_import", "lo_lseek", "lo_open", "lo_put", "lo_tell",
            "lo_truncate", "lo_unlink", "log", "log10", "loread", "lower",
            "lowrite", "lpad", "lseg", "ltrim", "macaddr", "macaddr8", "make_date", "make_interval",
            "make_time", "make_timestamp", "make_timestamptz", "max", "md5",
            "min", "mod", "name", "nextval",
            "normalize", "now", "npoints", "nth_value", "ntile", "num_nonnulls", "num_nulls",
            "numeric", "numnode", "obj_description", "octet_length", "oid", "overlaps", "overlay",
            "path", "pclose", "pg_advisory_lock", "pg_advisory_unlock", "pg_advisory_xact_lock",
            "pg_backend_pid", "pg_blocking_pids", "pg_cancel_backend",
            "pg_client_encoding", "pg_collation_is_visible", "pg_column_is_updatable",
            "pg_column_size", "pg_column_toast_chunk_id", "pg_conf_load_time",
            "pg_conversion_is_visible", "pg_current_logfile", "pg_current_snapshot",
            "pg_current_wal_flush_lsn", "pg_current_wal_insert_lsn", "pg_current_wal_lsn",
            "pg_current_xact_id", "pg_current_xact_id_if_assigned", "pg_database_size",
            "pg_describe_object", "pg_drop_replication_slot", "pg_encoding_to_char",
            "pg_export_snapshot", "pg_function_is_visible", "pg_get_constraintdef", "pg_get_expr",
            "pg_get_function_arguments", "pg_get_function_identity_arguments",
            "pg_get_function_result", "pg_get_function_sqlbody", "pg_get_functiondef",
            "pg_get_indexdef", "pg_get_keywords", "pg_get_partkeydef", "pg_get_ruledef",
            "pg_get_acl",
            "pg_get_loaded_modules", "pg_get_serial_sequence", "pg_get_triggerdef", "pg_get_userbyid", "pg_get_viewdef",
            "pg_has_role", "pg_indexam_has_property", "pg_indexes_size", "pg_is_in_recovery",
            "pg_is_other_temp_schema", "pg_is_wal_replay_paused", "pg_last_wal_receive_lsn",
            "pg_last_wal_replay_lsn", "pg_last_xact_replay_timestamp", "pg_listening_channels",
            "pg_log_backend_memory_contexts", "pg_ls_archive_statusdir", "pg_ls_dir", "pg_ls_logdir",
            "pg_ls_tmpdir", "pg_ls_waldir", "pg_my_temp_schema", "pg_notification_queue_usage",
            "pg_notify", "pg_opclass_is_visible", "pg_operator_is_visible", "pg_partition_ancestors",
            "pg_partition_root", "pg_partition_tree", "pg_postmaster_start_time", "pg_promote",
            "pg_read_binary_file", "pg_read_file", "pg_relation_filepath",
            "pg_relation_is_updatable", "pg_relation_size",
            "pg_reload_conf", "pg_rotate_logfile", "pg_safe_snapshot_blocking_pids",
            "pg_sequence_last_value", "pg_size_bytes", "pg_size_pretty", "pg_sleep", "pg_sleep_for",
            "pg_sleep_until", "pg_snapshot_xip", "pg_snapshot_xmax", "pg_snapshot_xmin",
            "pg_stat_clear_snapshot", "pg_stat_file", "pg_stat_reset", "pg_stat_reset_shared",
            "pg_stat_reset_single_function_counters", "pg_stat_reset_single_table_counters",
            "pg_stat_statements_reset", "pg_table_is_visible", "pg_stat_get_backend_io", "pg_table_size",
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
            "setweight", "sha224", "sha256", "sha384", "sha512", "shobj_description", "sign",
            "skeys", "slice", "slope", "split_part", "sqrt", "starts_with",
            "statement_timestamp", "string_agg", "string_to_array", "strip", "strpos", "substr",
            "substring", "sum", "svals", "text", "time",
            "timeofday", "timestamp", "timestamptz", "timetz", "timezone",
            "to_ascii", "to_char", "to_date", "to_hex", "to_number", "to_regclass",
            "to_regproc", "to_regprocedure", "to_regtype", "to_timestamp", "to_tsquery", "to_tsvector",
            "transaction_timestamp", "translate", "trim_array", "trunc", "ts_debug",
            "ts_delete", "ts_filter", "ts_headline", "ts_lexize", "ts_parse", "ts_rank", "ts_rank_cd",
            "ts_rewrite", "ts_stat", "ts_token_type", "tsquery_phrase",
            "tsvector_to_array", "txid_current", "txid_current_if_assigned", "txid_current_snapshot",
            "txid_snapshot_xip", "txid_snapshot_xmax", "txid_snapshot_xmin", "txid_status",
            "unistr", "unnest", "upper", "uuidv7", "varbit", "varchar",
            "version", "websearch_to_tsquery", "width", "xml"
    };

    /**
     * The rest of the register: names the engine dispatches that have no pg_proc row of their own.
     *
     * <p>{@link #NAMES} is what the catalog reports, and it is not the same question as what the
     * engine can call. The two differ in both directions. PostgreSQL evaluates {@code coalesce},
     * {@code greatest}, {@code trim} and {@code current_date} without a pg_proc row, so listing
     * them above would put a row in the catalog PostgreSQL does not have — but memgres can call
     * every one of them, so leaving them out of the register would refuse working SQL. The same
     * goes for the contrib functions memgres implements natively ({@code levenshtein},
     * {@code similarity}, {@code digest}, the uuid-ossp generators), the type names the grammar
     * spells like a call ({@code integer}, {@code jsonb}, {@code record}), and the handful of
     * internal names the parser writes for syntax it desugars ({@code __xmltable__} and friends),
     * and the few the engine answers for with something other than "no such function" —
     * {@code values} is a syntax error where it stands, {@code open} and {@code close} are a type
     * and not a function at all, and each of those answers is better than 42883.
     *
     * <p>Also here, and for the same reason read the other way round, are the names memgres adds
     * that PostgreSQL has no row for anywhere: the geometric aliases {@code closest_point},
     * {@code intersects}, {@code is_horizontal}, {@code is_vertical}, {@code is_parallel} and
     * {@code is_perpendicular}, and {@code merge_action}, which PostgreSQL evaluates as a parser
     * construct with no pg_proc row of its own. memgres keeps every one of them callable — they
     * are its own extension and code already written against them still runs — but a name
     * PostgreSQL has nowhere may not be advertised in {@code pg_catalog}, because a client that
     * reads it there writes a call the real server rejects.
     *
     * <p>This list was built by sweeping the engine's own dispatch — every case label of every
     * switch on a folded function name, every name compared against one, every name the parser
     * synthesises — and adding the aggregates, the window functions and every name the signature
     * table records. {@code FunctionRegisterTest} sweeps the sources again and fails if a name the
     * engine can dispatch is missing from here, because a name missing from the register is one
     * the resolution check would refuse a working call to.
     */
    private static final String[] ALSO_CALLABLE = {
            // The helpers information_schema's own views are written in terms of, and
            // pg_logical_emit_message. PostgreSQL has a pg_proc row for each — the helpers in the
            // information_schema namespace, not pg_catalog — so they belong in NAMES rather than
            // here, but a name listed there with no row in BuiltinFunctionSignatures is
            // registered with prorettype 0, which is a row no client can act on and one
            // PostgreSQL's own catalog never contains. They are callable here until each has its
            // signature recorded.
            "_pg_char_max_length", "_pg_char_octet_length", "_pg_datetime_precision",
            "_pg_index_position", "_pg_interval_type", "_pg_numeric_precision",
            "_pg_numeric_precision_radix", "_pg_numeric_scale", "_pg_truetypid", "_pg_truetypmod",
            "pg_logical_emit_message",
            "__array_assign_slice__", "__is_normalized__", "__json_table__", "__rows_from__",
            "__similar_to_escape__", "__subscript_assign__", "__tsquery_not__",
            "__xmlattributes__", "__xmltable__", "abbrev", "acos", "acosd", "acosh", "any_value",
            "array_to_json",
            "arraycontained", "arraycontains", "arrayoverlap", "asin", "asind", "asinh", "atan",
            "atan2", "atan2d", "atand", "atanh", "bigint", "bit_count", "bool", "boolean",
            "bpchar", "broadcast", "casefold", "cbrt", "character", "close", "closest_point",
            "coalesce", "convert_from",
            "convert_to", "corr", "cos", "cosd", "cosh", "cot", "cotd", "covar_pop", "covar_samp",
            "cube", "cube_dim", "cume_dist", "current_catalog", "current_date",
            "current_role",
            "current_time", "current_timestamp", "database_to_xml", "datemultirange", "daterange",
            "decimal", "degrees", "digest", "every", "factorial", "family", "gcd",
            "gen_random_uuid", "gen_salt", "get_bit", "get_byte", "greatest", "grouping", "hmac",
            "host", "hostmask", "inet_merge", "inet_same_family", "int", "int4multirange",
            "int4range", "int8multirange", "int8range", "integer", "intersects",
            "is_horizontal", "is_parallel", "is_perpendicular", "is_vertical",
            "isempty", "json", "json_array",
            "json_array_constructor", "json_array_subquery", "json_arrayagg", "json_each_text",
            "json_exists", "json_insert", "json_object", "json_object_agg",
            "json_object_constructor", "json_path_query", "json_path_query_first", "json_set",
            "json_set_lax",
            "json_objectagg", "json_populate_record", "json_populate_recordset", "json_query",
            "json_scalar", "json_serialize", "json_strip_nulls", "json_table", "json_to_record",
            "json_to_recordset", "json_value", "jsonb", "jsonb_exists_all", "jsonb_exists_any",
            "jsonb_object", "jsonb_object_agg", "jsonb_path_match", "jsonb_path_match_tz",
            "jsonb_populate_record", "jsonb_populate_recordset", "jsonb_strip_nulls",
            "jsonb_to_record", "jsonb_to_recordset", "lcm", "least", "levenshtein", "localtime",
            "localtimestamp", "lower_inc", "lower_inf", "macaddr8_set7bit", "masklen",
            "merge_action", "min_scale", "mode", "multirange", "netmask", "network", "nullif",
            "nummultirange", "numrange",
            "open", "parse_ident", "percent_rank", "percentile_cont", "percentile_disc",
            "pg_advisory_lock_shared", "pg_advisory_unlock_all", "pg_advisory_unlock_shared",
            "pg_advisory_xact_lock_shared", "pg_available_extension_versions",
            "pg_available_extensions", "pg_backup_start", "pg_backup_stop",
            "pg_create_logical_replication_slot", "pg_create_physical_replication_slot",
            "pg_create_restore_point", "pg_event_trigger_ddl_commands",
            "pg_event_trigger_dropped_objects", "pg_event_trigger_table_rewrite_oid",
            "pg_event_trigger_table_rewrite_reason", "pg_get_sequence_data",
            "pg_logical_slot_get_changes", "pg_logical_slot_peek_changes", "pg_options_to_table",
            "pg_replication_slot_advance", "pg_show_all_settings", "pg_switch_wal",
            "pg_try_advisory_lock", "pg_try_advisory_lock_shared", "pg_try_advisory_xact_lock",
            "pg_try_advisory_xact_lock_shared", "pg_wal_replay_pause", "pg_wal_replay_resume",
            "pg_walfile_name", "pow", "query_to_xml", "radians", "random_normal", "range_agg",
            "range_intersect_agg", "range_merge", "real", "record", "regexp_split_to_table",
            "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope",
            "regr_sxx", "regr_sxy", "regr_syy", "row", "row_to_json", "scale", "schema_to_xml",
            "set_bit", "set_byte", "set_masklen", "show_trgm", "similarity", "sin", "sind", "sinh",
            "smallint", "soundex", "stddev", "stddev_pop", "stddev_samp", "string_to_table",
            "substring_similar", "table_to_xml", "tan", "tand", "tanh", "to_json", "to_jsonb",
            "trigger", "trim", "trim_scale", "tsmultirange", "tsrange", "tstzmultirange",
            "tstzrange", "unaccent", "unicode", "unicode_assigned", "unicode_version", "upper_inc",
            "upper_inf", "uuid", "uuid_extract_timestamp", "uuid_extract_version", "values",
            "uuid_generate_v1", "uuid_generate_v3", "uuid_generate_v4", "uuid_generate_v5",
            "uuid_nil", "uuid_ns_dns", "uuid_ns_url", "uuidv4", "var_pop", "var_samp", "variance",
            "void", "width_bucket", "xml_is_well_formed", "xml_is_well_formed_content",
            "xml_is_well_formed_document", "xmlagg", "xmlcomment", "xmlconcat", "xmlelement",
            "xmlexists", "xmlforest", "xmlparse", "xmlpi", "xmlroot", "xmlserialize", "xmltable",
            "xmltext", "xpath", "xpath_exists",
    };

    private static final java.util.Set<String> NAME_SET =
            new java.util.HashSet<String>(java.util.Arrays.asList(NAMES));

    private static final java.util.Set<String> REGISTER = buildRegister();

    private static java.util.Set<String> buildRegister() {
        java.util.Set<String> all = new java.util.HashSet<String>(NAME_SET);
        all.addAll(java.util.Arrays.asList(ALSO_CALLABLE));
        return java.util.Collections.unmodifiableSet(all);
    }

    /** Every name in the register, for the sweep that checks nothing the engine calls is missing. */
    static java.util.Set<String> register() {
        return REGISTER;
    }

    /** True when {@code name} — already lower case and unqualified — is one of these. */
    static boolean contains(String name) {
        return NAME_SET.contains(name);
    }

    /**
     * Whether the engine can dispatch a call to this name at all.
     *
     * <p>The question a refusal turns on. PostgreSQL resolves a call before it judges the clause
     * carrying it, so an unknown name outranks a complaint about FILTER, about OVER, or about a
     * column further along the select list — but only a register that is complete may be read that
     * way, because a name missing from it would refuse SQL that works. This one is complete by
     * construction and by sweep, and it is deliberately generous at the edges: a type name spelled
     * like a call is cast syntax rather than a function, and answering yes to one costs nothing but
     * the earlier message. Names a user declared are not here — the caller asks the database for
     * those, which is where they live.
     */
    public static boolean isCallable(String name) {
        if (name == null) return false;
        if (REGISTER.contains(name)) return true;
        return DataType.fromPgName(name) != null;
    }
}
