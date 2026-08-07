package com.memgres.engine;

/**
 * The signatures PostgreSQL gives the built-in functions memgres implements.
 *
 * <p>{@link BuiltinFunctionNames} records that a function exists; this records what it takes and
 * what it returns. A pg_proc row carrying only a name is enough for a lookup by name and useless
 * for everything else a tool does with the catalog — overload resolution,
 * {@code pg_get_function_arguments}, a client deciding how to bind a call — so every name
 * PostgreSQL knows a signature for is listed here with it.
 *
 * <p>Every row below was read from the reference server's {@code pg_proc}: for each name memgres
 * has, <em>every</em> signature PostgreSQL declares for it, not only the one memgres's own
 * implementation was written against. That completeness is what makes the table safe to read as a
 * rule. It used to record several names in the long form PostgreSQL keeps internally and nowhere
 * else — {@code jsonb_path_query}, {@code overlaps}, {@code json_strip_nulls}, {@code jsonb_set},
 * {@code json_populate_record} among them — so reading "too few arguments" out of it refused SQL
 * PostgreSQL runs, and the arity rule had to be one-sided. With every form recorded it reads in
 * both directions. Only the names PostgreSQL has no {@code pg_proc} row for at all keep memgres's
 * own entry: the contrib functions memgres implements natively, and the handful it adds.
 *
 * <p>Columns: proname, prorettype, proargtypes (space-separated OIDs, empty when the function
 * takes none), then proretset ('t'/'f') and provolatile packed into one string, then the fewest
 * arguments this signature accepts — which is {@code pronargs} less the ones with defaults — with
 * a trailing {@code +} where the signature is variadic and so has no upper bound. Aggregates are
 * deliberately absent: they are registered separately with prokind='a'.
 */
public final class BuiltinFunctionSignatures {

    private BuiltinFunctionSignatures() {
    }

    /**
     * Whether this row is one PostgreSQL declares, rather than one of memgres's own.
     *
     * <p>The argument <em>types</em> here are read as a claim about what a name accepts, and that
     * reading holds only for the rows taken whole from the reference server. A row memgres wrote
     * for itself — a contrib function it implements natively, or one of its own extensions, marked
     * with a trailing {@code *} on the last column — names the types memgres happened to write
     * down, which are not always the ones its implementation takes: {@code closest_point} is
     * recorded over two line segments and takes a point and a segment too. Their argument counts
     * are still read, because those are recorded for every form memgres has.
     */
    private static boolean declaredByPostgres(String[] signature) {
        return signature.length > 4 && !signature[4].endsWith("*");
    }

    /** Whether this row's argument types may be read as a claim about what the name accepts. */
    static boolean isPostgresSignature(String[] signature) {
        return declaredByPostgres(signature);
    }

    /**
     * Whether PostgreSQL declares any signature at all for this name. A name it does not is one
     * this table says nothing about, and nothing may be concluded from its silence.
     */
    static boolean recordsSignature(String name) {
        return name != null && Arity.BY_NAME.containsKey(name.toLowerCase(java.util.Locale.ROOT));
    }

    /**
     * Whether some signature of this name takes this many arguments.
     *
     * <p>A signature takes anything from its fewest — {@code pronargs} less its defaulted
     * parameters — up to {@code pronargs}, and a variadic one takes any number from its fewest
     * upwards. A call whose count matches no signature of the name resolves to no function at all,
     * which is what PostgreSQL reports; the answer is only meaningful once
     * {@link #recordsSignature} is true.
     */
    static boolean acceptsArity(String name, int count) {
        Arity arity = name == null ? null
                : Arity.BY_NAME.get(name.toLowerCase(java.util.Locale.ROOT));
        return arity != null && arity.accepts(count);
    }

    /**
     * Whether this name is a window function PostgreSQL declares that takes no such argument list.
     *
     * <p>A window function is resolved by name and argument list like anything else, so
     * {@code row_number(v) OVER ()} is a function that does not exist rather than row_number with
     * something extra. The four names PostgreSQL declares both as a window function and as an
     * ordered-set aggregate — rank, dense_rank, percent_rank, cume_dist — are left out: their
     * aggregate form takes the arguments their window form does not, so a written argument list
     * settles nothing about them.
     */
    public static boolean windowCallCannotResolve(String name, int count) {
        if (name == null) return false;
        String bare = name.toLowerCase(java.util.Locale.ROOT);
        if (DUAL_KIND.contains(bare)) return false;
        boolean known = false;
        for (String[] win : WINDOW_FUNCTIONS) {
            if (!win[0].equals(bare)) continue;
            known = true;
            int params = win[2].isEmpty() ? 0 : win[2].split(" ").length;
            if (params == count) return false;
        }
        return known;
    }

    /**
     * The extension each of these names belongs to, for the names PostgreSQL keeps in no schema
     * at all until its extension is created.
     *
     * <p>PostgreSQL never puts an extension's functions in {@code pg_catalog}: they are created in
     * whatever schema {@code CREATE EXTENSION} installed the extension into, and before that they
     * do not exist. memgres implements these natively and gates the call itself — every one of
     * them answers 42883 until its extension is there — but listed them all in pg_catalog on a
     * fresh database, so the catalog advertised nine functions the engine would refuse.
     */
    private static final String[][] EXTENSION_OWNED = {
            {"uuid_generate_v1", "uuid-ossp"},
            {"uuid_generate_v3", "uuid-ossp"},
            {"uuid_generate_v4", "uuid-ossp"},
            {"uuid_generate_v5", "uuid-ossp"},
            {"uuid_nil", "uuid-ossp"},
            {"uuid_ns_dns", "uuid-ossp"},
            {"uuid_ns_url", "uuid-ossp"},
            {"show_trgm", "pg_trgm"},
            {"similarity", "pg_trgm"},
    };

    /** The extension this name belongs to, or null when it belongs to none. */
    static String owningExtension(String name) {
        if (name == null) return null;
        for (String[] owned : EXTENSION_OWNED) {
            if (owned[0].equalsIgnoreCase(name)) return owned[1];
        }
        return null;
    }

    /** The fewest arguments this signature accepts — pronargs less its defaulted parameters. */
    static int fewestArguments(String[] signature) {
        int params = signature[2].isEmpty() ? 0 : signature[2].split(" ").length;
        String fewest = signature.length > 4 ? signature[4] : String.valueOf(params);
        if (fewest.endsWith("*")) fewest = fewest.substring(0, fewest.length() - 1);
        if (fewest.endsWith("+")) fewest = fewest.substring(0, fewest.length() - 1);
        try {
            return Integer.parseInt(fewest);
        } catch (NumberFormatException e) {
            return params;
        }
    }

    /** Whether this signature collects a tail of arguments into its last declared type. */
    static boolean isVariadic(String[] signature) {
        if (signature.length <= 4) return false;
        String fewest = signature[4];
        if (fewest.endsWith("*")) fewest = fewest.substring(0, fewest.length() - 1);
        return fewest.endsWith("+");
    }

    /**
     * The routines memgres answers without reading their arguments, or by reading a NULL as some
     * value of its own.
     *
     * <p>Every one of them is strict, so PostgreSQL never enters the body with a NULL argument and
     * the call is NULL. memgres has no other backend to signal, no file to read, no lock keyspace
     * worth entering — so the implementations answer whatever they were going to answer whatever
     * they were given, and a NULL argument came back as {@code false}, as the empty array, as the
     * one role, as the empty string a void function prints. None of those is a value the client
     * asked about.
     *
     * <p>Read off the reference server: every strict pg_catalog function of one to three arguments
     * was called with NULLs on both, and these are the names where PostgreSQL answered NULL and
     * memgres answered something. The others already answer NULL of their own accord, which is why
     * this is a list rather than a rule applied to every call — a rule would have to read every
     * argument of every built-in call to find the few that need it.
     */
    private static final java.util.Set<String> NULL_IN_NULL_OUT =
            new java.util.HashSet<String>(java.util.Arrays.asList(
                    "current_schemas", "database_to_xml", "datemultirange", "int4multirange",
                    "int8multirange", "lo_creat", "lo_create", "lo_export", "lo_import",
                    "make_time", "nummultirange",
                    "pg_advisory_lock", "pg_advisory_lock_shared", "pg_advisory_unlock",
                    "pg_advisory_unlock_shared", "pg_advisory_xact_lock",
                    "pg_advisory_xact_lock_shared", "pg_blocking_pids", "pg_cancel_backend",
                    "pg_collation_is_visible", "pg_column_is_updatable", "pg_column_size",
                    "pg_conversion_is_visible", "pg_database_size", "pg_describe_object",
                    "pg_encoding_to_char", "pg_function_is_visible", "pg_get_userbyid",
                    "pg_indexam_has_property", "pg_indexes_size",
                    "pg_log_backend_memory_contexts", "pg_opclass_is_visible",
                    "pg_operator_is_visible", "pg_read_binary_file", "pg_read_file",
                    "pg_relation_filepath", "pg_relation_is_updatable", "pg_relation_size",
                    "pg_safe_snapshot_blocking_pids", "pg_sleep", "pg_sleep_for",
                    "pg_sleep_until", "pg_stat_file",
                    "pg_stat_reset_single_function_counters",
                    "pg_stat_reset_single_table_counters", "pg_table_is_visible", "pg_table_size",
                    "pg_tablespace_location", "pg_tablespace_size", "pg_terminate_backend",
                    "pg_total_relation_size", "pg_try_advisory_lock",
                    "pg_try_advisory_lock_shared", "pg_try_advisory_xact_lock",
                    "pg_try_advisory_xact_lock_shared", "pg_ts_config_is_visible",
                    "pg_ts_dict_is_visible", "pg_ts_parser_is_visible",
                    "pg_ts_template_is_visible", "pg_type_is_visible", "pg_wal_lsn_diff",
                    "random_normal", "setseed", "tsmultirange", "tstzmultirange",
                    "txid_snapshot_xmax", "txid_snapshot_xmin", "xpath", "xpath_exists"));

    /**
     * The multirange constructors, which collect a tail of ranges. Given one NULL that tail is
     * itself NULL and the call is NULL; given a NULL beside other ranges the tail is a real array
     * with a NULL in it, which is a value a multirange cannot hold and is reported as one.
     */
    private static final java.util.Set<String> RANGE_COLLECTORS =
            new java.util.HashSet<String>(java.util.Arrays.asList(
                    "datemultirange", "int4multirange", "int8multirange", "nummultirange",
                    "tsmultirange", "tstzmultirange"));

    /** Whether a NULL argument to {@code name} makes the whole call NULL without running it. */
    static boolean nullArgumentMakesTheCallNull(String name, int arity) {
        if (name == null) return false;
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        if (arity != 1 && RANGE_COLLECTORS.contains(lower)) return false;
        return NULL_IN_NULL_OUT.contains(lower);
    }

    /**
     * Whether PostgreSQL declares this signature strict.
     *
     * <p>Strict is the rule and this is the list of exceptions, read off the reference server one
     * signature at a time: a strict function is never entered with a NULL argument, so a client or
     * a planner reading the column is reading whether the body can see a NULL at all. It is keyed
     * by argument types as well as by name because the two are not decided together —
     * {@code array_to_string(anyarray, text)} is strict and the three-argument form that takes a
     * null-substitute is not.
     */
    static boolean isStrict(String name, String argTypes) {
        for (String[] lax : NOT_STRICT) {
            if (lax[0].equalsIgnoreCase(name) && lax[1].equals(argTypes)) return false;
        }
        return true;
    }

    /** Names PostgreSQL declares both prokind='w' and prokind='a'. */
    private static final java.util.Set<String> DUAL_KIND =
            new java.util.HashSet<String>(java.util.Arrays.asList(
                    "rank", "dense_rank", "percent_rank", "cume_dist"));

    /**
     * The argument counts one name accepts, gathered from every signature of it. Held in a nested
     * class so it is built on first use, after {@link #SIGNATURES} further down the file.
     */
    private static final class Arity {

        static final java.util.Map<String, Arity> BY_NAME = build();

        /** Smallest count seen with no upper bound, or -1 when every signature has one. */
        private int unbounded = -1;
        private final java.util.Set<Integer> exact = new java.util.HashSet<Integer>();

        boolean accepts(int count) {
            if (unbounded >= 0 && count >= unbounded) return true;
            return exact.contains(Integer.valueOf(count));
        }

        private static java.util.Map<String, Arity> build() {
            java.util.Map<String, Arity> map = new java.util.HashMap<String, Arity>();
            for (String[] sig : SIGNATURES) {
                String key = sig[0].toLowerCase(java.util.Locale.ROOT);
                Arity arity = map.get(key);
                if (arity == null) {
                    arity = new Arity();
                    map.put(key, arity);
                }
                int params = sig[2].isEmpty() ? 0 : sig[2].split(" ").length;
                String fewest = sig.length > 4 ? sig[4] : String.valueOf(params);
                if (fewest.endsWith("*")) fewest = fewest.substring(0, fewest.length() - 1);
                boolean variadic = fewest.endsWith("+");
                if (variadic) fewest = fewest.substring(0, fewest.length() - 1);
                int min;
                try {
                    min = Integer.parseInt(fewest);
                } catch (NumberFormatException e) {
                    min = params;
                }
                if (variadic) {
                    if (arity.unbounded < 0 || min < arity.unbounded) arity.unbounded = min;
                } else {
                    for (int n = min; n <= params; n++) arity.exact.add(Integer.valueOf(n));
                }
            }
            return java.util.Collections.unmodifiableMap(map);
        }
    }

    /**
     * Names whose every signature returns void, worked out from the table below. Held in a nested
     * class so it is built on first use: a static field here would initialise before
     * {@link #SIGNATURES} further down the file and read a null array.
     */
    private static final class VoidNames {
        static final java.util.Set<String> SET = buildVoidReturning();
    }

    private static java.util.Set<String> buildVoidReturning() {
        java.util.Set<String> all = new java.util.HashSet<String>();
        java.util.Set<String> notVoid = new java.util.HashSet<String>();
        for (String[] sig : SIGNATURES) {
            if ("2278".equals(sig[1])) {
                all.add(sig[0].toLowerCase(java.util.Locale.ROOT));
            } else {
                notVoid.add(sig[0].toLowerCase(java.util.Locale.ROOT));
            }
        }
        all.removeAll(notVoid);
        return java.util.Collections.unmodifiableSet(all);
    }

    /**
     * Whether every signature of this name returns void. A void result is not a NULL: PostgreSQL
     * sends an empty value of type void, so a client reading the column back gets an empty string
     * and {@code IS NULL} is false. Reading the answer off the signature table keeps the two from
     * drifting apart as functions are added.
     */
    static boolean returnsVoid(String name) {
        return name != null && VoidNames.SET.contains(name.toLowerCase(java.util.Locale.ROOT));
    }

    static final String[][] SIGNATURES = {
            {"_pg_expandarray", "2249", "2277", "ti", "1*"},
            {"abbrev", "25", "650", "fi", "1"},
            {"abbrev", "25", "869", "fi", "1"},
            {"abs", "1700", "1700", "fi", "1"},
            {"abs", "20", "20", "fi", "1"},
            {"abs", "21", "21", "fi", "1"},
            {"abs", "23", "23", "fi", "1"},
            {"abs", "700", "700", "fi", "1"},
            {"abs", "701", "701", "fi", "1"},
            {"acldefault", "1034", "18 26", "fi", "2"},
            {"aclexplode", "2249", "1034", "ts", "1"},
            {"acos", "701", "701", "fi", "1"},
            {"acosd", "701", "701", "fi", "1"},
            {"acosh", "701", "701", "fi", "1"},
            {"age", "1186", "1114", "fs", "1"},
            {"age", "1186", "1184", "fs", "1"},
            {"age", "23", "28", "fs", "1"},
            {"age", "1186", "1114 1114", "fi", "2"},
            {"age", "1186", "1184 1184", "fi", "2"},
            {"akeys", "1009", "90001", "fi", "1*"},
            {"area", "701", "602", "fi", "1"},
            {"area", "701", "603", "fi", "1"},
            {"area", "701", "718", "fi", "1"},
            {"array_append", "5078", "5078 5077", "fi", "2"},
            {"array_cat", "5078", "5078 5078", "fi", "2"},
            {"array_dims", "25", "2277", "fi", "1"},
            {"array_fill", "2277", "2283 1007", "fi", "2"},
            {"array_fill", "2277", "2283 1007 1007", "fi", "3"},
            {"array_length", "23", "2277 23", "fi", "2"},
            {"array_lower", "23", "2277 23", "fi", "2"},
            {"array_ndims", "23", "2277", "fi", "1"},
            {"array_position", "23", "5078 5077", "fi", "2"},
            {"array_position", "23", "5078 5077 23", "fi", "3"},
            {"array_positions", "1007", "5078 5077", "fi", "2"},
            {"array_prepend", "5078", "5077 5078", "fi", "2"},
            {"array_remove", "5078", "5078 5077", "fi", "2"},
            {"array_replace", "5078", "5078 5077 5077", "fi", "3"},
            {"array_reverse", "2277", "2277", "fi", "1"},
            {"array_sample", "2277", "2277 23", "fv", "2"},
            {"array_shuffle", "2277", "2277", "fv", "1"},
            {"array_sort", "2277", "2277", "fi", "1"},
            {"array_sort", "2277", "2277 16", "fi", "2"},
            {"array_sort", "2277", "2277 16 16", "fi", "3"},
            {"array_to_string", "25", "2277 25", "fs", "2"},
            {"array_to_string", "25", "2277 25 25", "fs", "3"},
            {"array_to_tsvector", "3614", "1009", "fi", "1"},
            {"array_upper", "23", "2277 23", "fi", "2"},
            {"arraycontained", "16", "2277 2277", "fi", "2"},
            {"arraycontains", "16", "2277 2277", "fi", "2"},
            {"arrayoverlap", "16", "2277 2277", "fi", "2"},
            {"ascii", "23", "25", "fi", "1"},
            {"asin", "701", "701", "fi", "1"},
            {"asind", "701", "701", "fi", "1"},
            {"asinh", "701", "701", "fi", "1"},
            {"atan", "701", "701", "fi", "1"},
            {"atan2", "701", "701 701", "fi", "2"},
            {"atan2d", "701", "701 701", "fi", "2"},
            {"atand", "701", "701", "fi", "1"},
            {"atanh", "701", "701", "fi", "1"},
            {"avals", "1009", "90001", "fi", "1*"},
            {"bit", "1560", "20 23", "fi", "2"},
            {"bit", "1560", "23 23", "fi", "2"},
            {"bit", "1560", "1560 23 16", "fi", "3"},
            {"bit_count", "20", "1560", "fi", "1"},
            {"bit_count", "20", "17", "fi", "1"},
            {"bit_length", "23", "1560", "fi", "1"},
            {"bit_length", "23", "17", "fi", "1"},
            {"bit_length", "23", "25", "fi", "1"},
            {"bool", "16", "23", "fi", "1"},
            {"bool", "16", "3802", "fi", "1"},
            {"bound_box", "603", "603 603", "fi", "2"},
            // memgres's own: PostgreSQL bounds a path or a polygon through its box() cast rather
            // than through bound_box, and a form recorded nowhere is a form no arity rule allows.
            {"bound_box", "603", "602", "fi", "1*"},
            {"bound_box", "603", "604", "fi", "1*"},
            {"box", "603", "600", "fi", "1"},
            {"box", "603", "604", "fi", "1"},
            {"box", "603", "718", "fi", "1"},
            {"box", "603", "600 600", "fi", "2"},
            {"bpchar", "1042", "18", "fi", "1"},
            {"bpchar", "1042", "19", "fi", "1"},
            {"bpchar", "1042", "1042 23 16", "fi", "3"},
            {"broadcast", "869", "869", "fi", "1"},
            {"btrim", "25", "25", "fi", "1"},
            {"btrim", "17", "17 17", "fi", "2"},
            {"btrim", "25", "25 25", "fi", "2"},
            {"bytea", "17", "20", "fi", "1"},
            {"bytea", "17", "21", "fi", "1"},
            {"bytea", "17", "23", "fi", "1"},
            {"cardinality", "23", "2277", "fi", "1"},
            {"casefold", "25", "25", "fi", "1"},
            {"cbrt", "701", "701", "fi", "1"},
            {"ceil", "1700", "1700", "fi", "1"},
            {"ceil", "701", "701", "fi", "1"},
            {"ceiling", "1700", "1700", "fi", "1"},
            {"ceiling", "701", "701", "fi", "1"},
            {"center", "600", "603", "fi", "1"},
            {"center", "600", "718", "fi", "1"},
            {"char", "18", "23", "fi", "1"},
            {"char", "18", "25", "fi", "1"},
            {"char_length", "23", "1042", "fi", "1"},
            {"char_length", "23", "25", "fi", "1"},
            {"character_length", "23", "1042", "fi", "1"},
            {"character_length", "23", "25", "fi", "1"},
            {"chr", "25", "23", "fi", "1"},
            {"cidr", "650", "869", "fi", "1"},
            {"circle", "718", "603", "fi", "1"},
            {"circle", "718", "604", "fi", "1"},
            {"circle", "718", "600 701", "fi", "2"},
            {"clock_timestamp", "1184", "", "fv", "0"},
            {"col_description", "25", "26 23", "fs", "2"},
            {"concat", "25", "2276", "fs", "1+"},
            {"concat_ws", "25", "25 2276", "fs", "2+"},
            {"convert", "17", "17 19 19", "fs", "3"},
            {"convert_from", "25", "17 19", "fs", "2"},
            {"convert_to", "17", "25 19", "fs", "2"},
            {"cos", "701", "701", "fi", "1"},
            {"cosd", "701", "701", "fi", "1"},
            {"cosh", "701", "701", "fi", "1"},
            {"cot", "701", "701", "fi", "1"},
            {"cotd", "701", "701", "fi", "1"},
            {"crc32", "20", "17", "fi", "1"},
            {"crc32c", "20", "17", "fi", "1"},
            {"current_database", "19", "", "fs", "0"},
            {"current_query", "25", "", "fv", "0"},
            {"current_schema", "19", "", "fs", "0"},
            {"current_schemas", "1003", "16", "fs", "1"},
            {"current_setting", "25", "25", "fs", "1"},
            {"current_setting", "25", "25 16", "fs", "2"},
            {"current_user", "19", "", "fs", "0"},
            {"currval", "20", "2205", "fv", "1"},
            {"database_to_xml", "142", "16 16 25", "fs", "3"},
            {"date", "1082", "1114", "fi", "1"},
            {"date", "1082", "1184", "fs", "1"},
            {"date_bin", "1114", "1186 1114 1114", "fi", "3"},
            {"date_bin", "1184", "1186 1184 1184", "fi", "3"},
            {"date_part", "701", "25 1082", "fi", "2"},
            {"date_part", "701", "25 1083", "fi", "2"},
            {"date_part", "701", "25 1114", "fi", "2"},
            {"date_part", "701", "25 1184", "fs", "2"},
            {"date_part", "701", "25 1186", "fi", "2"},
            {"date_part", "701", "25 1266", "fi", "2"},
            {"date_trunc", "1114", "25 1114", "fi", "2"},
            {"date_trunc", "1184", "25 1184", "fs", "2"},
            {"date_trunc", "1186", "25 1186", "fi", "2"},
            {"date_trunc", "1184", "25 1184 25", "fi", "3"},
            {"datemultirange", "4535", "", "fi", "0"},
            {"datemultirange", "4535", "3912", "fi", "1"},
            {"datemultirange", "4535", "3913", "fi", "1+"},
            {"daterange", "3912", "1082 1082", "fi", "2"},
            {"daterange", "3912", "1082 1082 25", "fi", "3"},
            {"decode", "17", "25 25", "fi", "2"},
            {"defined", "16", "90001 25", "fi", "2*"},
            {"degrees", "701", "701", "fi", "1"},
            {"delete", "90001", "90001 25", "fi", "2*"},
            {"delete", "90001", "90001 1009", "fi", "2*"},
            {"delete", "90001", "90001 90001", "fi", "2*"},
            {"diagonal", "601", "603", "fi", "1"},
            {"diameter", "701", "718", "fi", "1"},
            {"digest", "17", "25 25", "fi", "2*"},
            {"div", "1700", "1700 1700", "fi", "2"},
            {"each", "2249", "90001", "ti", "1*"},
            {"encode", "25", "17 25", "fi", "2"},
            {"enum_cmp", "23", "3500 3500", "fi", "2"},
            {"enum_first", "3500", "3500", "fs", "1"},
            {"enum_last", "3500", "3500", "fs", "1"},
            {"enum_range", "2277", "3500", "fs", "1"},
            {"enum_range", "2277", "3500 3500", "fs", "2"},
            {"exist", "16", "90001 25", "fi", "2*"},
            {"exp", "1700", "1700", "fi", "1"},
            {"exp", "701", "701", "fi", "1"},
            {"extract", "1700", "25 1082", "fi", "2"},
            {"extract", "1700", "25 1083", "fi", "2"},
            {"extract", "1700", "25 1114", "fi", "2"},
            {"extract", "1700", "25 1184", "fs", "2"},
            {"extract", "1700", "25 1186", "fi", "2"},
            {"extract", "1700", "25 1266", "fi", "2"},
            {"factorial", "1700", "20", "fi", "1"},
            {"family", "23", "869", "fi", "1"},
            {"float4", "700", "1700", "fi", "1"},
            {"float4", "700", "20", "fi", "1"},
            {"float4", "700", "21", "fi", "1"},
            {"float4", "700", "23", "fi", "1"},
            {"float4", "700", "3802", "fi", "1"},
            {"float4", "700", "701", "fi", "1"},
            {"float8", "701", "1700", "fi", "1"},
            {"float8", "701", "20", "fi", "1"},
            {"float8", "701", "21", "fi", "1"},
            {"float8", "701", "23", "fi", "1"},
            {"float8", "701", "3802", "fi", "1"},
            {"float8", "701", "700", "fi", "1"},
            {"floor", "1700", "1700", "fi", "1"},
            {"floor", "701", "701", "fi", "1"},
            {"format", "25", "25", "fs", "1"},
            {"format", "25", "25 2276", "fs", "2+"},
            {"format_type", "25", "26 23", "fs", "2"},
            {"gamma", "701", "701", "fi", "1"},
            {"gcd", "1700", "1700 1700", "fi", "2"},
            {"gcd", "20", "20 20", "fi", "2"},
            {"gcd", "23", "23 23", "fi", "2"},
            {"gen_random_bytes", "17", "23", "fv", "1*"},
            {"gen_random_uuid", "2950", "", "fv", "0"},
            {"gen_salt", "25", "25", "fv", "1*"},
            {"generate_series", "1700", "1700 1700", "ti", "2"},
            {"generate_series", "20", "20 20", "ti", "2"},
            {"generate_series", "23", "23 23", "ti", "2"},
            {"generate_series", "1114", "1114 1114 1186", "ti", "3"},
            {"generate_series", "1184", "1184 1184 1186", "ts", "3"},
            {"generate_series", "1700", "1700 1700 1700", "ti", "3"},
            {"generate_series", "20", "20 20 20", "ti", "3"},
            {"generate_series", "23", "23 23 23", "ti", "3"},
            {"generate_series", "1184", "1184 1184 1186 25", "ti", "4"},
            {"generate_subscripts", "23", "2277 23", "ti", "2"},
            {"generate_subscripts", "23", "2277 23 16", "ti", "3"},
            {"get_bit", "23", "1560 23", "fi", "2"},
            {"get_bit", "23", "17 20", "fi", "2"},
            {"get_byte", "23", "17 23", "fi", "2"},
            {"get_current_ts_config", "3734", "", "fs", "0"},
            {"has_any_column_privilege", "16", "25 25", "fs", "2"},
            {"has_any_column_privilege", "16", "26 25", "fs", "2"},
            {"has_any_column_privilege", "16", "19 25 25", "fs", "3"},
            {"has_any_column_privilege", "16", "19 26 25", "fs", "3"},
            {"has_any_column_privilege", "16", "26 25 25", "fs", "3"},
            {"has_any_column_privilege", "16", "26 26 25", "fs", "3"},
            {"has_column_privilege", "16", "25 21 25", "fs", "3"},
            {"has_column_privilege", "16", "25 25 25", "fs", "3"},
            {"has_column_privilege", "16", "26 21 25", "fs", "3"},
            {"has_column_privilege", "16", "26 25 25", "fs", "3"},
            {"has_column_privilege", "16", "19 25 21 25", "fs", "4"},
            {"has_column_privilege", "16", "19 25 25 25", "fs", "4"},
            {"has_column_privilege", "16", "19 26 21 25", "fs", "4"},
            {"has_column_privilege", "16", "19 26 25 25", "fs", "4"},
            {"has_column_privilege", "16", "26 25 21 25", "fs", "4"},
            {"has_column_privilege", "16", "26 25 25 25", "fs", "4"},
            {"has_column_privilege", "16", "26 26 21 25", "fs", "4"},
            {"has_column_privilege", "16", "26 26 25 25", "fs", "4"},
            {"has_database_privilege", "16", "25 25", "fs", "2"},
            {"has_database_privilege", "16", "26 25", "fs", "2"},
            {"has_database_privilege", "16", "19 25 25", "fs", "3"},
            {"has_database_privilege", "16", "19 26 25", "fs", "3"},
            {"has_database_privilege", "16", "26 25 25", "fs", "3"},
            {"has_database_privilege", "16", "26 26 25", "fs", "3"},
            {"has_foreign_data_wrapper_privilege", "16", "25 25", "fs", "2"},
            {"has_foreign_data_wrapper_privilege", "16", "26 25", "fs", "2"},
            {"has_foreign_data_wrapper_privilege", "16", "19 25 25", "fs", "3"},
            {"has_foreign_data_wrapper_privilege", "16", "19 26 25", "fs", "3"},
            {"has_foreign_data_wrapper_privilege", "16", "26 25 25", "fs", "3"},
            {"has_foreign_data_wrapper_privilege", "16", "26 26 25", "fs", "3"},
            {"has_function_privilege", "16", "25 25", "fs", "2"},
            {"has_function_privilege", "16", "26 25", "fs", "2"},
            {"has_function_privilege", "16", "19 25 25", "fs", "3"},
            {"has_function_privilege", "16", "19 26 25", "fs", "3"},
            {"has_function_privilege", "16", "26 25 25", "fs", "3"},
            {"has_function_privilege", "16", "26 26 25", "fs", "3"},
            {"has_language_privilege", "16", "25 25", "fs", "2"},
            {"has_language_privilege", "16", "26 25", "fs", "2"},
            {"has_language_privilege", "16", "19 25 25", "fs", "3"},
            {"has_language_privilege", "16", "19 26 25", "fs", "3"},
            {"has_language_privilege", "16", "26 25 25", "fs", "3"},
            {"has_language_privilege", "16", "26 26 25", "fs", "3"},
            {"has_largeobject_privilege", "16", "26 25", "fs", "2"},
            {"has_largeobject_privilege", "16", "19 26 25", "fs", "3"},
            {"has_largeobject_privilege", "16", "26 26 25", "fs", "3"},
            {"has_parameter_privilege", "16", "25 25", "fs", "2"},
            {"has_parameter_privilege", "16", "19 25 25", "fs", "3"},
            {"has_parameter_privilege", "16", "26 25 25", "fs", "3"},
            {"has_schema_privilege", "16", "25 25", "fs", "2"},
            {"has_schema_privilege", "16", "26 25", "fs", "2"},
            {"has_schema_privilege", "16", "19 25 25", "fs", "3"},
            {"has_schema_privilege", "16", "19 26 25", "fs", "3"},
            {"has_schema_privilege", "16", "26 25 25", "fs", "3"},
            {"has_schema_privilege", "16", "26 26 25", "fs", "3"},
            {"has_sequence_privilege", "16", "25 25", "fs", "2"},
            {"has_sequence_privilege", "16", "26 25", "fs", "2"},
            {"has_sequence_privilege", "16", "19 25 25", "fs", "3"},
            {"has_sequence_privilege", "16", "19 26 25", "fs", "3"},
            {"has_sequence_privilege", "16", "26 25 25", "fs", "3"},
            {"has_sequence_privilege", "16", "26 26 25", "fs", "3"},
            {"has_server_privilege", "16", "25 25", "fs", "2"},
            {"has_server_privilege", "16", "26 25", "fs", "2"},
            {"has_server_privilege", "16", "19 25 25", "fs", "3"},
            {"has_server_privilege", "16", "19 26 25", "fs", "3"},
            {"has_server_privilege", "16", "26 25 25", "fs", "3"},
            {"has_server_privilege", "16", "26 26 25", "fs", "3"},
            {"has_table_privilege", "16", "25 25", "fs", "2"},
            {"has_table_privilege", "16", "26 25", "fs", "2"},
            {"has_table_privilege", "16", "19 25 25", "fs", "3"},
            {"has_table_privilege", "16", "19 26 25", "fs", "3"},
            {"has_table_privilege", "16", "26 25 25", "fs", "3"},
            {"has_table_privilege", "16", "26 26 25", "fs", "3"},
            {"has_tablespace_privilege", "16", "25 25", "fs", "2"},
            {"has_tablespace_privilege", "16", "26 25", "fs", "2"},
            {"has_tablespace_privilege", "16", "19 25 25", "fs", "3"},
            {"has_tablespace_privilege", "16", "19 26 25", "fs", "3"},
            {"has_tablespace_privilege", "16", "26 25 25", "fs", "3"},
            {"has_tablespace_privilege", "16", "26 26 25", "fs", "3"},
            {"has_type_privilege", "16", "25 25", "fs", "2"},
            {"has_type_privilege", "16", "26 25", "fs", "2"},
            {"has_type_privilege", "16", "19 25 25", "fs", "3"},
            {"has_type_privilege", "16", "19 26 25", "fs", "3"},
            {"has_type_privilege", "16", "26 25 25", "fs", "3"},
            {"has_type_privilege", "16", "26 26 25", "fs", "3"},
            {"height", "701", "603", "fi", "1"},
            {"hmac", "17", "25 25 25", "fi", "3*"},
            {"host", "25", "869", "fi", "1"},
            {"hostmask", "869", "869", "fi", "1"},
            {"hstore", "90001", "1009", "fi", "1*"},
            {"hstore", "90001", "2249", "fi", "1*"},
            {"hstore", "90001", "25 25", "fi", "2*"},
            {"hstore", "90001", "1009 1009", "fi", "2*"},
            {"hstore_to_array", "1009", "90001", "fi", "1*"},
            {"hstore_to_json", "114", "90001", "fi", "1*"},
            {"hstore_to_json_loose", "114", "90001", "fi", "1*"},
            {"hstore_to_jsonb", "3802", "90001", "fi", "1*"},
            {"hstore_to_jsonb_loose", "3802", "90001", "fi", "1*"},
            {"hstore_to_matrix", "1009", "90001", "fi", "1*"},
            {"icu_unicode_version", "25", "", "fi", "0"},
            {"inet_client_addr", "869", "", "fs", "0"},
            {"inet_client_port", "23", "", "fs", "0"},
            {"inet_merge", "650", "869 869", "fi", "2"},
            {"inet_same_family", "16", "869 869", "fi", "2"},
            {"inet_server_addr", "869", "", "fs", "0"},
            {"inet_server_port", "23", "", "fs", "0"},
            {"initcap", "25", "25", "fi", "1"},
            {"int2", "21", "17", "fi", "1"},
            {"int2", "21", "1700", "fi", "1"},
            {"int2", "21", "20", "fi", "1"},
            {"int2", "21", "23", "fi", "1"},
            {"int2", "21", "3802", "fi", "1"},
            {"int2", "21", "700", "fi", "1"},
            {"int2", "21", "701", "fi", "1"},
            {"int4", "23", "1560", "fi", "1"},
            {"int4", "23", "16", "fi", "1"},
            {"int4", "23", "17", "fi", "1"},
            {"int4", "23", "1700", "fi", "1"},
            {"int4", "23", "18", "fi", "1"},
            {"int4", "23", "20", "fi", "1"},
            {"int4", "23", "21", "fi", "1"},
            {"int4", "23", "3802", "fi", "1"},
            {"int4", "23", "700", "fi", "1"},
            {"int4", "23", "701", "fi", "1"},
            {"int4multirange", "4451", "", "fi", "0"},
            {"int4multirange", "4451", "3904", "fi", "1"},
            {"int4multirange", "4451", "3905", "fi", "1+"},
            {"int4range", "3904", "23 23", "fi", "2"},
            {"int4range", "3904", "23 23 25", "fi", "3"},
            {"int8", "20", "1560", "fi", "1"},
            {"int8", "20", "17", "fi", "1"},
            {"int8", "20", "1700", "fi", "1"},
            {"int8", "20", "21", "fi", "1"},
            {"int8", "20", "23", "fi", "1"},
            {"int8", "20", "26", "fi", "1"},
            {"int8", "20", "3802", "fi", "1"},
            {"int8", "20", "700", "fi", "1"},
            {"int8", "20", "701", "fi", "1"},
            {"int8multirange", "4536", "", "fi", "0"},
            {"int8multirange", "4536", "3926", "fi", "1"},
            {"int8multirange", "4536", "3927", "fi", "1+"},
            {"int8range", "3926", "20 20", "fi", "2"},
            {"int8range", "3926", "20 20 25", "fi", "3"},
            {"interval", "1186", "1083", "fi", "1"},
            {"interval", "1186", "1186 23", "fi", "2"},
            {"isclosed", "16", "602", "fi", "1"},
            {"isdefined", "16", "90001 25", "fi", "2*"},
            {"isempty", "16", "3831", "fi", "1"},
            {"isempty", "16", "4537", "fi", "1"},
            {"isexists", "16", "90001 25", "fi", "2*"},
            {"isfinite", "16", "1082", "fi", "1"},
            {"isfinite", "16", "1114", "fi", "1"},
            {"isfinite", "16", "1184", "fi", "1"},
            {"isfinite", "16", "1186", "fi", "1"},
            {"isopen", "16", "602", "fi", "1"},
            {"json_array_elements", "114", "114", "ti", "1"},
            {"json_array_elements_text", "25", "114", "ti", "1"},
            {"json_array_length", "23", "114", "fi", "1"},
            {"json_build_array", "114", "", "fs", "0"},
            {"json_build_array", "114", "2276", "fs", "1+"},
            {"json_build_object", "114", "", "fs", "0"},
            {"json_build_object", "114", "2276", "fs", "1+"},
            {"json_each", "2249", "114", "ti", "1"},
            {"json_each_text", "2249", "114", "ti", "1"},
            {"json_extract_path", "114", "114 1009", "fi", "2+"},
            {"json_extract_path_text", "25", "114 1009", "fi", "2+"},
            {"json_object", "114", "1009", "fi", "1"},
            {"json_object", "114", "1009 1009", "fi", "2"},
            {"json_object_keys", "25", "114", "ti", "1"},
            {"json_populate_record", "2283", "2283 114 16", "fs", "2"},
            {"json_populate_recordset", "2283", "2283 114 16", "ts", "2"},
            {"json_strip_nulls", "114", "114 16", "fs", "1"},
            {"json_to_record", "2249", "114", "fs", "1"},
            {"json_to_recordset", "2249", "114", "ts", "1"},
            {"json_typeof", "25", "114", "fi", "1"},
            {"jsonb_array_elements", "3802", "3802", "ti", "1"},
            {"jsonb_array_elements_text", "25", "3802", "ti", "1"},
            {"jsonb_array_length", "23", "3802", "fi", "1"},
            {"jsonb_build_array", "3802", "", "fs", "0"},
            {"jsonb_build_array", "3802", "2276", "fs", "1+"},
            {"jsonb_build_object", "3802", "", "fs", "0"},
            {"jsonb_build_object", "3802", "2276", "fs", "1+"},
            {"jsonb_each", "2249", "3802", "ti", "1"},
            {"jsonb_each_text", "2249", "3802", "ti", "1"},
            {"jsonb_exists", "16", "3802 25", "fi", "2"},
            {"jsonb_exists_all", "16", "3802 1009", "fi", "2"},
            {"jsonb_exists_any", "16", "3802 1009", "fi", "2"},
            {"jsonb_extract_path", "3802", "3802 1009", "fi", "2+"},
            {"jsonb_extract_path_text", "25", "3802 1009", "fi", "2+"},
            {"jsonb_insert", "3802", "3802 1009 3802 16", "fi", "3"},
            {"jsonb_object", "3802", "1009", "fi", "1"},
            {"jsonb_object", "3802", "1009 1009", "fi", "2"},
            {"jsonb_object_keys", "25", "3802", "ti", "1"},
            {"jsonb_path_exists", "16", "3802 4072 3802 16", "fi", "2"},
            {"jsonb_path_exists_tz", "16", "3802 4072 3802 16", "fs", "2"},
            {"jsonb_path_match", "16", "3802 4072 3802 16", "fi", "2"},
            {"jsonb_path_match_tz", "16", "3802 4072 3802 16", "fs", "2"},
            {"jsonb_path_query", "3802", "3802 4072 3802 16", "ti", "2"},
            {"jsonb_path_query_array", "3802", "3802 4072 3802 16", "fi", "2"},
            {"jsonb_path_query_array_tz", "3802", "3802 4072 3802 16", "fs", "2"},
            {"jsonb_path_query_first", "3802", "3802 4072 3802 16", "fi", "2"},
            {"jsonb_path_query_first_tz", "3802", "3802 4072 3802 16", "fs", "2"},
            {"jsonb_path_query_tz", "3802", "3802 4072 3802 16", "ts", "2"},
            {"jsonb_populate_record", "2283", "2283 3802", "fs", "2"},
            {"jsonb_populate_recordset", "2283", "2283 3802", "ts", "2"},
            {"jsonb_pretty", "25", "3802", "fi", "1"},
            {"jsonb_set", "3802", "3802 1009 3802 16", "fi", "3"},
            {"jsonb_set_lax", "3802", "3802 1009 3802 16 25", "fi", "3"},
            {"jsonb_strip_nulls", "3802", "3802 16", "fs", "1"},
            {"jsonb_to_record", "2249", "3802", "fs", "1"},
            {"jsonb_to_recordset", "2249", "3802", "ts", "1"},
            {"jsonb_typeof", "25", "3802", "fi", "1"},
            {"justify_days", "1186", "1186", "fi", "1"},
            {"justify_hours", "1186", "1186", "fi", "1"},
            {"justify_interval", "1186", "1186", "fi", "1"},
            {"lastval", "20", "", "fv", "0"},
            {"lcm", "1700", "1700 1700", "fi", "2"},
            {"lcm", "20", "20 20", "fi", "2"},
            {"lcm", "23", "23 23", "fi", "2"},
            {"left", "25", "25 23", "fi", "2"},
            {"lgamma", "701", "701", "fi", "1"},
            {"length", "23", "1042", "fi", "1"},
            {"length", "23", "1560", "fi", "1"},
            {"length", "23", "17", "fi", "1"},
            {"length", "23", "25", "fi", "1"},
            {"length", "23", "3614", "fi", "1"},
            {"length", "701", "601", "fi", "1"},
            {"length", "701", "602", "fi", "1"},
            {"length", "23", "17 19", "fs", "2"},
            {"levenshtein", "23", "25 25", "fi", "2*"},
            // fuzzystrmatch also declares the form that costs an insert, a delete and a
            // substitution differently, which is a form the arity rule has to know about.
            {"levenshtein", "23", "25 25 23 23 23", "fi", "5*"},
            {"line", "628", "600 600", "fi", "2"},
            {"ln", "1700", "1700", "fi", "1"},
            {"ln", "701", "701", "fi", "1"},
            {"lo_close", "23", "23", "fv", "1"},
            {"lo_creat", "26", "23", "fv", "1"},
            {"lo_create", "26", "26", "fv", "1"},
            {"lo_export", "23", "26 25", "fv", "2"},
            {"lo_from_bytea", "26", "26 17", "fv", "2"},
            {"lo_get", "17", "26", "fv", "1"},
            {"lo_get", "17", "26 20 23", "fv", "3"},
            {"lo_import", "26", "25", "fv", "1"},
            {"lo_import", "26", "25 26", "fv", "2"},
            {"lo_lseek", "23", "23 23 23", "fv", "3"},
            {"lo_open", "23", "26 23", "fv", "2"},
            {"lo_put", "2278", "26 20 17", "fv", "3"},
            {"lo_tell", "23", "23", "fv", "1"},
            {"lo_truncate", "23", "23 23", "fv", "2"},
            {"lo_unlink", "23", "26", "fv", "1"},
            {"log", "1700", "1700", "fi", "1"},
            {"log", "701", "701", "fi", "1"},
            {"log", "1700", "1700 1700", "fi", "2"},
            {"log10", "1700", "1700", "fi", "1"},
            {"log10", "701", "701", "fi", "1"},
            {"loread", "17", "23 23", "fv", "2"},
            {"lower", "25", "25", "fi", "1"},
            {"lower", "2283", "3831", "fi", "1"},
            {"lower", "2283", "4537", "fi", "1"},
            {"lower_inc", "16", "3831", "fi", "1"},
            {"lower_inc", "16", "4537", "fi", "1"},
            {"lower_inf", "16", "3831", "fi", "1"},
            {"lower_inf", "16", "4537", "fi", "1"},
            {"lowrite", "23", "23 17", "fv", "2"},
            {"lpad", "25", "25 23", "fi", "2"},
            {"lpad", "25", "25 23 25", "fi", "3"},
            {"lseg", "601", "603", "fi", "1"},
            {"lseg", "601", "600 600", "fi", "2"},
            {"ltrim", "25", "25", "fi", "1"},
            {"ltrim", "17", "17 17", "fi", "2"},
            {"ltrim", "25", "25 25", "fi", "2"},
            {"macaddr", "829", "774", "fi", "1"},
            {"macaddr8", "774", "829", "fi", "1"},
            {"macaddr8_set7bit", "774", "774", "fi", "1"},
            {"make_date", "1082", "23 23 23", "fi", "3"},
            {"make_interval", "1186", "23 23 23 23 23 23 701", "fi", "0"},
            {"make_time", "1083", "23 23 701", "fi", "3"},
            {"make_timestamp", "1114", "23 23 23 23 23 701", "fi", "6"},
            {"make_timestamptz", "1184", "23 23 23 23 23 701", "fs", "6"},
            {"make_timestamptz", "1184", "23 23 23 23 23 701 25", "fs", "7"},
            {"masklen", "23", "869", "fi", "1"},
            {"md5", "25", "17", "fi", "1"},
            {"md5", "25", "25", "fi", "1"},
            {"min_scale", "23", "1700", "fi", "1"},
            {"mod", "1700", "1700 1700", "fi", "2"},
            {"mod", "20", "20 20", "fi", "2"},
            {"mod", "21", "21 21", "fi", "2"},
            {"mod", "23", "23 23", "fi", "2"},
            {"multirange", "4537", "3831", "fi", "1"},
            {"name", "19", "1042", "fi", "1"},
            {"name", "19", "1043", "fi", "1"},
            {"name", "19", "25", "fi", "1"},
            {"netmask", "869", "869", "fi", "1"},
            {"network", "650", "869", "fi", "1"},
            {"nextval", "20", "2205", "fv", "1"},
            {"normalize", "25", "25 25", "fi", "1"},
            {"now", "1184", "", "fs", "0"},
            {"npoints", "23", "602", "fi", "1"},
            {"npoints", "23", "604", "fi", "1"},
            {"num_nonnulls", "23", "2276", "fi", "1+"},
            {"num_nulls", "23", "2276", "fi", "1+"},
            {"numeric", "1700", "20", "fi", "1"},
            {"numeric", "1700", "21", "fi", "1"},
            {"numeric", "1700", "23", "fi", "1"},
            {"numeric", "1700", "3802", "fi", "1"},
            {"numeric", "1700", "700", "fi", "1"},
            {"numeric", "1700", "701", "fi", "1"},
            {"numeric", "1700", "790", "fs", "1"},
            {"numeric", "1700", "1700 23", "fi", "2"},
            {"nummultirange", "4532", "", "fi", "0"},
            {"nummultirange", "4532", "3906", "fi", "1"},
            {"nummultirange", "4532", "3907", "fi", "1+"},
            {"numnode", "23", "3615", "fi", "1"},
            {"numrange", "3906", "1700 1700", "fi", "2"},
            {"numrange", "3906", "1700 1700 25", "fi", "3"},
            {"obj_description", "25", "26", "fs", "1"},
            {"obj_description", "25", "26 19", "fs", "2"},
            {"octet_length", "23", "1042", "fi", "1"},
            {"octet_length", "23", "1560", "fi", "1"},
            {"octet_length", "23", "17", "fi", "1"},
            {"octet_length", "23", "25", "fi", "1"},
            {"oid", "26", "20", "fi", "1"},
            {"overlaps", "16", "1083 1083 1083 1083", "fi", "4"},
            {"overlaps", "16", "1083 1083 1083 1186", "fi", "4"},
            {"overlaps", "16", "1083 1186 1083 1083", "fi", "4"},
            {"overlaps", "16", "1083 1186 1083 1186", "fi", "4"},
            {"overlaps", "16", "1114 1114 1114 1114", "fi", "4"},
            {"overlaps", "16", "1114 1114 1114 1186", "fi", "4"},
            {"overlaps", "16", "1114 1186 1114 1114", "fi", "4"},
            {"overlaps", "16", "1114 1186 1114 1186", "fi", "4"},
            {"overlaps", "16", "1184 1184 1184 1184", "fi", "4"},
            {"overlaps", "16", "1184 1184 1184 1186", "fs", "4"},
            {"overlaps", "16", "1184 1186 1184 1184", "fs", "4"},
            {"overlaps", "16", "1184 1186 1184 1186", "fs", "4"},
            {"overlaps", "16", "1266 1266 1266 1266", "fi", "4"},
            {"overlay", "1560", "1560 1560 23", "fi", "3"},
            {"overlay", "17", "17 17 23", "fi", "3"},
            {"overlay", "25", "25 25 23", "fi", "3"},
            {"overlay", "1560", "1560 1560 23 23", "fi", "4"},
            {"overlay", "17", "17 17 23 23", "fi", "4"},
            {"overlay", "25", "25 25 23 23", "fi", "4"},
            {"parse_ident", "1009", "25 16", "fi", "1"},
            {"path", "602", "604", "fi", "1"},
            {"pclose", "602", "602", "fi", "1"},
            {"pg_advisory_lock", "2278", "20", "fv", "1"},
            {"pg_advisory_lock", "2278", "23 23", "fv", "2"},
            {"pg_advisory_lock_shared", "2278", "20", "fv", "1"},
            {"pg_advisory_lock_shared", "2278", "23 23", "fv", "2"},
            {"pg_advisory_unlock", "16", "20", "fv", "1"},
            {"pg_advisory_unlock", "16", "23 23", "fv", "2"},
            {"pg_advisory_unlock_all", "2278", "", "fv", "0"},
            {"pg_advisory_unlock_shared", "16", "20", "fv", "1"},
            {"pg_advisory_unlock_shared", "16", "23 23", "fv", "2"},
            {"pg_advisory_xact_lock", "2278", "20", "fv", "1"},
            {"pg_advisory_xact_lock", "2278", "23 23", "fv", "2"},
            {"pg_advisory_xact_lock_shared", "2278", "20", "fv", "1"},
            {"pg_advisory_xact_lock_shared", "2278", "23 23", "fv", "2"},
            {"pg_available_extension_versions", "2249", "", "ts", "0"},
            {"pg_backend_pid", "23", "", "fs", "0"},
            {"pg_backup_start", "3220", "25 16", "fv", "1"},
            {"pg_backup_stop", "2249", "16", "fv", "0"},
            {"pg_blocking_pids", "1007", "23", "fv", "1"},
            {"pg_cancel_backend", "16", "23", "fv", "1"},
            {"pg_client_encoding", "19", "", "fs", "0"},
            {"pg_collation_is_visible", "16", "26", "fs", "1"},
            {"pg_column_is_updatable", "16", "2205 21 16", "fs", "3"},
            {"pg_column_size", "23", "2276", "fs", "1"},
            {"pg_column_toast_chunk_id", "26", "2276", "fs", "1"},
            {"pg_conf_load_time", "1184", "", "fs", "0"},
            {"pg_conversion_is_visible", "16", "26", "fs", "1"},
            {"pg_create_logical_replication_slot", "2249", "19 19 16 16 16", "fv", "2"},
            {"pg_create_physical_replication_slot", "2249", "19 16 16", "fv", "1"},
            {"pg_create_restore_point", "3220", "25", "fv", "1"},
            {"pg_current_logfile", "25", "", "fv", "0"},
            {"pg_current_logfile", "25", "25", "fv", "1"},
            {"pg_current_snapshot", "5038", "", "fs", "0"},
            {"pg_current_wal_flush_lsn", "3220", "", "fv", "0"},
            {"pg_current_wal_insert_lsn", "3220", "", "fv", "0"},
            {"pg_current_wal_lsn", "3220", "", "fv", "0"},
            {"pg_current_xact_id", "5069", "", "fs", "0"},
            {"pg_current_xact_id_if_assigned", "5069", "", "fs", "0"},
            {"pg_database_size", "20", "19", "fv", "1"},
            {"pg_database_size", "20", "26", "fv", "1"},
            {"pg_describe_object", "25", "26 26 23", "fs", "3"},
            {"pg_drop_replication_slot", "2278", "19", "fv", "1"},
            {"pg_encoding_to_char", "19", "23", "fs", "1"},
            {"pg_event_trigger_ddl_commands", "2249", "", "ts", "0"},
            {"pg_event_trigger_dropped_objects", "2249", "", "ts", "0"},
            {"pg_event_trigger_table_rewrite_oid", "26", "", "fs", "0"},
            {"pg_event_trigger_table_rewrite_reason", "23", "", "fs", "0"},
            {"pg_export_snapshot", "25", "", "fv", "0"},
            {"pg_function_is_visible", "16", "26", "fs", "1"},
            {"pg_get_acl", "1034", "26 26 23", "fs", "3"},
            {"pg_get_constraintdef", "25", "26", "fs", "1"},
            {"pg_get_constraintdef", "25", "26 16", "fs", "2"},
            {"pg_get_expr", "25", "194 26", "fs", "2"},
            {"pg_get_expr", "25", "194 26 16", "fs", "3"},
            {"pg_get_function_arguments", "25", "26", "fs", "1"},
            {"pg_get_function_identity_arguments", "25", "26", "fs", "1"},
            {"pg_get_function_result", "25", "26", "fs", "1"},
            {"pg_get_function_sqlbody", "25", "26", "fs", "1"},
            {"pg_get_functiondef", "25", "26", "fs", "1"},
            {"pg_get_indexdef", "25", "26", "fs", "1"},
            {"pg_get_indexdef", "25", "26 23 16", "fs", "3"},
            {"pg_get_keywords", "2249", "", "ts", "0"},
            {"pg_get_partkeydef", "25", "26", "fs", "1"},
            {"pg_get_ruledef", "25", "26", "fs", "1"},
            {"pg_get_ruledef", "25", "26 16", "fs", "2"},
            {"pg_get_loaded_modules", "2249", "", "ts", "0"},
            {"pg_get_serial_sequence", "25", "25 25", "fs", "2"},
            {"pg_get_triggerdef", "25", "26", "fs", "1"},
            {"pg_get_triggerdef", "25", "26 16", "fs", "2"},
            {"pg_get_userbyid", "19", "26", "fs", "1"},
            {"pg_get_viewdef", "25", "25", "fs", "1"},
            {"pg_get_viewdef", "25", "26", "fs", "1"},
            {"pg_get_viewdef", "25", "25 16", "fs", "2"},
            {"pg_get_viewdef", "25", "26 16", "fs", "2"},
            {"pg_get_viewdef", "25", "26 23", "fs", "2"},
            {"pg_has_role", "16", "19 25", "fs", "2"},
            {"pg_has_role", "16", "26 25", "fs", "2"},
            {"pg_has_role", "16", "19 19 25", "fs", "3"},
            {"pg_has_role", "16", "19 26 25", "fs", "3"},
            {"pg_has_role", "16", "26 19 25", "fs", "3"},
            {"pg_has_role", "16", "26 26 25", "fs", "3"},
            {"pg_indexam_has_property", "16", "26 25", "fs", "2"},
            {"pg_indexes_size", "20", "2205", "fv", "1"},
            {"pg_is_in_recovery", "16", "", "fv", "0"},
            {"pg_is_other_temp_schema", "16", "26", "fs", "1"},
            {"pg_is_wal_replay_paused", "16", "", "fv", "0"},
            {"pg_last_wal_receive_lsn", "3220", "", "fv", "0"},
            {"pg_last_wal_replay_lsn", "3220", "", "fv", "0"},
            {"pg_last_xact_replay_timestamp", "1184", "", "fv", "0"},
            {"pg_listening_channels", "25", "", "ts", "0"},
            {"pg_log_backend_memory_contexts", "16", "23", "fv", "1"},
            {"pg_logical_slot_get_changes", "2249", "19 3220 23 1009", "tv", "3+"},
            {"pg_logical_slot_peek_changes", "2249", "19 3220 23 1009", "tv", "3+"},
            {"pg_ls_archive_statusdir", "2249", "", "tv", "0"},
            {"pg_ls_dir", "25", "25", "tv", "1"},
            {"pg_ls_dir", "25", "25 16 16", "tv", "3"},
            {"pg_ls_logdir", "2249", "", "tv", "0"},
            {"pg_ls_tmpdir", "2249", "", "tv", "0"},
            {"pg_ls_tmpdir", "2249", "26", "tv", "1"},
            {"pg_ls_waldir", "2249", "", "tv", "0"},
            {"pg_my_temp_schema", "26", "", "fs", "0"},
            {"pg_notification_queue_usage", "701", "", "fv", "0"},
            {"pg_notify", "2278", "25 25", "fv", "2"},
            {"pg_opclass_is_visible", "16", "26", "fs", "1"},
            {"pg_operator_is_visible", "16", "26", "fs", "1"},
            {"pg_options_to_table", "2249", "1009", "ts", "1"},
            {"pg_partition_ancestors", "2205", "2205", "tv", "1"},
            {"pg_partition_root", "2205", "2205", "fi", "1"},
            {"pg_partition_tree", "2249", "2205", "tv", "1"},
            {"pg_postmaster_start_time", "1184", "", "fs", "0"},
            {"pg_promote", "16", "16 23", "fv", "0"},
            {"pg_read_binary_file", "17", "25", "fv", "1"},
            {"pg_read_binary_file", "17", "25 16", "fv", "2"},
            {"pg_read_binary_file", "17", "25 20 20", "fv", "3"},
            {"pg_read_binary_file", "17", "25 20 20 16", "fv", "4"},
            {"pg_read_file", "25", "25", "fv", "1"},
            {"pg_read_file", "25", "25 16", "fv", "2"},
            {"pg_read_file", "25", "25 20 20", "fv", "3"},
            {"pg_read_file", "25", "25 20 20 16", "fv", "4"},
            {"pg_relation_filepath", "25", "2205", "fs", "1"},
            {"pg_relation_is_updatable", "23", "2205 16", "fs", "2"},
            {"pg_relation_size", "20", "2205", "fv", "1"},
            {"pg_relation_size", "20", "2205 25", "fv", "2"},
            {"pg_reload_conf", "16", "", "fv", "0"},
            {"pg_replication_slot_advance", "2249", "19 3220", "fv", "2"},
            {"pg_rotate_logfile", "16", "", "fv", "0"},
            {"pg_safe_snapshot_blocking_pids", "1007", "23", "fv", "1"},
            {"pg_sequence_last_value", "20", "2205", "fv", "1"},
            {"pg_show_all_settings", "2249", "", "ts", "0"},
            {"pg_size_bytes", "20", "25", "fi", "1"},
            {"pg_size_pretty", "25", "1700", "fi", "1"},
            {"pg_size_pretty", "25", "20", "fi", "1"},
            {"pg_sleep", "2278", "701", "fi", "1"},
            {"pg_sleep_for", "2278", "1186", "fv", "1"},
            {"pg_sleep_until", "2278", "1184", "fv", "1"},
            {"pg_snapshot_xip", "5069", "5038", "ti", "1"},
            {"pg_snapshot_xmax", "5069", "5038", "fi", "1"},
            {"pg_snapshot_xmin", "5069", "5038", "fi", "1"},
            {"pg_stat_clear_snapshot", "2278", "", "fv", "0"},
            {"pg_stat_file", "2249", "25", "fv", "1"},
            {"pg_stat_file", "2249", "25 16", "fv", "2"},
            {"pg_stat_reset", "2278", "", "fv", "0"},
            {"pg_stat_reset_shared", "2278", "25", "fv", "0"},
            {"pg_stat_reset_single_function_counters", "2278", "26", "fv", "1"},
            {"pg_stat_reset_single_table_counters", "2278", "26", "fv", "1"},
            // Every parameter has a default, so the form anybody writes is the one with none.
            {"pg_stat_statements_reset", "1184", "26 26 20 16", "fv", "0*"},
            {"pg_switch_wal", "3220", "", "fv", "0"},
            {"pg_table_is_visible", "16", "26", "fs", "1"},
            {"pg_stat_get_backend_io", "2249", "23", "ts", "1"},
            {"pg_table_size", "20", "2205", "fv", "1"},
            {"pg_tablespace_location", "25", "26", "fs", "1"},
            {"pg_tablespace_size", "20", "19", "fv", "1"},
            {"pg_tablespace_size", "20", "26", "fv", "1"},
            {"pg_terminate_backend", "16", "23 20", "fv", "1"},
            {"pg_total_relation_size", "20", "2205", "fv", "1"},
            {"pg_try_advisory_lock", "16", "20", "fv", "1"},
            {"pg_try_advisory_lock", "16", "23 23", "fv", "2"},
            {"pg_try_advisory_lock_shared", "16", "20", "fv", "1"},
            {"pg_try_advisory_lock_shared", "16", "23 23", "fv", "2"},
            {"pg_try_advisory_xact_lock", "16", "20", "fv", "1"},
            {"pg_try_advisory_xact_lock", "16", "23 23", "fv", "2"},
            {"pg_try_advisory_xact_lock_shared", "16", "20", "fv", "1"},
            {"pg_try_advisory_xact_lock_shared", "16", "23 23", "fv", "2"},
            {"pg_ts_config_is_visible", "16", "26", "fs", "1"},
            {"pg_ts_dict_is_visible", "16", "26", "fs", "1"},
            {"pg_ts_parser_is_visible", "16", "26", "fs", "1"},
            {"pg_ts_template_is_visible", "16", "26", "fs", "1"},
            {"pg_type_is_visible", "16", "26", "fs", "1"},
            {"pg_typeof", "2206", "2276", "fs", "1"},
            {"pg_visible_in_snapshot", "16", "5069 5038", "fi", "2"},
            {"pg_wal_lsn_diff", "1700", "3220 3220", "fi", "2"},
            {"pg_wal_replay_pause", "2278", "", "fv", "0"},
            {"pg_wal_replay_resume", "2278", "", "fv", "0"},
            {"pg_walfile_name", "25", "3220", "fi", "1"},
            {"pg_xact_status", "25", "5069", "fv", "1"},
            {"phraseto_tsquery", "3615", "25", "fs", "1"},
            {"phraseto_tsquery", "3615", "3734 25", "fi", "2"},
            {"pi", "701", "", "fi", "0"},
            {"plainto_tsquery", "3615", "25", "fs", "1"},
            {"plainto_tsquery", "3615", "3734 25", "fi", "2"},
            {"point", "600", "601", "fi", "1"},
            {"point", "600", "603", "fi", "1"},
            {"point", "600", "604", "fi", "1"},
            {"point", "600", "718", "fi", "1"},
            {"point", "600", "701 701", "fi", "2"},
            {"polygon", "604", "602", "fi", "1"},
            {"polygon", "604", "603", "fi", "1"},
            {"polygon", "604", "718", "fi", "1"},
            {"polygon", "604", "23 718", "fi", "2"},
            {"popen", "602", "602", "fi", "1"},
            {"populate_record", "2283", "2283 90001", "fi", "2*"},
            {"position", "23", "1560 1560", "fi", "2"},
            {"position", "23", "17 17", "fi", "2"},
            {"position", "23", "25 25", "fi", "2"},
            {"pow", "1700", "1700 1700", "fi", "2"},
            {"pow", "701", "701 701", "fi", "2"},
            {"power", "1700", "1700 1700", "fi", "2"},
            {"power", "701", "701 701", "fi", "2"},
            {"query_to_xml", "142", "25 16 16 25", "fv", "4"},
            {"querytree", "25", "3615", "fi", "1"},
            {"quote_ident", "25", "25", "fi", "1"},
            {"quote_literal", "25", "2283", "fs", "1"},
            {"quote_literal", "25", "25", "fi", "1"},
            {"quote_nullable", "25", "2283", "fs", "1"},
            {"quote_nullable", "25", "25", "fi", "1"},
            {"radians", "701", "701", "fi", "1"},
            {"radius", "701", "718", "fi", "1"},
            {"random", "701", "", "fv", "0"},
            {"random", "1700", "1700 1700", "fv", "2"},
            {"random", "20", "20 20", "fv", "2"},
            {"random", "23", "23 23", "fv", "2"},
            {"random_normal", "701", "701 701", "fv", "0"},
            {"range_merge", "3831", "4537", "fi", "1"},
            {"range_merge", "3831", "3831 3831", "fi", "2"},
            {"regclass", "2205", "25", "fs", "1"},
            {"regexp_count", "23", "25 25", "fi", "2"},
            {"regexp_count", "23", "25 25 23", "fi", "3"},
            {"regexp_count", "23", "25 25 23 25", "fi", "4"},
            {"regexp_instr", "23", "25 25", "fi", "2"},
            {"regexp_instr", "23", "25 25 23", "fi", "3"},
            {"regexp_instr", "23", "25 25 23 23", "fi", "4"},
            {"regexp_instr", "23", "25 25 23 23 23", "fi", "5"},
            {"regexp_instr", "23", "25 25 23 23 23 25", "fi", "6"},
            {"regexp_instr", "23", "25 25 23 23 23 25 23", "fi", "7"},
            {"regexp_like", "16", "25 25", "fi", "2"},
            {"regexp_like", "16", "25 25 25", "fi", "3"},
            {"regexp_match", "1009", "25 25", "fi", "2"},
            {"regexp_match", "1009", "25 25 25", "fi", "3"},
            {"regexp_matches", "1009", "25 25", "ti", "2"},
            {"regexp_matches", "1009", "25 25 25", "ti", "3"},
            {"regexp_replace", "25", "25 25 25", "fi", "3"},
            {"regexp_replace", "25", "25 25 25 23", "fi", "4"},
            {"regexp_replace", "25", "25 25 25 25", "fi", "4"},
            {"regexp_replace", "25", "25 25 25 23 23", "fi", "5"},
            {"regexp_replace", "25", "25 25 25 23 23 25", "fi", "6"},
            {"regexp_split_to_array", "1009", "25 25", "fi", "2"},
            {"regexp_split_to_array", "1009", "25 25 25", "fi", "3"},
            {"regexp_split_to_table", "25", "25 25", "ti", "2"},
            {"regexp_split_to_table", "25", "25 25 25", "ti", "3"},
            {"regexp_substr", "25", "25 25", "fi", "2"},
            {"regexp_substr", "25", "25 25 23", "fi", "3"},
            {"regexp_substr", "25", "25 25 23 23", "fi", "4"},
            {"regexp_substr", "25", "25 25 23 23 25", "fi", "5"},
            {"regexp_substr", "25", "25 25 23 23 25 23", "fi", "6"},
            {"repeat", "25", "25 23", "fi", "2"},
            {"replace", "25", "25 25 25", "fi", "3"},
            {"reverse", "17", "17", "fi", "1"},
            {"reverse", "25", "25", "fi", "1"},
            {"right", "25", "25 23", "fi", "2"},
            {"round", "1700", "1700", "fi", "1"},
            {"round", "701", "701", "fi", "1"},
            {"round", "1700", "1700 23", "fi", "2"},
            {"row_to_json", "114", "2249", "fs", "1"},
            {"row_to_json", "114", "2249 16", "fs", "2"},
            {"rpad", "25", "25 23", "fi", "2"},
            {"rpad", "25", "25 23 25", "fi", "3"},
            {"rtrim", "25", "25", "fi", "1"},
            {"rtrim", "17", "17 17", "fi", "2"},
            {"rtrim", "25", "25 25", "fi", "2"},
            {"scale", "23", "1700", "fi", "1"},
            {"schema_to_xml", "142", "19 16 16 25", "fs", "4"},
            {"session_user", "19", "", "fs", "0"},
            {"set_bit", "1560", "1560 23 23", "fi", "3"},
            {"set_bit", "17", "17 20 23", "fi", "3"},
            {"set_byte", "17", "17 23 23", "fi", "3"},
            {"set_config", "25", "25 25 16", "fv", "3"},
            {"set_masklen", "650", "650 23", "fi", "2"},
            {"set_masklen", "869", "869 23", "fi", "2"},
            {"setseed", "2278", "701", "fv", "1"},
            {"setval", "20", "2205 20", "fv", "2"},
            {"setval", "20", "2205 20 16", "fv", "3"},
            {"setweight", "3614", "3614 18", "fi", "2"},
            {"setweight", "3614", "3614 18 1009", "fi", "3"},
            {"sha224", "17", "17", "fi", "1"},
            {"sha256", "17", "17", "fi", "1"},
            {"sha384", "17", "17", "fi", "1"},
            {"sha512", "17", "17", "fi", "1"},
            {"shobj_description", "25", "26 19", "fs", "2"},
            {"show_trgm", "1009", "25", "fi", "1*"},
            {"sign", "1700", "1700", "fi", "1"},
            {"sign", "701", "701", "fi", "1"},
            {"similarity", "700", "25 25", "fi", "2*"},
            {"sin", "701", "701", "fi", "1"},
            {"sind", "701", "701", "fi", "1"},
            {"sinh", "701", "701", "fi", "1"},
            {"skeys", "25", "90001", "ti", "1*"},
            {"slice", "90001", "90001 1009", "fi", "2*"},
            {"slope", "701", "600 600", "fi", "2"},
            {"soundex", "25", "25", "fi", "1*"},
            {"split_part", "25", "25 25 23", "fi", "3"},
            {"sqrt", "1700", "1700", "fi", "1"},
            {"sqrt", "701", "701", "fi", "1"},
            {"starts_with", "16", "25 25", "fi", "2"},
            {"statement_timestamp", "1184", "", "fs", "0"},
            {"string_to_array", "1009", "25 25", "fi", "2"},
            {"string_to_array", "1009", "25 25 25", "fi", "3"},
            {"string_to_table", "25", "25 25", "ti", "2"},
            {"string_to_table", "25", "25 25 25", "ti", "3"},
            {"strip", "3614", "3614", "fi", "1"},
            {"strpos", "23", "25 25", "fi", "2"},
            {"substr", "17", "17 23", "fi", "2"},
            {"substr", "25", "25 23", "fi", "2"},
            {"substr", "17", "17 23 23", "fi", "3"},
            {"substr", "25", "25 23 23", "fi", "3"},
            {"substring", "1560", "1560 23", "fi", "2"},
            {"substring", "17", "17 23", "fi", "2"},
            {"substring", "25", "25 23", "fi", "2"},
            {"substring", "25", "25 25", "fi", "2"},
            {"substring", "1560", "1560 23 23", "fi", "3"},
            {"substring", "17", "17 23 23", "fi", "3"},
            {"substring", "25", "25 23 23", "fi", "3"},
            {"substring", "25", "25 25 25", "fi", "3"},
            {"svals", "25", "90001", "ti", "1*"},
            {"table_to_xml", "142", "2205 16 16 25", "fs", "4"},
            {"tan", "701", "701", "fi", "1"},
            {"tand", "701", "701", "fi", "1"},
            {"tanh", "701", "701", "fi", "1"},
            {"text", "25", "1042", "fi", "1"},
            {"text", "25", "142", "fi", "1"},
            {"text", "25", "16", "fi", "1"},
            {"text", "25", "18", "fi", "1"},
            {"text", "25", "19", "fi", "1"},
            {"text", "25", "869", "fi", "1"},
            {"time", "1083", "1114", "fi", "1"},
            {"time", "1083", "1184", "fs", "1"},
            {"time", "1083", "1186", "fi", "1"},
            {"time", "1083", "1266", "fi", "1"},
            {"time", "1083", "1083 23", "fi", "2"},
            {"timeofday", "25", "", "fv", "0"},
            {"timestamp", "1114", "1082", "fi", "1"},
            {"timestamp", "1114", "1184", "fs", "1"},
            {"timestamp", "1114", "1082 1083", "fi", "2"},
            {"timestamp", "1114", "1114 23", "fi", "2"},
            {"timestamptz", "1184", "1082", "fs", "1"},
            {"timestamptz", "1184", "1114", "fs", "1"},
            {"timestamptz", "1184", "1082 1083", "fs", "2"},
            {"timestamptz", "1184", "1082 1266", "fi", "2"},
            {"timestamptz", "1184", "1184 23", "fi", "2"},
            {"timetz", "1266", "1083", "fs", "1"},
            {"timetz", "1266", "1184", "fs", "1"},
            {"timetz", "1266", "1266 23", "fi", "2"},
            {"timezone", "1184", "1114", "fs", "1"},
            {"timezone", "1114", "1184", "fs", "1"},
            {"timezone", "1266", "1266", "fs", "1"},
            {"timezone", "1184", "1186 1114", "fi", "2"},
            {"timezone", "1114", "1186 1184", "fi", "2"},
            {"timezone", "1266", "1186 1266", "fi", "2"},
            {"timezone", "1184", "25 1114", "fi", "2"},
            {"timezone", "1114", "25 1184", "fi", "2"},
            {"timezone", "1266", "25 1266", "fs", "2"},
            {"to_char", "25", "1114 25", "fs", "2"},
            {"to_char", "25", "1184 25", "fs", "2"},
            {"to_char", "25", "1186 25", "fs", "2"},
            {"to_char", "25", "1700 25", "fs", "2"},
            {"to_char", "25", "20 25", "fs", "2"},
            {"to_char", "25", "23 25", "fs", "2"},
            {"to_char", "25", "700 25", "fs", "2"},
            {"to_char", "25", "701 25", "fs", "2"},
            {"to_date", "1082", "25 25", "fs", "2"},
            {"to_ascii", "25", "25", "fi", "1"},
            {"to_ascii", "25", "25 19", "fi", "2"},
            {"to_ascii", "25", "25 23", "fi", "2"},
            {"to_hex", "25", "20", "fi", "1"},
            {"to_hex", "25", "23", "fi", "1"},
            {"to_json", "114", "2283", "fs", "1"},
            {"to_jsonb", "3802", "2283", "fs", "1"},
            {"to_number", "1700", "25 25", "fs", "2"},
            {"to_regclass", "2205", "25", "fs", "1"},
            {"to_regproc", "24", "25", "fs", "1"},
            {"to_regprocedure", "2202", "25", "fs", "1"},
            {"to_regtype", "2206", "25", "fs", "1"},
            {"to_timestamp", "1184", "701", "fi", "1"},
            {"to_timestamp", "1184", "25 25", "fs", "2"},
            {"to_tsquery", "3615", "25", "fs", "1"},
            {"to_tsquery", "3615", "3734 25", "fi", "2"},
            {"to_tsvector", "3614", "114", "fs", "1"},
            {"to_tsvector", "3614", "25", "fs", "1"},
            {"to_tsvector", "3614", "3802", "fs", "1"},
            {"to_tsvector", "3614", "3734 114", "fi", "2"},
            {"to_tsvector", "3614", "3734 25", "fi", "2"},
            {"to_tsvector", "3614", "3734 3802", "fi", "2"},
            {"transaction_timestamp", "1184", "", "fs", "0"},
            {"translate", "25", "25 25 25", "fi", "3"},
            {"trim_array", "2277", "2277 23", "fi", "2"},
            {"trim_scale", "1700", "1700", "fi", "1"},
            {"trunc", "1700", "1700", "fi", "1"},
            {"trunc", "701", "701", "fi", "1"},
            {"trunc", "774", "774", "fi", "1"},
            {"trunc", "829", "829", "fi", "1"},
            {"trunc", "1700", "1700 23", "fi", "2"},
            {"ts_debug", "2249", "25", "ts", "1"},
            {"ts_debug", "2249", "3734 25", "ts", "2"},
            {"ts_delete", "3614", "3614 1009", "fi", "2"},
            {"ts_delete", "3614", "3614 25", "fi", "2"},
            {"ts_filter", "3614", "3614 1002", "fi", "2"},
            {"ts_headline", "114", "114 3615", "fs", "2"},
            {"ts_headline", "25", "25 3615", "fs", "2"},
            {"ts_headline", "3802", "3802 3615", "fs", "2"},
            {"ts_headline", "114", "114 3615 25", "fs", "3"},
            {"ts_headline", "25", "25 3615 25", "fs", "3"},
            {"ts_headline", "114", "3734 114 3615", "fi", "3"},
            {"ts_headline", "25", "3734 25 3615", "fi", "3"},
            {"ts_headline", "3802", "3734 3802 3615", "fi", "3"},
            {"ts_headline", "3802", "3802 3615 25", "fs", "3"},
            {"ts_headline", "114", "3734 114 3615 25", "fi", "4"},
            {"ts_headline", "25", "3734 25 3615 25", "fi", "4"},
            {"ts_headline", "3802", "3734 3802 3615 25", "fi", "4"},
            {"ts_lexize", "1009", "3769 25", "fi", "2"},
            {"ts_parse", "2249", "25 25", "ts", "2"},
            {"ts_parse", "2249", "26 25", "ti", "2"},
            {"ts_rank", "700", "3614 3615", "fi", "2"},
            {"ts_rank", "700", "1021 3614 3615", "fi", "3"},
            {"ts_rank", "700", "3614 3615 23", "fi", "3"},
            {"ts_rank", "700", "1021 3614 3615 23", "fi", "4"},
            {"ts_rank_cd", "700", "3614 3615", "fi", "2"},
            {"ts_rank_cd", "700", "1021 3614 3615", "fi", "3"},
            {"ts_rank_cd", "700", "3614 3615 23", "fi", "3"},
            {"ts_rank_cd", "700", "1021 3614 3615 23", "fi", "4"},
            {"ts_rewrite", "3615", "3615 25", "fv", "2"},
            {"ts_rewrite", "3615", "3615 3615 3615", "fi", "3"},
            {"ts_stat", "2249", "25", "tv", "1"},
            {"ts_stat", "2249", "25 25", "tv", "2"},
            {"ts_token_type", "2249", "25", "ts", "1"},
            {"ts_token_type", "2249", "26", "ti", "1"},
            {"tsmultirange", "4533", "", "fi", "0"},
            {"tsmultirange", "4533", "3908", "fi", "1"},
            {"tsmultirange", "4533", "3909", "fi", "1+"},
            {"tsquery_phrase", "3615", "3615 3615", "fi", "2"},
            {"tsquery_phrase", "3615", "3615 3615 23", "fi", "3"},
            {"tsrange", "3908", "1114 1114", "fi", "2"},
            {"tsrange", "3908", "1114 1114 25", "fi", "3"},
            {"tstzmultirange", "4534", "", "fi", "0"},
            {"tstzmultirange", "4534", "3910", "fi", "1"},
            {"tstzmultirange", "4534", "3911", "fi", "1+"},
            {"tstzrange", "3910", "1184 1184", "fi", "2"},
            {"tstzrange", "3910", "1184 1184 25", "fi", "3"},
            {"tsvector_to_array", "1009", "3614", "fi", "1"},
            {"txid_current", "20", "", "fs", "0"},
            {"txid_current_if_assigned", "20", "", "fs", "0"},
            {"txid_current_snapshot", "2970", "", "fs", "0"},
            {"txid_snapshot_xip", "20", "2970", "ti", "1"},
            {"txid_snapshot_xmax", "20", "2970", "fi", "1"},
            {"txid_snapshot_xmin", "20", "2970", "fi", "1"},
            {"txid_status", "25", "20", "fv", "1"},
            {"unaccent", "25", "25", "fs", "1*"},
            {"unaccent", "25", "3769 25", "fs", "2*"},
            {"unicode_assigned", "16", "25", "fi", "1"},
            {"unicode_version", "25", "", "fi", "0"},
            {"unistr", "25", "25", "fi", "1"},
            {"unnest", "2283", "2277", "ti", "1"},
            {"unnest", "2249", "3614", "ti", "1"},
            {"unnest", "3831", "4537", "ti", "1"},
            {"upper", "25", "25", "fi", "1"},
            {"upper", "2283", "3831", "fi", "1"},
            {"upper", "2283", "4537", "fi", "1"},
            {"upper_inc", "16", "3831", "fi", "1"},
            {"upper_inc", "16", "4537", "fi", "1"},
            {"upper_inf", "16", "3831", "fi", "1"},
            {"upper_inf", "16", "4537", "fi", "1"},
            {"uuid_extract_timestamp", "1184", "2950", "fi", "1"},
            {"uuid_extract_version", "21", "2950", "fi", "1"},
            {"uuid_generate_v1", "2950", "", "fv", "0*"},
            {"uuid_generate_v3", "2950", "2950 25", "fi", "2*"},
            {"uuid_generate_v4", "2950", "", "fv", "0*"},
            {"uuid_generate_v5", "2950", "2950 25", "fi", "2*"},
            {"uuid_nil", "2950", "", "fi", "0*"},
            {"uuid_ns_dns", "2950", "", "fi", "0*"},
            {"uuid_ns_url", "2950", "", "fi", "0*"},
            {"uuidv4", "2950", "", "fv", "0"},
            {"uuidv7", "2950", "", "fv", "0"},
            {"uuidv7", "2950", "1186", "fv", "1"},
            {"varbit", "1562", "1562 23 16", "fi", "3"},
            {"varchar", "1043", "19", "fi", "1"},
            {"varchar", "1043", "1043 23 16", "fi", "3"},
            {"version", "25", "", "fs", "0"},
            {"websearch_to_tsquery", "3615", "25", "fs", "1"},
            {"websearch_to_tsquery", "3615", "3734 25", "fi", "2"},
            {"width", "701", "603", "fi", "1"},
            {"width_bucket", "23", "5077 5078", "fi", "2"},
            {"width_bucket", "23", "1700 1700 1700 23", "fi", "4"},
            {"width_bucket", "23", "701 701 701 23", "fi", "4"},
            {"xml", "142", "25", "fs", "1"},
            {"xml_is_well_formed", "16", "25", "fs", "1"},
            {"xml_is_well_formed_content", "16", "25", "fi", "1"},
            {"xml_is_well_formed_document", "16", "25", "fi", "1"},
            {"xmlcomment", "142", "25", "fi", "1"},
            {"xmlexists", "16", "25 142", "fi", "2"},
            {"xmltext", "142", "25", "fi", "1"},
            {"xpath", "143", "25 142", "fi", "2"},
            {"xpath", "143", "25 142 1009", "fi", "3"},
            {"xpath_exists", "16", "25 142", "fi", "2"},
            {"xpath_exists", "16", "25 142 1009", "fi", "3"},
    };

    /**
     * The signatures PostgreSQL does <em>not</em> declare strict, by proname and proargtypes.
     * Read from the reference server, one row per signature memgres records.
     */
    private static final String[][] NOT_STRICT = {
            {"array_append", "5078 5077"},
            {"array_cat", "5078 5078"},
            {"array_fill", "2283 1007"},
            {"array_fill", "2283 1007 1007"},
            {"array_position", "5078 5077"},
            {"array_position", "5078 5077 23"},
            {"array_positions", "5078 5077"},
            {"array_prepend", "5077 5078"},
            {"array_remove", "5078 5077"},
            {"array_replace", "5078 5077 5077"},
            {"array_to_string", "2277 25 25"},
            {"concat", "2276"},
            {"concat_ws", "25 2276"},
            {"current_query", ""},
            {"daterange", "1082 1082"},
            {"daterange", "1082 1082 25"},
            {"enum_first", "3500"},
            {"enum_last", "3500"},
            {"enum_range", "3500"},
            {"enum_range", "3500 3500"},
            {"format", "25"},
            {"format", "25 2276"},
            {"format_type", "26 23"},
            {"inet_client_addr", ""},
            {"inet_client_port", ""},
            {"inet_server_addr", ""},
            {"inet_server_port", ""},
            {"int4range", "23 23"},
            {"int4range", "23 23 25"},
            {"int8range", "20 20"},
            {"int8range", "20 20 25"},
            {"json_build_array", ""},
            {"json_build_array", "2276"},
            {"json_build_object", ""},
            {"json_build_object", "2276"},
            {"json_populate_record", "2283 114 16"},
            {"json_populate_recordset", "2283 114 16"},
            {"json_to_recordset", "114"},
            {"jsonb_build_array", ""},
            {"jsonb_build_array", "2276"},
            {"jsonb_build_object", ""},
            {"jsonb_build_object", "2276"},
            {"jsonb_populate_record", "2283 3802"},
            {"jsonb_populate_recordset", "2283 3802"},
            {"jsonb_set_lax", "3802 1009 3802 16 25"},
            {"jsonb_to_recordset", "3802"},
            {"num_nonnulls", "2276"},
            {"num_nulls", "2276"},
            {"numrange", "1700 1700"},
            {"numrange", "1700 1700 25"},
            {"overlaps", "1083 1083 1083 1083"},
            {"overlaps", "1083 1083 1083 1186"},
            {"overlaps", "1083 1186 1083 1083"},
            {"overlaps", "1083 1186 1083 1186"},
            {"overlaps", "1114 1114 1114 1114"},
            {"overlaps", "1114 1114 1114 1186"},
            {"overlaps", "1114 1186 1114 1114"},
            {"overlaps", "1114 1186 1114 1186"},
            {"overlaps", "1184 1184 1184 1184"},
            {"overlaps", "1184 1184 1184 1186"},
            {"overlaps", "1184 1186 1184 1184"},
            {"overlaps", "1184 1186 1184 1186"},
            {"overlaps", "1266 1266 1266 1266"},
            {"pg_current_logfile", ""},
            {"pg_current_logfile", "25"},
            {"pg_logical_slot_get_changes", "19 3220 23 1009"},
            {"pg_logical_slot_peek_changes", "19 3220 23 1009"},
            {"pg_notify", "25 25"},
            {"pg_stat_clear_snapshot", ""},
            {"pg_stat_reset", ""},
            {"pg_stat_reset_shared", "25"},
            {"pg_typeof", "2276"},
            {"quote_nullable", "2283"},
            {"quote_nullable", "25"},
            {"set_config", "25 25 16"},
            {"string_to_array", "25 25"},
            {"string_to_array", "25 25 25"},
            {"string_to_table", "25 25"},
            {"string_to_table", "25 25 25"},
            {"tsrange", "1114 1114"},
            {"tsrange", "1114 1114 25"},
            {"tstzrange", "1184 1184"},
            {"tstzrange", "1184 1184 25"},
    };

    /**
     * The window functions. PostgreSQL marks these prokind='w': they are not callable as ordinary
     * functions and a tool that lists them as such will generate a call the server rejects.
     *
     * <p>Read from the reference server's {@code pg_proc} like the table above, so the argument
     * lists here are the ones a call is resolved against.
     *
     * <p>Columns: proname, prorettype, proargtypes.
     */
    static final String[][] WINDOW_FUNCTIONS = {
            {"cume_dist", "701", ""},
            {"dense_rank", "20", ""},
            {"first_value", "2283", "2283"},
            {"lag", "2283", "2283"},
            {"lag", "2283", "2283 23"},
            {"lag", "5077", "5077 23 5077"},
            {"last_value", "2283", "2283"},
            {"lead", "2283", "2283"},
            {"lead", "2283", "2283 23"},
            {"lead", "5077", "5077 23 5077"},
            {"nth_value", "2283", "2283 23"},
            {"ntile", "23", "23"},
            {"percent_rank", "701", ""},
            {"rank", "20", ""},
            {"row_number", "20", ""},
    };

    /**
     * What PostgreSQL records about a built-in beyond the types in its signature.
     *
     * <p>A signature says what a call may pass and what comes back. The rest of a pg_proc row says
     * how the call behaves and what its parameters are called, and those columns were left at
     * whatever memgres could derive: proallargtypes, proargmodes, proargnames and proargdefaults
     * were NULL on all 2338 rows, so {@code json_each} had no OUT columns, {@code concat} was not
     * marked VARIADIC, {@code jsonb_set}'s fourth argument claimed a default it did not carry, and
     * {@code pg_sleep} carried an argument name PostgreSQL does not give it. procost was 100 where
     * PostgreSQL says 1 and 1 where it says 10 or 100 or 1000; proparallel was derived from
     * volatility, which is not the rule PostgreSQL follows; proisstrict and provolatile were
     * guessed for the whole type-I/O family.
     *
     * <p>Each row here is one signature's answer read whole off the reference server, for every
     * signature memgres carries whose answer differs from what it derives. Keyed by name and
     * proargtypes together, because none of these columns is decided by the name alone —
     * {@code array_to_string(anyarray, text)} is strict and the three-argument form is not,
     * {@code ts_stat(text)} costs 10 and {@code ts_debug(text)} costs 100.
     *
     * <p>One row per string so the whole table is one array of constants rather than four hundred
     * nested ones — a nested-array initialiser this size does not fit in a class initialiser
     * beside {@link #SIGNATURES}. Fields, in order, separated by {@code |}: proname, proargtypes,
     * proallargtypes, proargmodes, proargnames, proargdefaults, provariadic, procost, prorows,
     * provolatile, proparallel, proisstrict. The three array columns are comma-separated and empty
     * where PostgreSQL has NULL; proargdefaults is separated by {@code ~} because {@code |} is
     * already spoken for, and is turned back into PostgreSQL's one-per-defaulted-argument list
     * when the row is written.
     */
    private static final class Recorded {

        static final String[] ROWS = {
            "aclexplode|1034|1034,26,26,25,16|i,o,o,o,o|acl,grantor,grantee,"
                    + "privilege_type,is_grantable||0|1|10|s|s|t",
            "aclitemin|2275|||||0|1|0|s|s|t",
            "aclitemout|1033|||||0|1|0|s|s|t",
            "age|28|||||0|1|0|s|r|t",
            "anyarray_out|2277|||||0|1|0|s|s|t",
            "anycompatiblearray_out|5078|||||0|1|0|s|s|t",
            "anyarray_recv|2281|||||0|1|0|s|s|t",
            "anyarray_send|2277|||||0|1|0|s|s|t",
            "anycompatiblearray_recv|2281|||||0|1|0|s|s|t",
            "anycompatiblearray_send|5078|||||0|1|0|s|s|t",
            "anycompatiblemultirange_in|2275 26 23|||||0|1|0|s|s|t",
            "anycompatiblemultirange_out|4538|||||0|1|0|s|s|t",
            "anycompatiblerange_in|2275 26 23|||||0|1|0|s|s|t",
            "anycompatiblerange_out|5080|||||0|1|0|s|s|t",
            "anyenum_out|3500|||||0|1|0|s|s|t",
            "anymultirange_in|2275 26 23|||||0|1|0|s|s|t",
            "anymultirange_out|4537|||||0|1|0|s|s|t",
            "anyrange_in|2275 26 23|||||0|1|0|s|s|t",
            "anyrange_out|3831|||||0|1|0|s|s|t",
            "anytextcat|2776 25|||||0|1|0|s|s|t",
            "array_in|2275 26 23|||||0|1|0|s|s|t",
            "array_out|2277|||||0|1|0|s|s|t",
            "array_recv|2281 26 23|||||0|1|0|s|s|t",
            "array_sample|2277 23|||||0|1|0|v|s|t",
            "array_send|2277|||||0|1|0|s|s|t",
            "array_shuffle|2277|||||0|1|0|v|s|t",
            "array_sort|2277 16|||array,descending||0|1|0|i|s|t",
            "array_sort|2277 16 16|||array,descending,nulls_first||0|1|0|i|s|t",
            "array_typanalyze|2281|||||0|1|0|s|s|t",
            "bpcharrecv|2281 26 23|||||0|1|0|s|s|t",
            "bpcharsend|1042|||||0|1|0|s|s|t",
            "brin_bloom_summary_recv|2281|||||0|1|0|s|s|t",
            "brin_bloom_summary_send|4600|||||0|1|0|s|s|t",
            "brin_minmax_multi_summary_recv|2281|||||0|1|0|s|s|t",
            "brin_minmax_multi_summary_send|4601|||||0|1|0|s|s|t",
            "brinhandler|2281|||||0|1|0|v|s|t",
            "bthandler|2281|||||0|1|0|v|s|t",
            "cash_in|2275|||||0|1|0|s|s|t",
            "cash_out|790|||||0|1|0|s|s|t",
            "clock_timestamp||||||0|1|0|v|s|t",
            "col_description|26 23|||||0|100|0|s|s|t",
            "concat|2276|2276|v|||2276|1|0|s|s|f",
            "concat_ws|25 2276|25,2276|i,v|||2276|1|0|s|s|f",
            "current_schema||||||0|1|0|s|u|t",
            "current_schemas|16|||||0|1|0|s|u|t",
            "currval|2205|||||0|1|0|v|u|t",
            "cstring_recv|2281|||||0|1|0|s|s|t",
            "cstring_send|2275|||||0|1|0|s|s|t",
            "database_to_xml|16 16 25|||nulls,tableforest,targetns||0|100|0|s|r|t",
            "date_eq_timestamptz|1082 1184|||||0|1|0|s|s|t",
            "date_ge_timestamptz|1082 1184|||||0|1|0|s|s|t",
            "date_gt_timestamptz|1082 1184|||||0|1|0|s|s|t",
            "date_in|2275|||||0|1|0|s|s|t",
            "date_le_timestamptz|1082 1184|||||0|1|0|s|s|t",
            "date_lt_timestamptz|1082 1184|||||0|1|0|s|s|t",
            "date_ne_timestamptz|1082 1184|||||0|1|0|s|s|t",
            "date_out|1082|||||0|1|0|s|s|t",
            "datemultirange|3913|3913|v|||3912|1|0|i|s|t",
            "event_trigger_in|2275|||||0|1|0|i|s|f",
            "fdw_handler_in|2275|||||0|1|0|i|s|f",
            "fmgr_c_validator|26|||||0|1|0|s|s|t",
            "fmgr_internal_validator|26|||||0|1|0|s|s|t",
            "fmgr_sql_validator|26|||||0|1|0|s|s|t",
            "format|25 2276|25,2276|i,v|||2276|1|0|s|s|f",
            "gen_random_uuid||||||0|1|0|v|s|t",
            "ginhandler|2281|||||0|1|0|v|s|t",
            "gisthandler|2281|||||0|1|0|v|s|t",
            "has_any_column_privilege|19 25 25|||||0|10|0|s|s|t",
            "has_any_column_privilege|19 26 25|||||0|10|0|s|s|t",
            "has_any_column_privilege|25 25|||||0|10|0|s|s|t",
            "has_any_column_privilege|26 25|||||0|10|0|s|s|t",
            "has_any_column_privilege|26 25 25|||||0|10|0|s|s|t",
            "has_any_column_privilege|26 26 25|||||0|10|0|s|s|t",
            "has_largeobject_privilege|19 26 25|||||0|10|0|s|s|t",
            "has_largeobject_privilege|26 25|||||0|10|0|s|s|t",
            "has_largeobject_privilege|26 26 25|||||0|10|0|s|s|t",
            "hashhandler|2281|||||0|1|0|v|s|t",
            "heap_tableam_handler|2281|||||0|1|0|v|s|t",
            "index_am_handler_in|2275|||||0|1|0|i|s|f",
            "inet_client_addr||||||0|1|0|s|r|f",
            "inet_client_port||||||0|1|0|s|r|f",
            "inet_server_addr||||||0|1|0|s|r|f",
            "inet_server_port||||||0|1|0|s|r|f",
            "int4multirange|3905|3905|v|||3904|1|0|i|s|t",
            "int8multirange|3927|3927|v|||3926|1|0|i|s|t",
            "internal_in|2275|||||0|1|0|i|s|f",
            "interval_in|2275 26 23|||||0|1|0|s|s|t",
            "interval_out|1186|||||0|1|0|s|s|t",
            "interval_pl_timestamptz|1186 1184|||||0|1|0|s|s|t",
            "json_agg|2283|||||0|1|0|s|s|f",
            "json_array_element|114 23|||from_json,element_index||0|1|0|i|s|t",
            "json_array_element_text|114 23|||from_json,element_index||0|1|0|i|s|t",
            "json_array_elements|114|114,114|i,o|from_json,value||0|1|100|i|s|t",
            "json_array_elements_text|114|114,25|i,o|from_json,value||0|1|100|i|s|t",
            "json_build_array|2276|2276|v|||2276|1|0|s|s|f",
            "json_build_object|2276|2276|v|||2276|1|0|s|s|f",
            "json_each|114|114,25,114|i,o,o|from_json,key,value||0|1|100|i|s|t",
            "json_each_text|114|114,25,25|i,o,o|from_json,key,value||0|1|100|i|s|t",
            "json_extract_path|114 1009|114,1009|i,v|from_json,path_elems||25|1|0|i|s|t",
            "json_extract_path_text|114 1009|114,1009|i,v|from_json,path_elems||25|1|0|i|s|t",
            "json_object_agg|2276 2276|||key,value||0|1|0|s|s|f",
            "json_object_field|114 25|||from_json,field_name||0|1|0|i|s|t",
            "json_object_field_text|114 25|||from_json,field_name||0|1|0|i|s|t",
            "json_object_keys|114|||||0|1|100|i|s|t",
            "json_populate_record|2283 114 16|||base,from_json,use_json_as_text|false|0|1|0|s|s|f",
            "json_populate_recordset|2283 114 16|||base,from_json,"
                    + "use_json_as_text|false|0|1|100|s|s|f",
            "json_strip_nulls|114 16|||target,strip_in_arrays|false|0|1|0|s|s|t",
            "json_to_recordset|114|||||0|1|100|s|s|f",
            "jsonb_agg|2283|||||0|1|0|s|s|f",
            "jsonb_array_element|3802 23|||from_json,element_index||0|1|0|i|s|t",
            "jsonb_array_element_text|3802 23|||from_json,element_index||0|1|0|i|s|t",
            "jsonb_array_elements|3802|3802,3802|i,o|from_json,value||0|1|100|i|s|t",
            "jsonb_array_elements_text|3802|3802,25|i,o|from_json,value||0|1|100|i|s|t",
            "jsonb_build_array|2276|2276|v|||2276|1|0|s|s|f",
            "jsonb_build_object|2276|2276|v|||2276|1|0|s|s|f",
            "jsonb_each|3802|3802,25,3802|i,o,o|from_json,key,value||0|1|100|i|s|t",
            "jsonb_each_text|3802|3802,25,25|i,o,o|from_json,key,value||0|1|100|i|s|t",
            "jsonb_extract_path|3802 1009|3802,1009|i,v|from_json,path_elems||25|1|0|i|s|t",
            "jsonb_extract_path_text|3802 1009|3802,1009|i,v|from_json,path_elems||25|1|0|i|s|t",
            "jsonb_insert|3802 1009 3802 16|||jsonb_in,path,replacement,"
                    + "insert_after|false|0|1|0|i|s|t",
            "jsonb_object_agg|2276 2276|||key,value||0|1|0|i|s|f",
            "jsonb_object_field|3802 25|||from_json,field_name||0|1|0|i|s|t",
            "jsonb_object_field_text|3802 25|||from_json,field_name||0|1|0|i|s|t",
            "jsonb_object_keys|3802|||||0|1|100|i|s|t",
            "jsonb_path_exists|3802 4072 3802 16|||target,path,vars,"
                    + "silent|'{}'::jsonb~false|0|1|0|i|s|t",
            "jsonb_path_exists_tz|3802 4072 3802 16|||target,path,vars,"
                    + "silent|'{}'::jsonb~false|0|1|0|s|s|t",
            "jsonb_path_match|3802 4072 3802 16|||target,path,vars,"
                    + "silent|'{}'::jsonb~false|0|1|0|i|s|t",
            "jsonb_path_match_tz|3802 4072 3802 16|||target,path,vars,"
                    + "silent|'{}'::jsonb~false|0|1|0|s|s|t",
            "jsonb_path_query|3802 4072 3802 16|||target,path,vars,"
                    + "silent|'{}'::jsonb~false|0|1|1000|i|s|t",
            "jsonb_path_query_array|3802 4072 3802 16|||target,path,vars,"
                    + "silent|'{}'::jsonb~false|0|1|0|i|s|t",
            "jsonb_path_query_array_tz|3802 4072 3802 16|||target,path,vars,"
                    + "silent|'{}'::jsonb~false|0|1|0|s|s|t",
            "jsonb_path_query_first|3802 4072 3802 16|||target,path,vars,"
                    + "silent|'{}'::jsonb~false|0|1|0|i|s|t",
            "jsonb_path_query_first_tz|3802 4072 3802 16|||target,path,vars,"
                    + "silent|'{}'::jsonb~false|0|1|0|s|s|t",
            "jsonb_path_query_tz|3802 4072 3802 16|||target,path,vars,"
                    + "silent|'{}'::jsonb~false|0|1|1000|s|s|t",
            "jsonb_populate_recordset|2283 3802|||||0|1|100|s|s|f",
            "jsonb_set|3802 1009 3802 16|||jsonb_in,path,replacement,"
                    + "create_if_missing|true|0|1|0|i|s|t",
            "jsonb_set_lax|3802 1009 3802 16 25|||jsonb_in,path,replacement,"
                    + "create_if_missing,"
                    + "null_value_treatment|true~'use_json_null'::text|0|1|0|i|s|f",
            "jsonb_strip_nulls|3802 16|||target,strip_in_arrays|false|0|1|0|s|s|t",
            "jsonb_to_recordset|3802|||||0|1|100|s|s|f",
            "language_handler_in|2275|||||0|1|0|i|s|f",
            "lastval||||||0|1|0|v|u|t",
            "lo_close|23|||||0|1|0|v|u|t",
            "lo_creat|23|||||0|1|0|v|u|t",
            "lo_create|26|||||0|1|0|v|u|t",
            "lo_export|26 25|||||0|1|0|v|u|t",
            "lo_from_bytea|26 17|||||0|1|0|v|u|t",
            "lo_get|26|||||0|1|0|v|u|t",
            "lo_get|26 20 23|||||0|1|0|v|u|t",
            "lo_import|25|||||0|1|0|v|u|t",
            "lo_import|25 26|||||0|1|0|v|u|t",
            "lo_lseek|23 23 23|||||0|1|0|v|u|t",
            "lo_open|26 23|||||0|1|0|v|u|t",
            "lo_put|26 20 17|||||0|1|0|v|u|t",
            "lo_tell|23|||||0|1|0|v|u|t",
            "lo_truncate|23 23|||||0|1|0|v|u|t",
            "lo_truncate64|23 20|||||0|1|0|v|u|t",
            "lo_unlink|26|||||0|1|0|v|u|t",
            "loread|23 23|||||0|1|0|v|u|t",
            "lowrite|23 17|||||0|1|0|v|u|t",
            "make_date|23 23 23|||year,month,day||0|1|0|i|s|t",
            "make_interval|23 23 23 23 23 23 701|||years,months,weeks,days,hours,mins,"
                    + "secs|0~0~0~0~0~0~0.0|0|1|0|i|s|t",
            "make_time|23 23 701|||hour,min,sec||0|1|0|i|s|t",
            "make_timestamp|23 23 23 23 23 701|||year,month,mday,hour,min,sec||0|1|0|i|s|t",
            "make_timestamptz|23 23 23 23 23 701|||year,month,mday,hour,min,sec||0|1|0|s|s|t",
            "make_timestamptz|23 23 23 23 23 701 25|||year,month,mday,hour,min,sec,"
                    + "timezone||0|1|0|s|s|t",
            "money|20|||||0|1|0|s|s|t",
            "multirange_in|2275 26 23|||||0|1|0|s|s|t",
            "multirange_out|4537|||||0|1|0|s|s|t",
            "multirange_recv|2281 26 23|||||0|1|0|s|s|t",
            "multirange_send|4537|||||0|1|0|s|s|t",
            "multirange_typanalyze|2281|||||0|1|0|s|s|t",
            "namerecv|2281|||||0|1|0|s|s|t",
            "namesend|19|||||0|1|0|s|s|t",
            "nextval|2205|||||0|1|0|v|u|t",
            "normalize|25 25||||'NFC'::text|0|1|0|i|s|t",
            "num_nonnulls|2276|2276|v|||2276|1|0|i|s|f",
            "num_nulls|2276|2276|v|||2276|1|0|i|s|f",
            "nummultirange|3907|3907|v|||3906|1|0|i|s|t",
            "obj_description|26|||||0|100|0|s|s|t",
            "obj_description|26 19|||||0|100|0|s|s|t",
            "parse_ident|25 16|||str,strict|true|0|1|0|i|s|t",
            "pg_available_extension_versions||19,25,16,16,16,19,1003,25|o,o,o,o,o,o,o,"
                    + "o|name,version,superuser,trusted,relocatable,schema,requires,"
                    + "comment||0|10|100|s|s|t",
            "pg_backend_pid||||||0|1|0|s|r|t",
            "pg_backup_start|25 16|||label,fast|false|0|1|0|v|r|t",
            "pg_backup_stop|16|16,3220,25,25|i,o,o,o|wait_for_archive,lsn,labelfile,"
                    + "spcmapfile|true|0|1|0|v|r|t",
            "pg_blocking_pids|23|||||0|1|0|v|s|t",
            "pg_cancel_backend|23|||||0|1|0|v|s|t",
            "pg_collation_is_visible|26|||||0|10|0|s|s|t",
            "pg_conf_load_time||||||0|1|0|s|r|t",
            "pg_control_checkpoint||3220,3220,25,23,23,16,25,26,28,28,28,26,28,28,26,28,"
                    + "28,1184|o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o|checkpoint_lsn,redo_lsn,"
                    + "redo_wal_file,timeline_id,prev_timeline_id,full_page_writes,next_xid,"
                    + "next_oid,next_multixact_id,next_multi_offset,oldest_xid,oldest_xid_dbid,"
                    + "oldest_active_xid,oldest_multi_xid,oldest_multi_dbid,oldest_commit_ts_xid,"
                    + "newest_commit_ts_xid,checkpoint_time||0|1|0|v|s|t",
            "pg_control_init||23,23,23,23,23,23,23,23,23,16,23,16|o,o,o,o,o,o,o,o,o,o,o,"
                    + "o|max_data_alignment,database_block_size,blocks_per_segment,wal_block_size,"
                    + "bytes_per_wal_segment,max_identifier_length,max_index_columns,"
                    + "max_toast_chunk_size,large_object_chunk_size,float8_pass_by_value,"
                    + "data_page_checksum_version,default_char_signedness||0|1|0|v|s|t",
            "pg_control_recovery||3220,23,3220,3220,16|o,o,o,o,o|min_recovery_end_lsn,"
                    + "min_recovery_end_timeline,backup_start_lsn,backup_end_lsn,"
                    + "end_of_backup_record_required||0|1|0|v|s|t",
            "pg_control_system||23,23,20,1184|o,o,o,o|pg_control_version,"
                    + "catalog_version_no,system_identifier,pg_control_last_modified||0|1|0|v|s|t",
            "pg_conversion_is_visible|26|||||0|10|0|s|s|t",
            "pg_create_logical_replication_slot|19 19 16 16 16|19,19,16,16,16,19,3220|i,"
                    + "i,i,i,i,o,o|slot_name,plugin,temporary,twophase,failover,slot_name,"
                    + "lsn|false~false~false|0|1|0|v|u|t",
            "pg_create_physical_replication_slot|19 16 16|19,16,16,19,3220|i,i,i,o,"
                    + "o|slot_name,immediately_reserve,temporary,slot_name,"
                    + "lsn|false~false|0|1|0|v|u|t",
            "pg_create_restore_point|25|||||0|1|0|v|s|t",
            "pg_current_logfile||||||0|1|0|v|s|f",
            "pg_current_logfile|25|||||0|1|0|v|s|f",
            "pg_current_wal_flush_lsn||||||0|1|0|v|s|t",
            "pg_current_wal_insert_lsn||||||0|1|0|v|s|t",
            "pg_current_wal_lsn||||||0|1|0|v|s|t",
            "pg_current_xact_id||||||0|1|0|s|u|t",
            "pg_current_xact_id_if_assigned||||||0|1|0|s|u|t",
            "pg_database_size|19|||||0|1|0|v|s|t",
            "pg_database_size|26|||||0|1|0|v|s|t",
            "pg_dependencies_recv|2281|||||0|1|0|s|s|t",
            "pg_dependencies_send|3402|||||0|1|0|s|s|t",
            "pg_drop_replication_slot|19|||||0|1|0|v|u|t",
            "pg_event_trigger_ddl_commands||26,26,23,25,25,25,25,16,32|o,o,o,o,o,o,o,o,"
                    + "o|classid,objid,objsubid,command_tag,object_type,schema_name,"
                    + "object_identity,in_extension,command||0|10|100|s|r|t",
            "pg_event_trigger_dropped_objects||26,26,23,16,16,16,25,25,25,25,1009,1009|o,"
                    + "o,o,o,o,o,o,o,o,o,o,o|classid,objid,objsubid,original,normal,is_temporary,"
                    + "object_type,schema_name,object_name,object_identity,address_names,"
                    + "address_args||0|10|100|s|r|t",
            "pg_event_trigger_table_rewrite_oid||26|o|oid||0|1|0|s|r|t",
            "pg_event_trigger_table_rewrite_reason||||||0|1|0|s|r|t",
            "pg_export_snapshot||||||0|1|0|v|u|t",
            "pg_function_is_visible|26|||||0|10|0|s|s|t",
            "pg_get_acl|26 26 23|||classid,objid,objsubid||0|1|0|s|s|t",
            "pg_get_keywords||25,18,16,25,25|o,o,o,o,o|word,catcode,barelabel,catdesc,"
                    + "baredesc||0|10|500|s|s|t",
            "pg_get_viewdef|25|||||0|1|0|s|r|t",
            "pg_get_viewdef|25 16|||||0|1|0|s|r|t",
            "pg_get_viewdef|26|||||0|1|0|s|r|t",
            "pg_get_viewdef|26 16|||||0|1|0|s|r|t",
            "pg_get_viewdef|26 23|||||0|1|0|s|r|t",
            "pg_indexes_size|2205|||||0|1|0|v|s|t",
            "pg_is_in_recovery||||||0|1|0|v|s|t",
            "pg_is_wal_replay_paused||||||0|1|0|v|s|t",
            "pg_last_wal_receive_lsn||||||0|1|0|v|s|t",
            "pg_last_wal_replay_lsn||||||0|1|0|v|s|t",
            "pg_last_xact_replay_timestamp||||||0|1|0|v|s|t",
            "pg_listening_channels||||||0|1|10|s|r|t",
            "pg_log_backend_memory_contexts|23|||||0|1|0|v|s|t",
            "pg_logical_slot_get_changes|19 3220 23 1009|19,3220,23,1009,3220,28,25|i,i,"
                    + "i,v,o,o,o|slot_name,upto_lsn,upto_nchanges,options,lsn,xid,"
                    + "data|'{}'::text[]|25|1000|1000|v|u|f",
            "pg_logical_slot_peek_changes|19 3220 23 1009|19,3220,23,1009,3220,28,25|i,i,"
                    + "i,v,o,o,o|slot_name,upto_lsn,upto_nchanges,options,lsn,xid,"
                    + "data|'{}'::text[]|25|1000|1000|v|u|f",
            "pg_ls_archive_statusdir||25,20,1184|o,o,o|name,size,modification||0|10|20|v|s|t",
            "pg_ls_dir|25|||||0|1|1000|v|s|t",
            "pg_ls_dir|25 16 16|||||0|1|1000|v|s|t",
            "pg_ls_logdir||25,20,1184|o,o,o|name,size,modification||0|10|20|v|s|t",
            "pg_ls_tmpdir||25,20,1184|o,o,o|name,size,modification||0|10|20|v|s|t",
            "pg_ls_tmpdir|26|26,25,20,1184|i,o,o,o|tablespace,name,size,"
                    + "modification||0|10|20|v|s|t",
            "pg_ls_waldir||25,20,1184|o,o,o|name,size,modification||0|10|20|v|s|t",
            "pg_mcv_list_recv|2281|||||0|1|0|s|s|t",
            "pg_mcv_list_send|5017|||||0|1|0|s|s|t",
            "pg_my_temp_schema||||||0|1|0|s|r|t",
            "pg_ndistinct_recv|2281|||||0|1|0|s|s|t",
            "pg_ndistinct_send|3361|||||0|1|0|s|s|t",
            "pg_node_tree_recv|2281|||||0|1|0|s|s|t",
            "pg_node_tree_send|194|||||0|1|0|s|s|t",
            "pg_opclass_is_visible|26|||||0|10|0|s|s|t",
            "pg_operator_is_visible|26|||||0|10|0|s|s|t",
            "pg_options_to_table|1009|1009,25,25|i,o,o|options_array,option_name,"
                    + "option_value||0|1|3|s|s|t",
            "pg_partition_ancestors|2205|2205,2205|i,o|partitionid,relid||0|1|10|v|s|t",
            "pg_partition_tree|2205|2205,2205,2205,16,23|i,o,o,o,o|rootrelid,relid,"
                    + "parentrelid,isleaf,level||0|1|1000|v|s|t",
            "pg_promote|16 23|||wait,wait_seconds|true~60|0|1|0|v|s|t",
            "pg_read_binary_file|25|||||0|1|0|v|s|t",
            "pg_read_binary_file|25 16|||||0|1|0|v|s|t",
            "pg_read_binary_file|25 20 20|||||0|1|0|v|s|t",
            "pg_read_binary_file|25 20 20 16|||||0|1|0|v|s|t",
            "pg_read_file|25|||||0|1|0|v|s|t",
            "pg_read_file|25 16|||||0|1|0|v|s|t",
            "pg_read_file|25 20 20|||||0|1|0|v|s|t",
            "pg_read_file|25 20 20 16|||||0|1|0|v|s|t",
            "pg_relation_size|2205|||||0|1|0|v|s|t",
            "pg_relation_size|2205 25|||||0|1|0|v|s|t",
            "pg_reload_conf||||||0|1|0|v|s|t",
            "pg_replication_slot_advance|19 3220|19,3220,19,3220|i,i,o,o|slot_name,"
                    + "upto_lsn,slot_name,end_lsn||0|1|0|v|u|t",
            "pg_rotate_logfile||||||0|1|0|v|s|t",
            "pg_safe_snapshot_blocking_pids|23|||||0|1|0|v|s|t",
            "pg_sequence_last_value|2205|||||0|1|0|v|u|t",
            "pg_show_all_settings||25,25,25,25,25,25,25,25,25,25,25,1009,25,25,25,23,"
                    + "16|o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o|name,setting,unit,category,short_desc,"
                    + "extra_desc,context,vartype,source,min_val,max_val,enumvals,boot_val,"
                    + "reset_val,sourcefile,sourceline,pending_restart||0|1|1000|s|s|t",
            "pg_sleep|701|||||0|1|0|i|s|t",
            "pg_sleep_for|1186|||||0|1|0|v|s|t",
            "pg_sleep_until|1184|||||0|1|0|v|s|t",
            "pg_snapshot_xip|5038|||||0|1|50|i|s|t",
            "pg_stat_file|25|25,20,1184,1184,1184,1184,16|i,o,o,o,o,o,o|filename,size,"
                    + "access,modification,change,creation,isdir||0|1|0|v|s|t",
            "pg_stat_file|25 16|25,16,20,1184,1184,1184,1184,16|i,i,o,o,o,o,o,o|filename,"
                    + "missing_ok,size,access,modification,change,creation,isdir||0|1|0|v|s|t",
            "pg_stat_reset||||||0|1|0|v|s|f",
            "pg_stat_reset_shared|25|||target|NULL::text|0|1|0|v|s|f",
            "pg_stat_reset_single_function_counters|26|||||0|1|0|v|s|t",
            "pg_stat_reset_single_table_counters|26|||||0|1|0|v|s|t",
            "pg_switch_wal||||||0|1|0|v|s|t",
            "pg_table_is_visible|26|||||0|10|0|s|s|t",
            "pg_table_size|2205|||||0|1|0|v|s|t",
            "pg_tablespace_size|19|||||0|1|0|v|s|t",
            "pg_tablespace_size|26|||||0|1|0|v|s|t",
            "pg_terminate_backend|23 20|||pid,timeout|0|0|1|0|v|s|t",
            "pg_total_relation_size|2205|||||0|1|0|v|s|t",
            "pg_ts_config_is_visible|26|||||0|10|0|s|s|t",
            "pg_ts_dict_is_visible|26|||||0|10|0|s|s|t",
            "pg_ts_parser_is_visible|26|||||0|10|0|s|s|t",
            "pg_ts_template_is_visible|26|||||0|10|0|s|s|t",
            "pg_type_is_visible|26|||||0|10|0|s|s|t",
            "pg_wal_replay_pause||||||0|1|0|v|s|t",
            "pg_wal_replay_resume||||||0|1|0|v|s|t",
            "pg_xact_status|5069|||||0|1|0|v|s|t",
            "phraseto_tsquery|25|||||0|100|0|s|s|t",
            "phraseto_tsquery|3734 25|||||0|100|0|i|s|t",
            "plainto_tsquery|25|||||0|100|0|s|s|t",
            "plainto_tsquery|3734 25|||||0|100|0|i|s|t",
            "query_to_xml|25 16 16 25|||query,nulls,tableforest,targetns||0|100|0|v|u|t",
            "random|1700 1700|||min,max||0|1|0|v|r|t",
            "random|20 20|||min,max||0|1|0|v|r|t",
            "random|23 23|||min,max||0|1|0|v|r|t",
            "random_normal|701 701|||mean,stddev|0~1|0|1|0|v|r|t",
            "range_in|2275 26 23|||||0|1|0|s|s|t",
            "range_out|3831|||||0|1|0|s|s|t",
            "range_recv|2281 26 23|||||0|1|0|s|s|t",
            "range_send|3831|||||0|1|0|s|s|t",
            "range_typanalyze|2281|||||0|1|0|s|s|t",
            "record_in|2275 26 23|||||0|1|0|s|s|t",
            "record_out|2249|||||0|1|0|s|s|t",
            "record_recv|2281 26 23|||||0|1|0|s|s|t",
            "record_send|2249|||||0|1|0|s|s|t",
            "regclassin|2275|||||0|1|0|s|s|t",
            "regclassout|2205|||||0|1|0|s|s|t",
            "regcollationin|2275|||||0|1|0|s|s|t",
            "regcollationout|4191|||||0|1|0|s|s|t",
            "regconfigin|2275|||||0|1|0|s|s|t",
            "regconfigout|3734|||||0|1|0|s|s|t",
            "regdictionaryin|2275|||||0|1|0|s|s|t",
            "regdictionaryout|3769|||||0|1|0|s|s|t",
            "regexp_count|25 25|||string,pattern||0|1|0|i|s|t",
            "regexp_count|25 25 23|||string,pattern,start||0|1|0|i|s|t",
            "regexp_count|25 25 23 25|||string,pattern,start,flags||0|1|0|i|s|t",
            "regexp_instr|25 25|||string,pattern||0|1|0|i|s|t",
            "regexp_instr|25 25 23|||string,pattern,start||0|1|0|i|s|t",
            "regexp_instr|25 25 23 23|||string,pattern,start,N||0|1|0|i|s|t",
            "regexp_instr|25 25 23 23 23|||string,pattern,start,N,endoption||0|1|0|i|s|t",
            "regexp_instr|25 25 23 23 23 25|||string,pattern,start,N,endoption,flags||0|1|0|i|s|t",
            "regexp_instr|25 25 23 23 23 25 23|||string,pattern,start,N,endoption,flags,"
                    + "subexpr||0|1|0|i|s|t",
            "regexp_like|25 25|||string,pattern||0|1|0|i|s|t",
            "regexp_like|25 25 25|||string,pattern,flags||0|1|0|i|s|t",
            "regexp_match|25 25|||string,pattern||0|1|0|i|s|t",
            "regexp_match|25 25 25|||string,pattern,flags||0|1|0|i|s|t",
            "regexp_matches|25 25|||string,pattern||0|1|1|i|s|t",
            "regexp_matches|25 25 25|||string,pattern,flags||0|1|10|i|s|t",
            "regexp_replace|25 25 25|||string,pattern,replacement||0|1|0|i|s|t",
            "regexp_replace|25 25 25 23|||string,pattern,replacement,start||0|1|0|i|s|t",
            "regexp_replace|25 25 25 23 23|||string,pattern,replacement,start,N||0|1|0|i|s|t",
            "regexp_replace|25 25 25 23 23 25|||string,pattern,replacement,start,N,"
                    + "flags||0|1|0|i|s|t",
            "regexp_replace|25 25 25 25|||string,pattern,replacement,flags||0|1|0|i|s|t",
            "regexp_split_to_array|25 25|||string,pattern||0|1|0|i|s|t",
            "regexp_split_to_array|25 25 25|||string,pattern,flags||0|1|0|i|s|t",
            "regexp_split_to_table|25 25|||string,pattern||0|1|1000|i|s|t",
            "regexp_split_to_table|25 25 25|||string,pattern,flags||0|1|1000|i|s|t",
            "regexp_substr|25 25|||string,pattern||0|1|0|i|s|t",
            "regexp_substr|25 25 23|||string,pattern,start||0|1|0|i|s|t",
            "regexp_substr|25 25 23 23|||string,pattern,start,N||0|1|0|i|s|t",
            "regexp_substr|25 25 23 23 25|||string,pattern,start,N,flags||0|1|0|i|s|t",
            "regexp_substr|25 25 23 23 25 23|||string,pattern,start,N,flags,subexpr||0|1|0|i|s|t",
            "regnamespacein|2275|||||0|1|0|s|s|t",
            "regnamespaceout|4089|||||0|1|0|s|s|t",
            "regoperatorin|2275|||||0|1|0|s|s|t",
            "regoperatorout|2204|||||0|1|0|s|s|t",
            "regoperin|2275|||||0|1|0|s|s|t",
            "regoperout|2203|||||0|1|0|s|s|t",
            "regprocedurein|2275|||||0|1|0|s|s|t",
            "regprocedureout|2202|||||0|1|0|s|s|t",
            "regprocin|2275|||||0|1|0|s|s|t",
            "regprocout|24|||||0|1|0|s|s|t",
            "regrolein|2275|||||0|1|0|s|s|t",
            "regroleout|4096|||||0|1|0|s|s|t",
            "regtypein|2275|||||0|1|0|s|s|t",
            "regtypeout|2206|||||0|1|0|s|s|t",
            "schema_to_xml|19 16 16 25|||schema,nulls,tableforest,targetns||0|100|0|s|r|t",
            "set_config|25 25 16|||||0|1|0|v|u|f",
            "setval|2205 20|||||0|1|0|v|u|t",
            "setval|2205 20 16|||||0|1|0|v|u|t",
            "shobj_description|26 19|||||0|100|0|s|s|t",
            "spghandler|2281|||||0|1|0|v|s|t",
            "string_agg|17 17|||value,delimiter||0|1|0|i|s|f",
            "string_agg|25 25|||value,delimiter||0|1|0|i|s|f",
            "suppress_redundant_updates_trigger||||||0|1|0|v|s|t",
            "table_am_handler_in|2275|||||0|1|0|i|s|f",
            "table_to_xml|2205 16 16 25|||tbl,nulls,tableforest,targetns||0|100|0|s|r|t",
            "textanycat|25 2776|||||0|1|0|s|s|t",
            "textrecv|2281|||||0|1|0|s|s|t",
            "textsend|25|||||0|1|0|s|s|t",
            "time_in|2275 26 23|||||0|1|0|s|s|t",
            "timeofday||||||0|1|0|v|s|t",
            "timestamp_eq_timestamptz|1114 1184|||||0|1|0|s|s|t",
            "timestamp_ge_timestamptz|1114 1184|||||0|1|0|s|s|t",
            "timestamp_gt_timestamptz|1114 1184|||||0|1|0|s|s|t",
            "timestamp_in|2275 26 23|||||0|1|0|s|s|t",
            "timestamp_le_timestamptz|1114 1184|||||0|1|0|s|s|t",
            "timestamp_lt_timestamptz|1114 1184|||||0|1|0|s|s|t",
            "timestamp_ne_timestamptz|1114 1184|||||0|1|0|s|s|t",
            "timestamp_out|1114|||||0|1|0|s|s|t",
            "timestamptz_eq_date|1184 1082|||||0|1|0|s|s|t",
            "timestamptz_eq_timestamp|1184 1114|||||0|1|0|s|s|t",
            "timestamptz_ge_date|1184 1082|||||0|1|0|s|s|t",
            "timestamptz_ge_timestamp|1184 1114|||||0|1|0|s|s|t",
            "timestamptz_gt_date|1184 1082|||||0|1|0|s|s|t",
            "timestamptz_gt_timestamp|1184 1114|||||0|1|0|s|s|t",
            "timestamptz_in|2275 26 23|||||0|1|0|s|s|t",
            "timestamptz_le_date|1184 1082|||||0|1|0|s|s|t",
            "timestamptz_le_timestamp|1184 1114|||||0|1|0|s|s|t",
            "timestamptz_lt_date|1184 1082|||||0|1|0|s|s|t",
            "timestamptz_lt_timestamp|1184 1114|||||0|1|0|s|s|t",
            "timestamptz_mi_interval|1184 1186|||||0|1|0|s|s|t",
            "timestamptz_ne_date|1184 1082|||||0|1|0|s|s|t",
            "timestamptz_ne_timestamp|1184 1114|||||0|1|0|s|s|t",
            "timestamptz_out|1184|||||0|1|0|s|s|t",
            "timestamptz_pl_interval|1184 1186|||||0|1|0|s|s|t",
            "timetz_in|2275 26 23|||||0|1|0|s|s|t",
            "to_tsquery|25|||||0|100|0|s|s|t",
            "to_tsquery|3734 25|||||0|100|0|i|s|t",
            "to_tsvector|114|||||0|100|0|s|s|t",
            "to_tsvector|25|||||0|100|0|s|s|t",
            "to_tsvector|3734 114|||||0|100|0|i|s|t",
            "to_tsvector|3734 25|||||0|100|0|i|s|t",
            "to_tsvector|3734 3802|||||0|100|0|i|s|t",
            "to_tsvector|3802|||||0|100|0|s|s|t",
            "trigger_in|2275|||||0|1|0|i|s|f",
            "ts_debug|25|25,25,25,25,3770,3769,1009|i,o,o,o,o,o,o|document,alias,"
                    + "description,token,dictionaries,dictionary,lexemes||0|100|1000|s|s|t",
            "ts_debug|3734 25|3734,25,25,25,25,3770,3769,1009|i,i,o,o,o,o,o,o|config,"
                    + "document,alias,description,token,dictionaries,dictionary,"
                    + "lexemes||0|100|1000|s|s|t",
            "ts_headline|114 3615|||||0|100|0|s|s|t",
            "ts_headline|114 3615 25|||||0|100|0|s|s|t",
            "ts_headline|25 3615|||||0|100|0|s|s|t",
            "ts_headline|25 3615 25|||||0|100|0|s|s|t",
            "ts_headline|3734 114 3615|||||0|100|0|i|s|t",
            "ts_headline|3734 114 3615 25|||||0|100|0|i|s|t",
            "ts_headline|3734 25 3615|||||0|100|0|i|s|t",
            "ts_headline|3734 25 3615 25|||||0|100|0|i|s|t",
            "ts_headline|3734 3802 3615|||||0|100|0|i|s|t",
            "ts_headline|3734 3802 3615 25|||||0|100|0|i|s|t",
            "ts_headline|3802 3615|||||0|100|0|s|s|t",
            "ts_headline|3802 3615 25|||||0|100|0|s|s|t",
            "ts_match_tq|25 3615|||||0|100|0|s|s|t",
            "ts_match_tt|25 25|||||0|100|0|s|s|t",
            "ts_parse|25 25|25,25,23,25|i,i,o,o|parser_name,txt,tokid,token||0|1|1000|s|s|t",
            "ts_parse|26 25|26,25,23,25|i,i,o,o|parser_oid,txt,tokid,token||0|1|1000|i|s|t",
            "ts_rewrite|3615 25|||||0|100|0|v|u|t",
            "ts_stat|25|25,25,23,23|i,o,o,o|query,word,ndoc,nentry||0|10|10000|v|u|t",
            "ts_stat|25 25|25,25,25,23,23|i,i,o,o,o|query,weights,word,ndoc,"
                    + "nentry||0|10|10000|v|u|t",
            "ts_token_type|25|25,23,25,25|i,o,o,o|parser_name,tokid,alias,"
                    + "description||0|1|16|s|s|t",
            "ts_token_type|26|26,23,25,25|i,o,o,o|parser_oid,tokid,alias,description||0|1|16|i|s|t",
            "ts_typanalyze|2281|||||0|1|0|s|s|t",
            "tsm_handler_in|2275|||||0|1|0|i|s|f",
            "tsmultirange|3909|3909|v|||3908|1|0|i|s|t",
            "tstzmultirange|3911|3911|v|||3910|1|0|i|s|t",
            "txid_current||||||0|1|0|s|u|t",
            "txid_current_if_assigned||||||0|1|0|s|u|t",
            "txid_snapshot_xip|2970|||||0|1|50|i|s|t",
            "txid_status|20|||||0|1|0|v|s|t",
            "unnest|2277|||||0|1|100|i|s|t",
            "unnest|3614|3614,25,1005,1009|i,o,o,o|tsvector,lexeme,positions,weights||0|1|10|i|s|t",
            "unnest|4537|||||0|1|100|i|s|t",
            "uuidv4||||||0|1|0|v|s|t",
            "uuidv7||||||0|1|0|v|s|t",
            "uuidv7|1186|||shift||0|1|0|v|s|t",
            "varcharrecv|2281 26 23|||||0|1|0|s|s|t",
            "varcharsend|1043|||||0|1|0|s|s|t",
            "websearch_to_tsquery|25|||||0|100|0|s|s|t",
            "websearch_to_tsquery|3734 25|||||0|100|0|i|s|t",
            "xml_in|2275|||||0|1|0|s|s|t",
            "xml_recv|2281|||||0|1|0|s|s|t",
            "xml_send|142|||||0|1|0|s|s|t",
        };

        /** Declared after the rows: a field initialiser runs in source order and would read null. */
        static final java.util.Map<String, String[]> BY_SIGNATURE = build();

        private static java.util.Map<String, String[]> build() {
            java.util.Map<String, String[]> map =
                    new java.util.HashMap<String, String[]>(ROWS.length * 2);
            for (String row : ROWS) {
                String[] f = row.split("\\|", -1);
                map.put(signatureKey(f[0], f[1]), f);
            }
            return java.util.Collections.unmodifiableMap(map);
        }
    }

    private static String signatureKey(String name, String argTypes) {
        return name.toLowerCase(java.util.Locale.ROOT) + "(" + argTypes.trim() + ")";
    }

    /**
     * PostgreSQL's own pg_proc row for this exact signature, or null where PostgreSQL's answer is
     * the one memgres already derives.
     *
     * <p>Fields as documented on {@link Recorded}. Read only for functions in pg_catalog: a user
     * function that happens to share a built-in's name and argument list is that user's function,
     * and describing it with PostgreSQL's numbers would be a lie about their code.
     */
    static String[] recordedProcRow(String name, String argTypes) {
        if (name == null || argTypes == null) return null;
        return Recorded.BY_SIGNATURE.get(signatureKey(name, argTypes));
    }

    /**
     * The type a variadic parameter's tail collects into, which is what provariadic names.
     *
     * <p>PostgreSQL records the ELEMENT type, not the array the parameter is declared as:
     * {@code json_extract_path(json, VARIADIC text[])} has provariadic 25, not 1009, and
     * {@code datemultirange(VARIADIC daterange[])} has 3912, not 3913. memgres recorded the
     * declared type on all twelve of its variadic signatures, so a client working out how many
     * arguments the tail may take read one level of array too many. {@code "any"} is its own
     * element type and is returned unchanged.
     */
    static int variadicElementType(int declaredType) {
        switch (declaredType) {
            case 1009: return 25;     // text[]     -> text
            case 3905: return 3904;   // int4range[]  -> int4range
            case 3907: return 3906;   // numrange[]   -> numrange
            case 3909: return 3908;   // tsrange[]    -> tsrange
            case 3911: return 3910;   // tstzrange[]  -> tstzrange
            case 3913: return 3912;   // daterange[]  -> daterange
            case 3927: return 3926;   // int8range[]  -> int8range
            default: return declaredType;
        }
    }
}
