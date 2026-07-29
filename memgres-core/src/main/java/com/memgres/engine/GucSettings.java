package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * GUC (Grand Unified Configuration) settings manager.
 * Provides sensible defaults for PostgreSQL configuration parameters.
 * Stores session-level SET overrides.
 */
public class GucSettings {

    private static final Map<String, String> DEFAULTS = new LinkedHashMap<>();
    static {
        // Connection
        DEFAULTS.put("server_version", "18.0");
        DEFAULTS.put("server_version_num", "180000");
        DEFAULTS.put("server_encoding", "UTF8");
        DEFAULTS.put("client_encoding", "UTF8");
        DEFAULTS.put("client_min_messages", "notice");
        DEFAULTS.put("is_superuser", "on");
        DEFAULTS.put("session_authorization", "test");
        DEFAULTS.put("role", "test");

        // Search path
        DEFAULTS.put("search_path", "\"$user\", public");

        // Transaction
        DEFAULTS.put("transaction_isolation", "read committed");
        DEFAULTS.put("default_transaction_isolation", "read committed");
        DEFAULTS.put("transaction_read_only", "off");
        DEFAULTS.put("default_transaction_read_only", "off");
        DEFAULTS.put("transaction_deferrable", "off");
        DEFAULTS.put("default_transaction_deferrable", "off");

        // Date/Time
        DEFAULTS.put("timezone", "UTC");
        DEFAULTS.put("datestyle", "ISO, MDY");
        DEFAULTS.put("intervalstyle", "postgres");
        DEFAULTS.put("lc_messages", "en_US.UTF-8");
        DEFAULTS.put("lc_monetary", "en_US.UTF-8");
        DEFAULTS.put("lc_numeric", "en_US.UTF-8");
        DEFAULTS.put("lc_time", "en_US.UTF-8");

        // Memory / Performance (sensible in-memory defaults)
        DEFAULTS.put("shared_buffers", "128MB");
        DEFAULTS.put("work_mem", "4MB");
        DEFAULTS.put("maintenance_work_mem", "64MB");
        DEFAULTS.put("effective_cache_size", "4GB");
        DEFAULTS.put("max_connections", "100");
        DEFAULTS.put("max_worker_processes", "8");
        DEFAULTS.put("max_prepared_transactions", "0");

        // Query planning
        DEFAULTS.put("random_page_cost", "4.0");
        DEFAULTS.put("seq_page_cost", "1.0");
        DEFAULTS.put("cpu_tuple_cost", "0.01");
        DEFAULTS.put("cpu_index_tuple_cost", "0.005");
        DEFAULTS.put("cpu_operator_cost", "0.0025");
        DEFAULTS.put("enable_seqscan", "on");
        DEFAULTS.put("enable_indexscan", "on");
        DEFAULTS.put("enable_hashjoin", "on");
        DEFAULTS.put("enable_mergejoin", "on");
        DEFAULTS.put("enable_nestloop", "on");
        DEFAULTS.put("enable_hashagg", "on");
        DEFAULTS.put("enable_partitionwise_join", "off");
        DEFAULTS.put("enable_presorted_aggregate", "on");
        DEFAULTS.put("enable_incremental_sort", "on");
        DEFAULTS.put("parallel_leader_participation", "on");
        DEFAULTS.put("min_parallel_table_scan_size", "8MB");
        DEFAULTS.put("plan_cache_mode", "auto");

        // Statistics / monitoring
        DEFAULTS.put("compute_query_id", "auto");
        DEFAULTS.put("track_functions", "none");
        DEFAULTS.put("track_activity_query_size", "1024");

        // Logging
        DEFAULTS.put("log_statement", "none");
        DEFAULTS.put("log_min_duration_statement", "-1");
        DEFAULTS.put("log_lock_failures", "off");

        // Replication / hot standby
        DEFAULTS.put("in_hot_standby", "off");

        // WAL / Replication (not applicable, but return defaults)
        DEFAULTS.put("wal_level", "replica");
        DEFAULTS.put("max_wal_senders", "10");
        DEFAULTS.put("synchronous_commit", "on");
        DEFAULTS.put("session_replication_role", "origin"); // pg_restore --disable-triggers sets to "replica"

        // Auth / SSL
        DEFAULTS.put("ssl", "off");
        DEFAULTS.put("password_encryption", "scram-sha-256");

        // Misc
        DEFAULTS.put("standard_conforming_strings", "on");
        DEFAULTS.put("bytea_output", "hex");
        DEFAULTS.put("xmloption", "content");
        DEFAULTS.put("application_name", "memgres");
        DEFAULTS.put("idle_in_transaction_session_timeout", "0");
        DEFAULTS.put("statement_timeout", "0");
        DEFAULTS.put("lock_timeout", "0");
        DEFAULTS.put("transaction_timeout", "0");
        DEFAULTS.put("row_security", "on");
        DEFAULTS.put("check_function_bodies", "on");
        DEFAULTS.put("default_tablespace", "");
        DEFAULTS.put("default_table_access_method", "heap");
        DEFAULTS.put("default_toast_compression", "pglz");
        DEFAULTS.put("temp_tablespaces", "");
        DEFAULTS.put("max_parallel_workers_per_gather", "2");
        DEFAULTS.put("jit", "off");
        DEFAULTS.put("integer_datetimes", "on");
        DEFAULTS.put("synchronize_seqscans", "on");

        // Float/numeric display (PG default is 1; JDBC drivers typically SET it to 3)
        DEFAULTS.put("extra_float_digits", "1");

        // Text search
        DEFAULTS.put("default_text_search_config", "pg_catalog.simple");

        // Index settings (queried by JDBC driver for DatabaseMetaData)
        DEFAULTS.put("max_index_keys", "32");

        // Settings a client or pool reads at startup to decide how to talk to the server. They
        // describe how input is parsed and how much fits in a page, so an unrecognised name is
        // not a missing feature to a driver — it is a server it does not know how to address.
        DEFAULTS.put("array_nulls", "on");
        DEFAULTS.put("backslash_quote", "safe_encoding");
        DEFAULTS.put("block_size", "8192");
        DEFAULTS.put("segment_size", "131072");
        DEFAULTS.put("wal_block_size", "8192");
        DEFAULTS.put("wal_segment_size", "16777216");
        DEFAULTS.put("data_checksums", "off");
        DEFAULTS.put("debug_assertions", "off");
        DEFAULTS.put("escape_string_warning", "on");
        DEFAULTS.put("quote_all_identifiers", "off");
        DEFAULTS.put("lo_compat_privileges", "off");
        DEFAULTS.put("operator_precedence_warning", "off");
        DEFAULTS.put("exit_on_error", "off");
        DEFAULTS.put("restart_after_crash", "on");

        // Background activity. Nothing here runs in an in-memory server, but the names are read
        // by monitoring tools and by anything that reports how a database is configured.
        DEFAULTS.put("autovacuum", "on");
        DEFAULTS.put("autovacuum_max_workers", "3");
        DEFAULTS.put("autovacuum_naptime", "1min");
        DEFAULTS.put("autovacuum_vacuum_threshold", "50");
        DEFAULTS.put("autovacuum_vacuum_insert_threshold", "1000");
        DEFAULTS.put("autovacuum_analyze_threshold", "50");
        DEFAULTS.put("autovacuum_vacuum_scale_factor", "0.2");
        DEFAULTS.put("autovacuum_vacuum_insert_scale_factor", "0.2");
        DEFAULTS.put("autovacuum_analyze_scale_factor", "0.1");
        DEFAULTS.put("autovacuum_freeze_max_age", "200000000");
        DEFAULTS.put("autovacuum_multixact_freeze_max_age", "400000000");
        DEFAULTS.put("autovacuum_vacuum_cost_delay", "2ms");
        DEFAULTS.put("autovacuum_vacuum_cost_limit", "-1");
        DEFAULTS.put("archive_mode", "off");
        DEFAULTS.put("archive_command", "");
        DEFAULTS.put("archive_library", "");
        DEFAULTS.put("archive_timeout", "0");
        DEFAULTS.put("bgwriter_delay", "200ms");
        DEFAULTS.put("bgwriter_lru_maxpages", "100");
        DEFAULTS.put("bgwriter_lru_multiplier", "2");
        DEFAULTS.put("bgwriter_flush_after", "512kB");
        DEFAULTS.put("checkpoint_timeout", "5min");
        DEFAULTS.put("checkpoint_completion_target", "0.9");
        DEFAULTS.put("checkpoint_flush_after", "256kB");
        DEFAULTS.put("checkpoint_warning", "30s");
        DEFAULTS.put("max_wal_size", "1GB");
        DEFAULTS.put("min_wal_size", "80MB");
        DEFAULTS.put("wal_level", "replica");
        DEFAULTS.put("wal_compression", "off");
        DEFAULTS.put("fsync", "on");
        DEFAULTS.put("full_page_writes", "on");
        DEFAULTS.put("synchronous_commit", "on");
        DEFAULTS.put("effective_cache_size", "4GB");
        DEFAULTS.put("shared_buffers", "128MB");
        DEFAULTS.put("maintenance_work_mem", "64MB");
        DEFAULTS.put("temp_buffers", "8MB");
        DEFAULTS.put("max_worker_processes", "8");
        DEFAULTS.put("max_parallel_workers", "8");
        DEFAULTS.put("max_parallel_workers_per_gather", "2");
        DEFAULTS.put("max_parallel_maintenance_workers", "2");
        DEFAULTS.put("max_replication_slots", "10");
        DEFAULTS.put("max_wal_senders", "10");
        DEFAULTS.put("max_locks_per_transaction", "64");
        DEFAULTS.put("max_pred_locks_per_transaction", "64");
        DEFAULTS.put("deadlock_timeout", "1s");
        DEFAULTS.put("effective_io_concurrency", "16");
        DEFAULTS.put("random_page_cost", "4");
        DEFAULTS.put("seq_page_cost", "1");
        DEFAULTS.put("cpu_tuple_cost", "0.01");
        DEFAULTS.put("cpu_index_tuple_cost", "0.005");
        DEFAULTS.put("cpu_operator_cost", "0.0025");
        DEFAULTS.put("jit", "on");
        DEFAULTS.put("log_min_messages", "warning");
        DEFAULTS.put("log_min_error_statement", "error");
        DEFAULTS.put("log_statement", "none");
        DEFAULTS.put("logging_collector", "off");
        DEFAULTS.put("log_timezone", "UTC");
        DEFAULTS.put("track_activities", "on");
        DEFAULTS.put("track_counts", "on");
        DEFAULTS.put("track_io_timing", "off");
        DEFAULTS.put("track_functions", "none");
        DEFAULTS.put("ssl", "off");
        DEFAULTS.put("password_encryption", "scram-sha-256");
        DEFAULTS.put("hot_standby", "on");
        DEFAULTS.put("idle_in_transaction_session_timeout", "0");
        DEFAULTS.put("idle_session_timeout", "0");
        DEFAULTS.put("transaction_timeout", "0");
        DEFAULTS.put("temp_file_limit", "-1");
        DEFAULTS.put("vacuum_freeze_min_age", "50000000");
        DEFAULTS.put("vacuum_freeze_table_age", "150000000");
        DEFAULTS.put("vacuum_cost_delay", "0");
        DEFAULTS.put("vacuum_cost_limit", "200");
    }

    /** Canonical display names for parameters that use mixed-case in PG (e.g. "TimeZone"). */
    private static final Map<String, String> CANONICAL_NAMES = new LinkedHashMap<>();
    static {
        CANONICAL_NAMES.put("timezone", "TimeZone");
        CANONICAL_NAMES.put("datestyle", "DateStyle");
        CANONICAL_NAMES.put("intervalstyle", "IntervalStyle");
    }

    private final Map<String, String> sessionOverrides = new LinkedHashMap<>();
    private final Map<String, String> transactionOverrides = new LinkedHashMap<>();
    private final Map<String, String> bootDefaults = new LinkedHashMap<>();
    // L7: custom (dotted) parameters referenced in the session become placeholders.
    // PG keeps them defined with an empty-string value after RESET, so current_setting
    // returns '' rather than raising an "unrecognized parameter" error.
    private final Set<String> customPlaceholders = new HashSet<>();

    /** Set a session-level parameter. */
    public void set(String name, String value) {
        // Normalize boolean-like values to lowercase (PG convention).
        // The parser uppercases keywords like ON/OFF/TRUE/FALSE/YES/NO,
        // but JDBC drivers expect lowercase for ParameterStatus messages.
        String normalized = value;
        if (normalized != null) {
            String upper = normalized.trim().toUpperCase();
            if ("ON".equals(upper) || "OFF".equals(upper)
                    || "TRUE".equals(upper) || "FALSE".equals(upper)
                    || "YES".equals(upper) || "NO".equals(upper)) {
                normalized = normalized.trim().toLowerCase();
            }
        }
        String key = name.toLowerCase();
        // L7: remember custom (dotted) parameters so RESET keeps them as an empty placeholder.
        if (key.indexOf('.') >= 0) {
            customPlaceholders.add(key);
        }
        sessionOverrides.put(key, normalized);
    }

    /** Set a transaction-scoped (LOCAL) parameter that reverts on commit/rollback. */
    public void setLocal(String name, String value) {
        transactionOverrides.put(name.toLowerCase(), value);
    }

    /** Clear all transaction-scoped overrides (called on commit/rollback). */
    public void clearTransactionOverrides() {
        transactionOverrides.clear();
    }

    /** Snapshot both override layers so ROLLBACK TO SAVEPOINT can undo SET / SET LOCAL. */
    public Map<String, String> snapshotTransactionOverrides() {
        return new LinkedHashMap<>(transactionOverrides);
    }

    /** Restore the transaction-scoped overrides captured by {@link #snapshotTransactionOverrides()}. */
    public void restoreTransactionOverrides(Map<String, String> snapshot) {
        transactionOverrides.clear();
        transactionOverrides.putAll(snapshot);
    }

    /** Reset a single parameter to default. */
    public void reset(String name) {
        String key = name.toLowerCase();
        // L7: a custom placeholder stays defined with an empty value after RESET (PG behavior).
        if (customPlaceholders.contains(key)) {
            sessionOverrides.put(key, "");
        } else {
            sessionOverrides.remove(key);
        }
    }

    /** Reset all session parameters. */
    public void resetAll() {
        sessionOverrides.clear();
        customPlaceholders.clear();
    }

    /** Set a boot-time default that overrides the static default (e.g., for session_authorization). */
    public void setBootDefault(String name, String value) {
        bootDefaults.put(name.toLowerCase(), value);
    }

    /**
     * The per-transaction settings and the session default each supplies when a transaction
     * starts. Until the transaction says otherwise, reading one has to report the other.
     */
    private static final Map<String, String> TRANSACTION_DEFAULTS = new LinkedHashMap<>();
    static {
        TRANSACTION_DEFAULTS.put("transaction_isolation", "default_transaction_isolation");
        TRANSACTION_DEFAULTS.put("transaction_read_only", "default_transaction_read_only");
        TRANSACTION_DEFAULTS.put("transaction_deferrable", "default_transaction_deferrable");
    }

    /** Get a parameter value (transaction override, then session override, then boot default, then static default). */
    public String get(String name) {
        String key = name.toLowerCase();
        String val = transactionOverrides.get(key);
        if (val != null) return val;
        val = sessionOverrides.get(key);
        if (val != null) return val;
        String sessionDefault = TRANSACTION_DEFAULTS.get(key);
        if (sessionDefault != null) {
            String derived = get(sessionDefault);
            if (derived != null && !derived.isEmpty()) return derived;
        }
        val = bootDefaults.get(key);
        if (val != null) return val;
        return DEFAULTS.getOrDefault(key, null);
    }

    /** Set of timeout parameters whose display value should be human-formatted by PG. */
    private static final Set<String> TIMEOUT_PARAMS = Cols.setOf(
        "statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout",
        "idle_session_timeout", "authentication_timeout", "transaction_timeout");

    /**
     * Get a parameter value formatted for display (SHOW).
     * For timeout parameters, PG normalizes plain millisecond integers to human-friendly form
     * (e.g., "5000" -> "5s", "60000" -> "1min", "0" stays "0").
     */
    public String getForDisplay(String name) {
        String val = get(name);
        if (val == null) return null;
        String key = name.toLowerCase();
        if (TIMEOUT_PARAMS.contains(key)) {
            return formatTimeoutForDisplay(val);
        }
        // PG always displays boolean GUC values in lowercase (on/off)
        if (val.equalsIgnoreCase("on") || val.equalsIgnoreCase("off")
                || val.equalsIgnoreCase("true") || val.equalsIgnoreCase("false")
                || val.equalsIgnoreCase("yes") || val.equalsIgnoreCase("no")) {
            return val.toLowerCase();
        }
        return val;
    }

    /**
     * Format a timeout value for SHOW display.
     * Plain integer values (milliseconds) are converted to the shortest human unit:
     * 0 -> "0", 100 -> "100ms", 5000 -> "5s", 60000 -> "1min", 3600000 -> "1h", 86400000 -> "1d".
     * Values already with a unit suffix are returned as-is.
     */
    private static String formatTimeoutForDisplay(String value) {
        if (value == null || value.isEmpty()) return value;
        // If it already has a unit suffix, return as-is
        if (value.matches(".*[a-zA-Z]$")) return value;
        long ms;
        try {
            ms = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return value;
        }
        if (ms == 0) return "0";
        if (ms % 86400000 == 0) return (ms / 86400000) + "d";
        if (ms % 3600000 == 0) return (ms / 3600000) + "h";
        if (ms % 60000 == 0) return (ms / 60000) + "min";
        if (ms % 1000 == 0) return (ms / 1000) + "s";
        return ms + "ms";
    }

    /** Take a snapshot of session overrides for transactional rollback (M13). */
    public Map<String, String> snapshotSessionOverrides() {
        return new LinkedHashMap<>(sessionOverrides);
    }

    /** Restore session overrides from a snapshot (M13 — plain SET rollback). */
    public void restoreSessionOverrides(Map<String, String> snapshot) {
        sessionOverrides.clear();
        sessionOverrides.putAll(snapshot);
    }

    /** Check if a parameter has been explicitly set at the session level (not just the default). */
    public boolean hasSessionOverride(String name) {
        return sessionOverrides.containsKey(name.toLowerCase()) || transactionOverrides.containsKey(name.toLowerCase());
    }

    /** Check if a parameter name is known (either in defaults or session overrides). */
    public boolean isKnown(String name) {
        String key = name.toLowerCase();
        return DEFAULTS.containsKey(key) || sessionOverrides.containsKey(key);
    }

    /** Get the canonical (display) name for a parameter, preserving PG's mixed-case conventions. */
    public String getCanonicalName(String name) {
        String canonical = CANONICAL_NAMES.get(name.toLowerCase());
        return canonical != null ? canonical : name.toLowerCase();
    }

    /** Get all known parameters for SHOW ALL. */
    public Map<String, String> getAll() {
        Map<String, String> result = new LinkedHashMap<>(DEFAULTS);
        result.putAll(sessionOverrides);
        return result;
    }

    /**
     * Parse a PostgreSQL timeout GUC value (e.g. "0", "100ms", "1s", "5min", "2h")
     * and return the equivalent number of milliseconds.
     * Returns 0 if the value represents no timeout (0 or "0").
     * Returns -1 if the value cannot be parsed.
     */
    public static long parseTimeoutMillis(String value) {
        if (value == null) return 0;
        value = value.trim();
        if (value.isEmpty() || value.equals("0")) return 0;
        // Try plain numeric (milliseconds assumed by PostgreSQL for integer timeout values)
        try {
            long v = Long.parseLong(value);
            return v; // plain integers are milliseconds
        } catch (NumberFormatException ignored) {}
        // Try with unit suffix
        value = value.toLowerCase();
        if (value.endsWith("ms")) {
            try { return Long.parseLong(value.substring(0, value.length() - 2).trim()); } catch (NumberFormatException ignored) {}
        } else if (value.endsWith("s")) {
            try { return TimeUnit.SECONDS.toMillis(Long.parseLong(value.substring(0, value.length() - 1).trim())); } catch (NumberFormatException ignored) {}
        } else if (value.endsWith("min")) {
            try { return TimeUnit.MINUTES.toMillis(Long.parseLong(value.substring(0, value.length() - 3).trim())); } catch (NumberFormatException ignored) {}
        } else if (value.endsWith("h")) {
            try { return TimeUnit.HOURS.toMillis(Long.parseLong(value.substring(0, value.length() - 1).trim())); } catch (NumberFormatException ignored) {}
        } else if (value.endsWith("d")) {
            try { return TimeUnit.DAYS.toMillis(Long.parseLong(value.substring(0, value.length() - 1).trim())); } catch (NumberFormatException ignored) {}
        }
        return -1; // unparseable, treat as no timeout
    }
}
