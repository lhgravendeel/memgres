package com.memgres.engine;

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

        // Logging
        DEFAULTS.put("log_statement", "none");
        DEFAULTS.put("log_min_duration_statement", "-1");

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
        DEFAULTS.put("row_security", "on");
        DEFAULTS.put("check_function_bodies", "on");
        DEFAULTS.put("default_tablespace", "");
        DEFAULTS.put("default_table_access_method", "heap");
        DEFAULTS.put("default_toast_compression", "pglz");
        DEFAULTS.put("temp_tablespaces", "");
        DEFAULTS.put("max_parallel_workers_per_gather", "2");
        DEFAULTS.put("jit", "off");
        DEFAULTS.put("integer_datetimes", "on");

        // Float/numeric display (commonly set by JDBC drivers)
        DEFAULTS.put("extra_float_digits", "3");

        // Text search
        DEFAULTS.put("default_text_search_config", "pg_catalog.simple");

        // Index settings (queried by JDBC driver for DatabaseMetaData)
        DEFAULTS.put("max_index_keys", "32");
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

    /** Set a session-level parameter. */
    public void set(String name, String value) {
        sessionOverrides.put(name.toLowerCase(), value);
    }

    /** Set a transaction-scoped (LOCAL) parameter that reverts on commit/rollback. */
    public void setLocal(String name, String value) {
        transactionOverrides.put(name.toLowerCase(), value);
    }

    /** Clear all transaction-scoped overrides (called on commit/rollback). */
    public void clearTransactionOverrides() {
        transactionOverrides.clear();
    }

    /** Reset a single parameter to default. */
    public void reset(String name) {
        sessionOverrides.remove(name.toLowerCase());
    }

    /** Reset all session parameters. */
    public void resetAll() {
        sessionOverrides.clear();
    }

    /** Get a parameter value (transaction override, then session override, then default). */
    public String get(String name) {
        String key = name.toLowerCase();
        String val = transactionOverrides.get(key);
        if (val != null) return val;
        val = sessionOverrides.get(key);
        if (val != null) return val;
        return DEFAULTS.getOrDefault(key, null);
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
