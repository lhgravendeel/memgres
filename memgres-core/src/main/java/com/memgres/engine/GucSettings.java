package com.memgres.engine;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * GUC (Grand Unified Configuration) settings manager.
 * Provides sensible defaults for PostgreSQL configuration parameters.
 * Stores session-level SET overrides.
 *
 * <p>Each parameter is declared once, with its boot value and the metadata PostgreSQL reports
 * for it in {@code pg_settings}: the category it belongs to, the context in which it may be
 * changed, the type of value it holds, its unit and its bounds. Keeping the two together is
 * what lets {@code pg_settings} answer for the setting rather than for settings in general —
 * a client reads {@code vartype} to decide how to render a value and {@code context} to decide
 * whether it may be changed at all.</p>
 */
public class GucSettings {

    /**
     * The configuration parameters memgres carries.
     * Columns: name | boot value | vartype | context | category | unit | min_val | max_val |
     * enumvals | short_desc. A null boot value means the parameter has no default; the empty
     * string is a real empty default. Values are stored the way {@code pg_settings.setting}
     * reports them — in the parameter's own unit, so {@code work_mem} is 4096 (kB), not "4MB".
     */
    private static final String[][] SETTING_DEFS = {
            {"application_name", "memgres", "string", "user", "Reporting and Logging / What to Log", null, null, null, null, "Sets the application name to be reported in statistics and logs."},
            {"archive_command", "", "string", "sighup", "Write-Ahead Log / Archiving", null, null, null, null, "Sets the shell command that will be called to archive a WAL file."},
            {"archive_library", "", "string", "sighup", "Write-Ahead Log / Archiving", null, null, null, null, "Sets the library that will be called to archive a WAL file."},
            {"archive_mode", "off", "enum", "postmaster", "Write-Ahead Log / Archiving", null, null, null, "always,on,off", "Allows archiving of WAL files using \"archive_command\"."},
            {"archive_timeout", "0", "integer", "sighup", "Write-Ahead Log / Archiving", "s", "0", "1073741823", null, "Sets the amount of time to wait before forcing a switch to the next WAL file."},
            {"array_nulls", "on", "bool", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Enables input of NULL elements in arrays."},
            {"autovacuum", "on", "bool", "sighup", "Vacuuming / Automatic Vacuuming", null, null, null, null, "Starts the autovacuum subprocess."},
            {"autovacuum_analyze_scale_factor", "0.1", "real", "sighup", "Vacuuming / Automatic Vacuuming", null, "0", "100", null, "Number of tuple inserts, updates, or deletes prior to analyze as a fraction of reltuples."},
            {"autovacuum_analyze_threshold", "50", "integer", "sighup", "Vacuuming / Automatic Vacuuming", null, "0", "2147483647", null, "Minimum number of tuple inserts, updates, or deletes prior to analyze."},
            {"autovacuum_freeze_max_age", "200000000", "integer", "postmaster", "Vacuuming / Automatic Vacuuming", null, "100000", "2000000000", null, "Age at which to autovacuum a table to prevent transaction ID wraparound."},
            {"autovacuum_max_workers", "3", "integer", "sighup", "Vacuuming / Automatic Vacuuming", null, "1", "262143", null, "Sets the maximum number of simultaneously running autovacuum worker processes."},
            {"autovacuum_multixact_freeze_max_age", "400000000", "integer", "postmaster", "Vacuuming / Automatic Vacuuming", null, "10000", "2000000000", null, "Multixact age at which to autovacuum a table to prevent multixact wraparound."},
            {"autovacuum_naptime", "60", "integer", "sighup", "Vacuuming / Automatic Vacuuming", "s", "1", "2147483", null, "Time to sleep between autovacuum runs."},
            {"autovacuum_vacuum_cost_delay", "2", "real", "sighup", "Vacuuming / Automatic Vacuuming", "ms", "-1", "100", null, "Vacuum cost delay in milliseconds, for autovacuum."},
            {"autovacuum_vacuum_cost_limit", "-1", "integer", "sighup", "Vacuuming / Automatic Vacuuming", null, "-1", "10000", null, "Vacuum cost amount available before napping, for autovacuum."},
            {"autovacuum_vacuum_insert_scale_factor", "0.2", "real", "sighup", "Vacuuming / Automatic Vacuuming", null, "0", "100", null, "Number of tuple inserts prior to vacuum as a fraction of reltuples."},
            {"autovacuum_vacuum_insert_threshold", "1000", "integer", "sighup", "Vacuuming / Automatic Vacuuming", null, "-1", "2147483647", null, "Minimum number of tuple inserts prior to vacuum."},
            {"autovacuum_vacuum_scale_factor", "0.2", "real", "sighup", "Vacuuming / Automatic Vacuuming", null, "0", "100", null, "Number of tuple updates or deletes prior to vacuum as a fraction of reltuples."},
            {"autovacuum_vacuum_threshold", "50", "integer", "sighup", "Vacuuming / Automatic Vacuuming", null, "0", "2147483647", null, "Minimum number of tuple updates or deletes prior to vacuum."},
            {"backslash_quote", "safe_encoding", "enum", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, "safe_encoding,on,off", "Sets whether \"\\'\" is allowed in string literals."},
            {"bgwriter_delay", "200", "integer", "sighup", "Resource Usage / Background Writer", "ms", "10", "10000", null, "Background writer sleep time between rounds."},
            {"bgwriter_flush_after", "64", "integer", "sighup", "Resource Usage / Background Writer", "8kB", "0", "256", null, "Number of pages after which previously performed writes are flushed to disk."},
            {"bgwriter_lru_maxpages", "100", "integer", "sighup", "Resource Usage / Background Writer", null, "0", "1073741823", null, "Background writer maximum number of LRU pages to flush per round."},
            {"bgwriter_lru_multiplier", "2", "real", "sighup", "Resource Usage / Background Writer", null, "0", "10", null, "Multiple of the average buffer usage to free per round."},
            {"block_size", "8192", "integer", "internal", "Preset Options", null, "8192", "8192", null, "Shows the size of a disk block."},
            {"bytea_output", "hex", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "escape,hex", "Sets the output format for bytea."},
            {"check_function_bodies", "on", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Check routine bodies during CREATE FUNCTION and CREATE PROCEDURE."},
            {"checkpoint_completion_target", "0.9", "real", "sighup", "Write-Ahead Log / Checkpoints", null, "0", "1", null, "Time spent flushing dirty buffers during checkpoint, as fraction of checkpoint interval."},
            {"checkpoint_flush_after", "32", "integer", "sighup", "Write-Ahead Log / Checkpoints", "8kB", "0", "256", null, "Number of pages after which previously performed writes are flushed to disk."},
            {"checkpoint_timeout", "300", "integer", "sighup", "Write-Ahead Log / Checkpoints", "s", "30", "86400", null, "Sets the maximum time between automatic WAL checkpoints."},
            {"checkpoint_warning", "30", "integer", "sighup", "Write-Ahead Log / Checkpoints", "s", "0", "2147483647", null, "Sets the maximum time before warning if checkpoints triggered by WAL volume happen too frequently."},
            {"client_encoding", "UTF8", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the client's character set encoding."},
            {"client_min_messages", "notice", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "debug5,debug4,debug3,debug2,debug1,log,notice,warning,error", "Sets the message levels that are sent to the client."},
            {"compute_query_id", "auto", "enum", "superuser", "Statistics / Monitoring", null, null, null, "auto,regress,on,off", "Enables in-core computation of query identifiers."},
            {"cpu_index_tuple_cost", "0.005", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of processing each index entry during an index scan."},
            {"cpu_operator_cost", "0.0025", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of processing each operator or function call."},
            {"cpu_tuple_cost", "0.01", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of processing each tuple (row)."},
            {"data_checksums", "off", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether data checksums are turned on for this cluster."},
            {"datestyle", "ISO, MDY", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the display format for date and time values."},
            {"deadlock_timeout", "1000", "integer", "superuser", "Lock Management", "ms", "1", "2147483647", null, "Sets the time to wait on a lock before checking for deadlock."},
            {"debug_assertions", "off", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether the running server has assertion checks enabled."},
            {"default_table_access_method", "heap", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the default table access method for new tables."},
            {"default_tablespace", "", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the default tablespace to create tables and indexes in."},
            {"default_text_search_config", "pg_catalog.simple", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets default text search configuration."},
            {"default_toast_compression", "pglz", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "pglz,lz4", "Sets the default compression method for compressible values."},
            {"default_transaction_deferrable", "off", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the default deferrable status of new transactions."},
            {"default_transaction_isolation", "read committed", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "serializable,repeatable read,read committed,read uncommitted", "Sets the transaction isolation level of each new transaction."},
            {"default_transaction_read_only", "off", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the default read-only status of new transactions."},
            {"effective_cache_size", "524288", "integer", "user", "Query Tuning / Planner Cost Constants", "8kB", "1", "2147483647", null, "Sets the planner's assumption about the total size of the data caches."},
            {"effective_io_concurrency", "16", "integer", "user", "Resource Usage / I/O", null, "0", "1000", null, "Number of simultaneous requests that can be handled efficiently by the disk subsystem."},
            {"enable_hashagg", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of hashed aggregation plans."},
            {"enable_hashjoin", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of hash join plans."},
            {"enable_incremental_sort", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of incremental sort steps."},
            {"enable_indexscan", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of index-scan plans."},
            {"enable_mergejoin", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of merge join plans."},
            {"enable_nestloop", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of nested-loop join plans."},
            {"enable_partitionwise_join", "off", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables partitionwise join."},
            {"enable_presorted_aggregate", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's ability to produce plans that provide presorted input for ORDER BY / DISTINCT aggregate functions."},
            {"enable_seqscan", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of sequential-scan plans."},
            {"escape_string_warning", "on", "bool", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Warn about backslash escapes in ordinary string literals."},
            {"exit_on_error", "off", "bool", "user", "Error Handling", null, null, null, null, "Terminate session on any error."},
            {"extra_float_digits", "1", "integer", "user", "Client Connection Defaults / Locale and Formatting", null, "-15", "3", null, "Sets the number of digits displayed for floating-point values."},
            {"fsync", "on", "bool", "sighup", "Write-Ahead Log / Settings", null, null, null, null, "Forces synchronization of updates to disk."},
            {"full_page_writes", "on", "bool", "sighup", "Write-Ahead Log / Settings", null, null, null, null, "Writes full pages to WAL when first modified after a checkpoint."},
            {"hot_standby", "on", "bool", "postmaster", "Replication / Standby Servers", null, null, null, null, "Allows connections and queries during recovery."},
            {"idle_in_transaction_session_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed idle time between queries, when in a transaction."},
            {"idle_session_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed idle time between queries, when not in a transaction."},
            {"in_hot_standby", "off", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether hot standby is currently active."},
            {"integer_datetimes", "on", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether datetimes are integer based."},
            {"intervalstyle", "postgres", "enum", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, "postgres,postgres_verbose,sql_standard,iso_8601", "Sets the display format for interval values."},
            {"jit", "on", "bool", "user", "Query Tuning / Other Planner Options", null, null, null, null, "Allow JIT compilation."},
            {"lc_messages", "en_US.UTF-8", "string", "superuser", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the language in which messages are displayed."},
            {"lc_monetary", "en_US.UTF-8", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the locale for formatting monetary amounts."},
            {"lc_numeric", "en_US.UTF-8", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the locale for formatting numbers."},
            {"lc_time", "en_US.UTF-8", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the locale for formatting date and time values."},
            {"lo_compat_privileges", "off", "bool", "superuser", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Enables backward compatibility mode for privilege checks on large objects."},
            {"lock_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed duration of any wait for a lock."},
            {"log_lock_failures", "off", "bool", "superuser", "Reporting and Logging / What to Log", null, null, null, null, "Logs lock failures."},
            {"log_min_duration_statement", "-1", "integer", "superuser", "Reporting and Logging / When to Log", "ms", "-1", "2147483647", null, "Sets the minimum execution time above which all statements will be logged."},
            {"log_min_error_statement", "error", "enum", "superuser", "Reporting and Logging / When to Log", null, null, null, "debug5,debug4,debug3,debug2,debug1,info,notice,warning,error,log,fatal,panic", "Causes all statements generating error at or above this level to be logged."},
            {"log_min_messages", "warning", "enum", "superuser", "Reporting and Logging / When to Log", null, null, null, "debug5,debug4,debug3,debug2,debug1,info,notice,warning,error,log,fatal,panic", "Sets the message levels that are logged."},
            {"log_statement", "none", "enum", "superuser", "Reporting and Logging / What to Log", null, null, null, "none,ddl,mod,all", "Sets the type of statements logged."},
            {"log_timezone", "UTC", "string", "sighup", "Reporting and Logging / What to Log", null, null, null, null, "Sets the time zone to use in log messages."},
            {"logging_collector", "off", "bool", "postmaster", "Reporting and Logging / Where to Log", null, null, null, null, "Start a subprocess to capture stderr, csvlog and/or jsonlog into log files."},
            {"maintenance_work_mem", "65536", "integer", "user", "Resource Usage / Memory", "kB", "64", "2147483647", null, "Sets the maximum memory to be used for maintenance operations."},
            {"max_connections", "100", "integer", "postmaster", "Connections and Authentication / Connection Settings", null, "1", "262143", null, "Sets the maximum number of concurrent connections."},
            {"max_index_keys", "32", "integer", "internal", "Preset Options", null, "32", "32", null, "Shows the maximum number of index keys."},
            {"max_locks_per_transaction", "64", "integer", "postmaster", "Lock Management", null, "10", "2147483647", null, "Sets the maximum number of locks per transaction."},
            {"max_parallel_maintenance_workers", "2", "integer", "user", "Resource Usage / Worker Processes", null, "0", "1024", null, "Sets the maximum number of parallel processes per maintenance operation."},
            {"max_parallel_workers", "8", "integer", "user", "Resource Usage / Worker Processes", null, "0", "1024", null, "Sets the maximum number of parallel workers that can be active at one time."},
            {"max_parallel_workers_per_gather", "2", "integer", "user", "Resource Usage / Worker Processes", null, "0", "1024", null, "Sets the maximum number of parallel processes per executor node."},
            {"max_pred_locks_per_transaction", "64", "integer", "postmaster", "Lock Management", null, "10", "2147483647", null, "Sets the maximum number of predicate locks per transaction."},
            {"max_prepared_transactions", "0", "integer", "postmaster", "Resource Usage / Memory", null, "0", "262143", null, "Sets the maximum number of simultaneously prepared transactions."},
            {"max_replication_slots", "10", "integer", "postmaster", "Replication / Sending Servers", null, "0", "262143", null, "Sets the maximum number of simultaneously defined replication slots."},
            {"max_wal_senders", "10", "integer", "postmaster", "Replication / Sending Servers", null, "0", "262143", null, "Sets the maximum number of simultaneously running WAL sender processes."},
            {"max_wal_size", "1024", "integer", "sighup", "Write-Ahead Log / Checkpoints", "MB", "2", "2147483647", null, "Sets the WAL size that triggers a checkpoint."},
            {"max_worker_processes", "8", "integer", "postmaster", "Resource Usage / Worker Processes", null, "0", "262143", null, "Maximum number of concurrent worker processes."},
            {"min_parallel_table_scan_size", "1024", "integer", "user", "Query Tuning / Planner Cost Constants", "8kB", "0", "715827882", null, "Sets the minimum amount of table data for a parallel scan."},
            {"min_wal_size", "80", "integer", "sighup", "Write-Ahead Log / Checkpoints", "MB", "2", "2147483647", null, "Sets the minimum size to shrink the WAL to."},
            {"parallel_leader_participation", "on", "bool", "user", "Resource Usage / Worker Processes", null, null, null, null, "Controls whether Gather and Gather Merge also run subplans."},
            {"password_encryption", "scram-sha-256", "enum", "user", "Connections and Authentication / Authentication", null, null, null, "md5,scram-sha-256", "Chooses the algorithm for encrypting passwords."},
            {"plan_cache_mode", "auto", "enum", "user", "Query Tuning / Other Planner Options", null, null, null, "auto,force_generic_plan,force_custom_plan", "Controls the planner's selection of custom or generic plan."},
            {"quote_all_identifiers", "off", "bool", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "When generating SQL fragments, quote all identifiers."},
            {"random_page_cost", "4", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of a nonsequentially fetched disk page."},
            {"restart_after_crash", "on", "bool", "sighup", "Error Handling", null, null, null, null, "Reinitialize server after backend crash."},
            {"row_security", "on", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Enables row security."},
            {"search_path", "\"$user\", public", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the schema search order for names that are not schema-qualified."},
            {"segment_size", "131072", "integer", "internal", "Preset Options", "8kB", "131072", "131072", null, "Shows the number of pages per disk file."},
            {"seq_page_cost", "1", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of a sequentially fetched disk page."},
            {"server_encoding", "UTF8", "string", "internal", "Preset Options", null, null, null, null, "Shows the server (database) character set encoding."},
            {"server_version", "18.0", "string", "internal", "Preset Options", null, null, null, null, "Shows the server version."},
            {"server_version_num", "180000", "integer", "internal", "Preset Options", null, "180000", "180000", null, "Shows the server version as an integer."},
            {"session_replication_role", "origin", "enum", "superuser", "Client Connection Defaults / Statement Behavior", null, null, null, "origin,replica,local", "Sets the session's behavior for triggers and rewrite rules."},
            {"shared_buffers", "16384", "integer", "postmaster", "Resource Usage / Memory", "8kB", "16", "1073741823", null, "Sets the number of shared memory buffers used by the server."},
            {"ssl", "off", "bool", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Enables SSL connections."},
            {"standard_conforming_strings", "on", "bool", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Causes '...' strings to treat backslashes literally."},
            {"statement_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed duration of any statement."},
            {"synchronize_seqscans", "on", "bool", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Enables synchronized sequential scans."},
            {"synchronous_commit", "on", "enum", "user", "Write-Ahead Log / Settings", null, null, null, "local,remote_write,remote_apply,on,off", "Sets the current transaction's synchronization level."},
            {"temp_buffers", "1024", "integer", "user", "Resource Usage / Memory", "8kB", "100", "1073741823", null, "Sets the maximum number of temporary buffers used by each session."},
            {"temp_file_limit", "-1", "integer", "superuser", "Resource Usage / Disk", "kB", "-1", "2147483647", null, "Limits the total size of all temporary files used by each process."},
            {"temp_tablespaces", "", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the tablespace(s) to use for temporary tables and sort files."},
            {"timezone", "UTC", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the time zone for displaying and interpreting time stamps."},
            {"track_activities", "on", "bool", "superuser", "Statistics / Cumulative Query and Index Statistics", null, null, null, null, "Collects information about executing commands."},
            {"track_activity_query_size", "1024", "integer", "postmaster", "Statistics / Cumulative Query and Index Statistics", "B", "100", "1048576", null, "Sets the size reserved for pg_stat_activity.query, in bytes."},
            {"track_counts", "on", "bool", "superuser", "Statistics / Cumulative Query and Index Statistics", null, null, null, null, "Collects statistics on database activity."},
            {"track_functions", "none", "enum", "superuser", "Statistics / Cumulative Query and Index Statistics", null, null, null, "none,pl,all", "Collects function-level statistics on database activity."},
            {"track_io_timing", "off", "bool", "superuser", "Statistics / Cumulative Query and Index Statistics", null, null, null, null, "Collects timing statistics for database I/O activity."},
            {"transaction_deferrable", "off", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Whether to defer a read-only serializable transaction until it can be executed with no possible serialization failures."},
            {"transaction_isolation", "read committed", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "serializable,repeatable read,read committed,read uncommitted", "Sets the current transaction's isolation level."},
            {"transaction_read_only", "off", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the current transaction's read-only status."},
            {"transaction_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed duration of any transaction within a session (not a prepared transaction)."},
            {"vacuum_cost_delay", "0", "real", "user", "Vacuuming / Cost-Based Vacuum Delay", "ms", "0", "100", null, "Vacuum cost delay in milliseconds."},
            {"vacuum_cost_limit", "200", "integer", "user", "Vacuuming / Cost-Based Vacuum Delay", null, "1", "10000", null, "Vacuum cost amount available before napping."},
            {"vacuum_freeze_min_age", "50000000", "integer", "user", "Vacuuming / Freezing", null, "0", "1000000000", null, "Minimum age at which VACUUM should freeze a table row."},
            {"vacuum_freeze_table_age", "150000000", "integer", "user", "Vacuuming / Freezing", null, "0", "2000000000", null, "Age at which VACUUM should scan whole table to freeze tuples."},
            {"wal_block_size", "8192", "integer", "internal", "Preset Options", null, "8192", "8192", null, "Shows the block size in the write ahead log."},
            {"wal_compression", "off", "enum", "superuser", "Write-Ahead Log / Settings", null, null, null, "pglz,lz4,zstd,on,off", "Compresses full-page writes written in WAL file with specified method."},
            {"wal_level", "replica", "enum", "postmaster", "Write-Ahead Log / Settings", null, null, null, "minimal,replica,logical", "Sets the level of information written to the WAL."},
            {"wal_segment_size", "16777216", "integer", "internal", "Preset Options", "B", "1048576", "1073741824", null, "Shows the size of write ahead log segments."},
            {"work_mem", "4096", "integer", "user", "Resource Usage / Memory", "kB", "64", "2147483647", null, "Sets the maximum memory to be used for query workspaces."},
            {"xmloption", "content", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "content,document", "Sets whether XML data in implicit parsing and serialization operations is to be considered as documents or content fragments."},
    };

    /**
     * Parameters PostgreSQL marks {@code GUC_NO_SHOW_ALL}: {@code SHOW} and
     * {@code current_setting} answer for them, but they are deliberately absent from
     * {@code pg_settings} and from {@code SHOW ALL}.
     */
    private static final String[][] HIDDEN_DEFS = {
            {"is_superuser", "on", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether the current user is a superuser."},
            {"role", "test", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the current role."},
            {"session_authorization", "test", "string", "superuser", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the session user name."},
    };

    /** A single parameter's definition: its boot value plus the metadata pg_settings reports. */
    public static final class Def {
        public final String name;
        public final String bootVal;
        public final String vartype;
        public final String context;
        public final String category;
        /** The unit the stored value is counted in ("kB", "8kB", "ms", "s", ...), or null. */
        public final String unit;
        public final String minVal;
        public final String maxVal;
        /** The permitted values of an enum parameter, in pg_settings array form, or null. */
        public final String enumVals;
        public final String shortDesc;
        /** Whether pg_settings and SHOW ALL list the parameter. */
        public final boolean listed;

        Def(String[] row, boolean listed) {
            this.name = row[0];
            this.bootVal = row[1];
            this.vartype = row[2];
            this.context = row[3];
            this.category = row[4];
            this.unit = row[5];
            this.minVal = row[6];
            this.maxVal = row[7];
            this.enumVals = row[8] == null ? null : arrayLiteral(row[8]);
            this.shortDesc = row[9];
            this.listed = listed;
        }

        /**
         * Render a comma-separated list of permitted values as a PostgreSQL text array, quoting
         * the elements that need it — "read committed" has a space in it, and a client that
         * splits the literal on commas has to see the quotes to keep the value whole.
         */
        private static String arrayLiteral(String commaSeparated) {
            StringBuilder sb = new StringBuilder("{");
            String[] parts = commaSeparated.split(",");
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) sb.append(',');
                String p = parts[i];
                boolean quote = p.isEmpty() || p.equalsIgnoreCase("NULL");
                for (int j = 0; !quote && j < p.length(); j++) {
                    char c = p.charAt(j);
                    quote = Character.isWhitespace(c) || c == '{' || c == '}' || c == '"'
                            || c == '\\' || c == ',';
                }
                if (quote) {
                    sb.append('"').append(p.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
                } else {
                    sb.append(p);
                }
            }
            return sb.append('}').toString();
        }
    }

    private static final Map<String, Def> DEFINITIONS;
    private static final Map<String, String> DEFAULTS = new LinkedHashMap<>();

    static {
        Map<String, Def> defs = new LinkedHashMap<>();
        for (String[] row : SETTING_DEFS) defs.put(row[0], new Def(row, true));
        for (String[] row : HIDDEN_DEFS) defs.put(row[0], new Def(row, false));
        DEFINITIONS = Collections.unmodifiableMap(defs);
        for (Def d : defs.values()) {
            if (d.bootVal != null) DEFAULTS.put(d.name, d.bootVal);
        }
    }

    /** The definition of a parameter, or null when memgres does not carry it. */
    public static Def definition(String name) {
        return name == null ? null : DEFINITIONS.get(name.toLowerCase());
    }

    /** Every parameter definition, in name order. */
    public static Collection<Def> definitions() {
        return DEFINITIONS.values();
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
        sessionOverrides.put(key, toBaseUnit(key, canonicalValue(key, normalized)));
    }

    /** Set a transaction-scoped (LOCAL) parameter that reverts on commit/rollback. */
    public void setLocal(String name, String value) {
        String key = name.toLowerCase();
        transactionOverrides.put(key, toBaseUnit(key, canonicalValue(key, value)));
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
        return DEFAULTS.get(key);
    }

    /** The value RESET would restore: the boot default, ignoring anything this session has set. */
    public String getResetValue(String name) {
        String key = name.toLowerCase();
        String val = bootDefaults.get(key);
        if (val != null) return val;
        return DEFAULTS.get(key);
    }

    /**
     * Get a parameter value formatted for display (SHOW, current_setting).
     * A parameter that counts in a unit is stored in that unit and displayed in the largest
     * one that divides it evenly, the way PostgreSQL does: 4096 kB reads back as "4MB" and
     * 5000 ms as "5s". Only positive values take a unit, so 0 and -1 stay as they are.
     */
    public String getForDisplay(String name) {
        String val = get(name);
        if (val == null) return null;
        Def def = definition(name);
        if (def != null && def.unit != null) {
            return fromBaseUnit(val, def.unit);
        }
        // PG always displays boolean GUC values in lowercase (on/off)
        if (val.equalsIgnoreCase("on") || val.equalsIgnoreCase("off")
                || val.equalsIgnoreCase("true") || val.equalsIgnoreCase("false")
                || val.equalsIgnoreCase("yes") || val.equalsIgnoreCase("no")) {
            return val.toLowerCase();
        }
        return val;
    }

    /** Memory display units, largest first, in bytes. */
    private static final String[] MEMORY_UNITS = {"TB", "GB", "MB", "kB", "B"};
    /** Time display units, largest first, in milliseconds. */
    private static final String[] TIME_UNITS = {"d", "h", "min", "s", "ms", "us"};

    /** The size of one {@code unit} in bytes (memory) or milliseconds (time), or -1. */
    private static double unitFactor(String unit) {
        if (unit == null) return -1;
        switch (unit) {
            case "B": return 1;
            case "kB": return 1024;
            case "8kB": return 8192;
            case "16kB": return 16384;
            case "32kB": return 32768;
            case "MB": return 1024.0 * 1024;
            case "GB": return 1024.0 * 1024 * 1024;
            case "TB": return 1024.0 * 1024 * 1024 * 1024;
            case "us": return 0.001;
            case "ms": return 1;
            case "s": return 1000;
            case "min": return 60000;
            case "h": return 3600000.0;
            case "d": return 86400000.0;
            default: return -1;
        }
    }

    /** Whether a unit counts memory (as opposed to time). */
    private static boolean isMemoryUnit(String unit) {
        return unit != null && unit.endsWith("B");
    }

    /**
     * Render a value stored in {@code unit} the way SHOW does: the largest unit of the same
     * dimension that divides it evenly. Non-numeric and non-positive values are left alone,
     * which is how PostgreSQL prints 0 and -1 for a parameter that has a unit.
     */
    static String fromBaseUnit(String value, String unit) {
        double base = unitFactor(unit);
        if (base < 0 || value == null) return value;
        double n;
        try {
            n = Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return value; // already carries a unit, or is not a number at all
        }
        if (!(n > 0)) return value;
        double amount = n * base;
        String[] units = isMemoryUnit(unit) ? MEMORY_UNITS : TIME_UNITS;
        for (String u : units) {
            double f = unitFactor(u);
            double scaled = amount / f;
            if (Math.abs(scaled - Math.rint(scaled)) < 1e-10) {
                long whole = (long) Math.rint(scaled);
                return whole + u;
            }
        }
        return value;
    }

    /**
     * Convert a written value ("8MB", "5s") into the unit the parameter is counted in, so that
     * pg_settings.setting reports what PostgreSQL reports. A value with no unit suffix, or a
     * parameter that has no unit, is stored as written.
     */
    private static String toBaseUnit(String name, String value) {
        Def def = definition(name);
        if (def == null || def.unit == null || value == null) return value;
        String text = value.trim();
        int i = 0;
        while (i < text.length() && (Character.isDigit(text.charAt(i)) || text.charAt(i) == '-'
                || text.charAt(i) == '+' || text.charAt(i) == '.')) {
            i++;
        }
        String number = text.substring(0, i);
        String suffix = text.substring(i).trim();
        if (number.isEmpty() || suffix.isEmpty()) return value;
        double from = unitFactor(suffix);
        double to = unitFactor(def.unit);
        if (from < 0 || to < 0 || isMemoryUnit(suffix) != isMemoryUnit(def.unit)) return value;
        double n;
        try {
            n = Double.parseDouble(number);
        } catch (NumberFormatException e) {
            return value;
        }
        double converted = n * from / to;
        if (Math.abs(converted - Math.rint(converted)) < 1e-10) {
            return String.valueOf((long) Math.rint(converted));
        }
        return String.valueOf(converted);
    }

    /**
     * Refuse an assignment the parameter's own definition does not allow.
     *
     * <p>{@code pg_settings} reports the context a parameter may be changed in, the type of value
     * it holds, the values an enum permits and the range a number has to fall in. Those are the
     * same facts SET is judged against, so a client that reads them and a client that writes the
     * value get the same answer: a preset the server computed at startup cannot be assigned at
     * all, and one that can is still refused a value of the wrong type or outside its range.
     */
    public static void checkAssignable(String name, String rawValue) {
        Def def = definition(name);
        if (def == null) return;
        if ("internal".equals(def.context)) {
            throw new MemgresException("parameter \"" + def.name + "\" cannot be changed", "55P02");
        }
        if ("postmaster".equals(def.context)) {
            throw new MemgresException("parameter \"" + def.name
                    + "\" cannot be changed without restarting the server", "55P02");
        }
        if ("sighup".equals(def.context)) {
            throw new MemgresException("parameter \"" + def.name + "\" cannot be changed now", "55P02");
        }
        if ("backend".equals(def.context) || "superuser-backend".equals(def.context)) {
            throw new MemgresException("parameter \"" + def.name
                    + "\" cannot be set after connection start", "55P02");
        }
        String value = unquote(rawValue);
        if (value == null) return;
        if ("bool".equals(def.vartype)) {
            if (parseBool(value) == null) {
                throw new MemgresException("parameter \"" + def.name + "\" requires a Boolean value", "22023");
            }
            return;
        }
        if ("enum".equals(def.vartype)) {
            if (enumMatch(def, value) == null) {
                throw new MemgresException("invalid value for parameter \"" + def.name + "\": \"" + value + "\""
                        + "\n  Hint: Available values: " + enumHint(def) + ".", "22023");
            }
            return;
        }
        if ("integer".equals(def.vartype) || "real".equals(def.vartype)) {
            Double n = numericInBaseUnit(def, value);
            if (n == null) {
                throw new MemgresException("invalid value for parameter \"" + def.name + "\": \"" + value + "\"", "22023");
            }
            checkRange(def, n, value);
        }
    }

    /** Refuse a number the parameter's declared bounds exclude, naming the range in its unit. */
    private static void checkRange(Def def, double n, String written) {
        Double min = parseNumber(def.minVal);
        Double max = parseNumber(def.maxVal);
        boolean below = min != null && n < min;
        boolean above = max != null && n > max;
        if (!below && !above) return;
        String unit = def.unit == null ? "" : " " + def.unit;
        throw new MemgresException(trimNumber(n) + unit + " is outside the valid range for parameter \""
                + def.name + "\" (" + def.minVal + unit + " .. " + def.maxVal + unit + ")", "22023");
    }

    /** The value as pg_settings would store it, or null when it is not a number at all. */
    private static Double numericInBaseUnit(Def def, String value) {
        String text = value.trim();
        int i = 0;
        while (i < text.length() && (Character.isDigit(text.charAt(i)) || text.charAt(i) == '-'
                || text.charAt(i) == '+' || text.charAt(i) == '.')) {
            i++;
        }
        Double number = parseNumber(text.substring(0, i));
        if (number == null) return null;
        String suffix = text.substring(i).trim();
        if (suffix.isEmpty()) return number;
        if (def.unit == null) return null;
        double from = unitFactor(suffix);
        double to = unitFactor(def.unit);
        if (from < 0 || to < 0 || isMemoryUnit(suffix) != isMemoryUnit(def.unit)) return null;
        return Double.valueOf(number.doubleValue() * from / to);
    }

    private static Double parseNumber(String text) {
        if (text == null || text.isEmpty()) return null;
        try {
            return Double.valueOf(Double.parseDouble(text));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** A whole number without the ".0" Java prints for a double. */
    private static String trimNumber(double n) {
        if (n == Math.rint(n) && !Double.isInfinite(n)) return String.valueOf((long) n);
        return String.valueOf(n);
    }

    /** The boolean a written value stands for, or null when it stands for neither. */
    static Boolean parseBool(String value) {
        String v = unquote(value);
        if (v == null) return null;
        v = v.trim().toLowerCase();
        if (v.isEmpty()) return null;
        if ("on".startsWith(v) && !"o".equals(v)) return Boolean.TRUE;   // on
        if ("off".startsWith(v) && !"o".equals(v)) return Boolean.FALSE; // off, of
        if ("true".startsWith(v)) return Boolean.TRUE;
        if ("false".startsWith(v)) return Boolean.FALSE;
        if ("yes".startsWith(v)) return Boolean.TRUE;
        if ("no".startsWith(v)) return Boolean.FALSE;
        if ("1".equals(v)) return Boolean.TRUE;
        if ("0".equals(v)) return Boolean.FALSE;
        return null;
    }

    /** The permitted value a written one names, in the enum's own spelling, or null. */
    private static String enumMatch(Def def, String value) {
        if (def.enumVals == null) return value;
        String v = unquote(value);
        if (v == null) return null;
        v = v.trim();
        for (String permitted : enumValues(def)) {
            if (permitted.equalsIgnoreCase(v)) return permitted;
        }
        return null;
    }

    /** The permitted values of an enum parameter, unwrapped from the array literal. */
    private static List<String> enumValues(Def def) {
        List<String> out = new ArrayList<String>();
        if (def.enumVals == null) return out;
        String body = def.enumVals.substring(1, def.enumVals.length() - 1);
        StringBuilder cur = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '"') { quoted = !quoted; continue; }
            if (c == ',' && !quoted) { out.add(cur.toString()); cur.setLength(0); continue; }
            cur.append(c);
        }
        if (cur.length() > 0) out.add(cur.toString());
        return out;
    }

    /** The permitted values as PostgreSQL lists them in its hint. */
    private static String enumHint(Def def) {
        StringBuilder sb = new StringBuilder();
        for (String v : enumValues(def)) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(v);
        }
        return sb.toString();
    }

    /** A value with the quotes SET may have carried into it removed. */
    private static String unquote(String value) {
        if (value == null) return null;
        String v = value.trim();
        if (v.length() >= 2 && ((v.startsWith("'") && v.endsWith("'"))
                || (v.startsWith("\"") && v.endsWith("\"")))) {
            return v.substring(1, v.length() - 1);
        }
        return v;
    }

    /**
     * The value as the parameter records it: an enum keeps the spelling its own list uses, so
     * {@code SET client_min_messages = 'WARNING'} reads back as {@code warning} the way it does
     * in PostgreSQL.
     */
    private static String canonicalValue(String name, String value) {
        Def def = definition(name);
        if (def == null || value == null) return value;
        if ("enum".equals(def.vartype)) {
            String match = enumMatch(def, value);
            if (match != null) return match;
        }
        return value;
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

    /**
     * The parameters pg_settings and SHOW ALL list: every listed default plus whatever this
     * session has set, minus the ones PostgreSQL keeps out of that listing.
     */
    public Map<String, String> getAll() {
        Map<String, String> result = new LinkedHashMap<>();
        for (Def d : DEFINITIONS.values()) {
            if (d.listed && d.bootVal != null) result.put(d.name, d.bootVal);
        }
        for (Map.Entry<String, String> e : sessionOverrides.entrySet()) {
            Def d = DEFINITIONS.get(e.getKey());
            if (d != null && !d.listed) continue;
            result.put(e.getKey(), e.getValue());
        }
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
