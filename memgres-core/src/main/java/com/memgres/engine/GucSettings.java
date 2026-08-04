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
     * enumvals | short_desc | extra_desc. A null boot value means the parameter has no default;
     * the empty string is a real empty default. Values are stored the way
     * {@code pg_settings.setting} reports them — in the parameter's own unit, so
     * {@code work_mem} is 4096 (kB), not "4MB".
     *
     * <p>The eleventh column is optional and a row may stop at ten: PostgreSQL leaves
     * {@code extra_desc} null for most parameters, and writing it out only where there is one
     * keeps the value beside the parameter it belongs to. It is the sentence a client shows
     * under the short description, and it is where PostgreSQL says what {@code 0} or {@code -1}
     * means for a parameter that gives them a special meaning.</p>
     */
    private static final String[][] SETTING_DEFS = {
            {"application_name", "memgres", "string", "user", "Reporting and Logging / What to Log", null, null, null, null, "Sets the application name to be reported in statistics and logs."},
            {"archive_command", "", "string", "sighup", "Write-Ahead Log / Archiving", null, null, null, null, "Sets the shell command that will be called to archive a WAL file.", "An empty string means use \"archive_library\"."},
            {"archive_library", "", "string", "sighup", "Write-Ahead Log / Archiving", null, null, null, null, "Sets the library that will be called to archive a WAL file.", "An empty string means use \"archive_command\"."},
            {"archive_mode", "off", "enum", "postmaster", "Write-Ahead Log / Archiving", null, null, null, "always,on,off", "Allows archiving of WAL files using \"archive_command\"."},
            {"archive_timeout", "0", "integer", "sighup", "Write-Ahead Log / Archiving", "s", "0", "1073741823", null, "Sets the amount of time to wait before forcing a switch to the next WAL file.", "0 disables the timeout."},
            {"array_nulls", "on", "bool", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Enables input of NULL elements in arrays.", "When turned on, unquoted NULL in an array input value means a null value; otherwise it is taken literally."},
            {"autovacuum", "on", "bool", "sighup", "Vacuuming / Automatic Vacuuming", null, null, null, null, "Starts the autovacuum subprocess."},
            {"autovacuum_analyze_scale_factor", "0.1", "real", "sighup", "Vacuuming / Automatic Vacuuming", null, "0", "100", null, "Number of tuple inserts, updates, or deletes prior to analyze as a fraction of reltuples."},
            {"autovacuum_analyze_threshold", "50", "integer", "sighup", "Vacuuming / Automatic Vacuuming", null, "0", "2147483647", null, "Minimum number of tuple inserts, updates, or deletes prior to analyze."},
            {"autovacuum_freeze_max_age", "200000000", "integer", "postmaster", "Vacuuming / Automatic Vacuuming", null, "100000", "2000000000", null, "Age at which to autovacuum a table to prevent transaction ID wraparound."},
            {"autovacuum_max_workers", "3", "integer", "sighup", "Vacuuming / Automatic Vacuuming", null, "1", "262143", null, "Sets the maximum number of simultaneously running autovacuum worker processes."},
            {"autovacuum_multixact_freeze_max_age", "400000000", "integer", "postmaster", "Vacuuming / Automatic Vacuuming", null, "10000", "2000000000", null, "Multixact age at which to autovacuum a table to prevent multixact wraparound."},
            {"autovacuum_naptime", "60", "integer", "sighup", "Vacuuming / Automatic Vacuuming", "s", "1", "2147483", null, "Time to sleep between autovacuum runs."},
            {"autovacuum_vacuum_cost_delay", "2", "real", "sighup", "Vacuuming / Automatic Vacuuming", "ms", "-1", "100", null, "Vacuum cost delay in milliseconds, for autovacuum.", "-1 means use \"vacuum_cost_delay\"."},
            {"autovacuum_vacuum_cost_limit", "-1", "integer", "sighup", "Vacuuming / Automatic Vacuuming", null, "-1", "10000", null, "Vacuum cost amount available before napping, for autovacuum.", "-1 means use \"vacuum_cost_limit\"."},
            {"autovacuum_vacuum_insert_scale_factor", "0.2", "real", "sighup", "Vacuuming / Automatic Vacuuming", null, "0", "100", null, "Number of tuple inserts prior to vacuum as a fraction of reltuples."},
            {"autovacuum_vacuum_insert_threshold", "1000", "integer", "sighup", "Vacuuming / Automatic Vacuuming", null, "-1", "2147483647", null, "Minimum number of tuple inserts prior to vacuum.", "-1 disables insert vacuums."},
            {"autovacuum_vacuum_scale_factor", "0.2", "real", "sighup", "Vacuuming / Automatic Vacuuming", null, "0", "100", null, "Number of tuple updates or deletes prior to vacuum as a fraction of reltuples."},
            {"autovacuum_vacuum_threshold", "50", "integer", "sighup", "Vacuuming / Automatic Vacuuming", null, "0", "2147483647", null, "Minimum number of tuple updates or deletes prior to vacuum."},
            {"backslash_quote", "safe_encoding", "enum", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, "safe_encoding,on,off", "Sets whether \"\\'\" is allowed in string literals."},
            {"bgwriter_delay", "200", "integer", "sighup", "Resource Usage / Background Writer", "ms", "10", "10000", null, "Background writer sleep time between rounds."},
            {"bgwriter_flush_after", "64", "integer", "sighup", "Resource Usage / Background Writer", "8kB", "0", "256", null, "Number of pages after which previously performed writes are flushed to disk.", "0 disables forced writeback."},
            {"bgwriter_lru_maxpages", "100", "integer", "sighup", "Resource Usage / Background Writer", null, "0", "1073741823", null, "Background writer maximum number of LRU pages to flush per round."},
            {"bgwriter_lru_multiplier", "2", "real", "sighup", "Resource Usage / Background Writer", null, "0", "10", null, "Multiple of the average buffer usage to free per round."},
            {"block_size", "8192", "integer", "internal", "Preset Options", null, "8192", "8192", null, "Shows the size of a disk block."},
            {"bytea_output", "hex", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "escape,hex", "Sets the output format for bytea."},
            {"check_function_bodies", "on", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Check routine bodies during CREATE FUNCTION and CREATE PROCEDURE."},
            {"checkpoint_completion_target", "0.9", "real", "sighup", "Write-Ahead Log / Checkpoints", null, "0", "1", null, "Time spent flushing dirty buffers during checkpoint, as fraction of checkpoint interval."},
            {"checkpoint_flush_after", "32", "integer", "sighup", "Write-Ahead Log / Checkpoints", "8kB", "0", "256", null, "Number of pages after which previously performed writes are flushed to disk.", "0 disables forced writeback."},
            {"checkpoint_timeout", "300", "integer", "sighup", "Write-Ahead Log / Checkpoints", "s", "30", "86400", null, "Sets the maximum time between automatic WAL checkpoints."},
            {"checkpoint_warning", "30", "integer", "sighup", "Write-Ahead Log / Checkpoints", "s", "0", "2147483647", null, "Sets the maximum time before warning if checkpoints triggered by WAL volume happen too frequently."},
            {"client_encoding", "UTF8", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the client's character set encoding."},
            {"client_min_messages", "notice", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "debug5,debug4,debug3,debug2,debug1,log,notice,warning,error", "Sets the message levels that are sent to the client.", "Each level includes all the levels that follow it. The later the level, the fewer messages are sent."},
            {"compute_query_id", "auto", "enum", "superuser", "Statistics / Monitoring", null, null, null, "auto,regress,on,off", "Enables in-core computation of query identifiers."},
            {"cpu_index_tuple_cost", "0.005", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of processing each index entry during an index scan."},
            {"cpu_operator_cost", "0.0025", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of processing each operator or function call."},
            {"cpu_tuple_cost", "0.01", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of processing each tuple (row)."},
            {"data_checksums", "off", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether data checksums are turned on for this cluster."},
            {"datestyle", "ISO, MDY", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the display format for date and time values.", "Also controls interpretation of ambiguous date inputs."},
            {"deadlock_timeout", "1000", "integer", "superuser", "Lock Management", "ms", "1", "2147483647", null, "Sets the time to wait on a lock before checking for deadlock."},
            {"debug_assertions", "off", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether the running server has assertion checks enabled."},
            {"default_table_access_method", "heap", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the default table access method for new tables."},
            {"default_tablespace", "", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the default tablespace to create tables and indexes in.", "An empty string means use the database's default tablespace."},
            {"default_text_search_config", "pg_catalog.simple", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets default text search configuration."},
            {"default_toast_compression", "pglz", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "pglz,lz4", "Sets the default compression method for compressible values."},
            {"default_transaction_deferrable", "off", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the default deferrable status of new transactions."},
            {"default_transaction_isolation", "read committed", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "serializable,repeatable read,read committed,read uncommitted", "Sets the transaction isolation level of each new transaction."},
            {"default_transaction_read_only", "off", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the default read-only status of new transactions."},
            {"effective_cache_size", "524288", "integer", "user", "Query Tuning / Planner Cost Constants", "8kB", "1", "2147483647", null, "Sets the planner's assumption about the total size of the data caches.", "That is, the total size of the caches (kernel cache and shared buffers) used for PostgreSQL data files. This is measured in disk pages, which are normally 8 kB each."},
            {"effective_io_concurrency", "16", "integer", "user", "Resource Usage / I/O", null, "0", "1000", null, "Number of simultaneous requests that can be handled efficiently by the disk subsystem.", "0 disables simultaneous requests."},
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
            {"extra_float_digits", "1", "integer", "user", "Client Connection Defaults / Locale and Formatting", null, "-15", "3", null, "Sets the number of digits displayed for floating-point values.", "This affects real, double precision, and geometric data types. A zero or negative parameter value is added to the standard number of digits (FLT_DIG or DBL_DIG as appropriate). Any value greater than zero selects precise output mode."},
            {"fsync", "on", "bool", "sighup", "Write-Ahead Log / Settings", null, null, null, null, "Forces synchronization of updates to disk.", "The server will use the fsync() system call in several places to make sure that updates are physically written to disk. This ensures that a database cluster will recover to a consistent state after an operating system or hardware crash."},
            {"full_page_writes", "on", "bool", "sighup", "Write-Ahead Log / Settings", null, null, null, null, "Writes full pages to WAL when first modified after a checkpoint.", "A page write in process during an operating system crash might be only partially written to disk.  During recovery, the row changes stored in WAL are not enough to recover.  This option writes pages when first modified after a checkpoint to WAL so full recovery is possible."},
            {"hot_standby", "on", "bool", "postmaster", "Replication / Standby Servers", null, null, null, null, "Allows connections and queries during recovery."},
            {"idle_in_transaction_session_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed idle time between queries, when in a transaction."},
            {"idle_session_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed idle time between queries, when not in a transaction."},
            {"in_hot_standby", "off", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether hot standby is currently active."},
            {"integer_datetimes", "on", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether datetimes are integer based."},
            {"intervalstyle", "postgres", "enum", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, "postgres,postgres_verbose,sql_standard,iso_8601", "Sets the display format for interval values."},
            {"jit", "on", "bool", "user", "Query Tuning / Other Planner Options", null, null, null, null, "Allow JIT compilation."},
            {"lc_messages", "en_US.UTF-8", "string", "superuser", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the language in which messages are displayed.", "An empty string means use the operating system setting."},
            {"lc_monetary", "en_US.UTF-8", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the locale for formatting monetary amounts.", "An empty string means use the operating system setting."},
            {"lc_numeric", "en_US.UTF-8", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the locale for formatting numbers.", "An empty string means use the operating system setting."},
            {"lc_time", "en_US.UTF-8", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the locale for formatting date and time values.", "An empty string means use the operating system setting."},
            {"lo_compat_privileges", "off", "bool", "superuser", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Enables backward compatibility mode for privilege checks on large objects.", "Skips privilege checks when reading or modifying large objects, for compatibility with PostgreSQL releases prior to 9.0."},
            {"lock_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed duration of any wait for a lock.", "0 disables the timeout."},
            {"log_lock_failures", "off", "bool", "superuser", "Reporting and Logging / What to Log", null, null, null, null, "Logs lock failures."},
            {"log_min_duration_statement", "-1", "integer", "superuser", "Reporting and Logging / When to Log", "ms", "-1", "2147483647", null, "Sets the minimum execution time above which all statements will be logged.", "-1 disables logging statement durations. 0 means log all statement durations."},
            {"log_min_error_statement", "error", "enum", "superuser", "Reporting and Logging / When to Log", null, null, null, "debug5,debug4,debug3,debug2,debug1,info,notice,warning,error,log,fatal,panic", "Causes all statements generating error at or above this level to be logged."},
            {"log_min_messages", "warning", "enum", "superuser", "Reporting and Logging / When to Log", null, null, null, "debug5,debug4,debug3,debug2,debug1,info,notice,warning,error,log,fatal,panic", "Sets the message levels that are logged."},
            {"log_statement", "none", "enum", "superuser", "Reporting and Logging / What to Log", null, null, null, "none,ddl,mod,all", "Sets the type of statements logged."},
            {"log_timezone", "UTC", "string", "sighup", "Reporting and Logging / What to Log", null, null, null, null, "Sets the time zone to use in log messages."},
            {"logging_collector", "off", "bool", "postmaster", "Reporting and Logging / Where to Log", null, null, null, null, "Start a subprocess to capture stderr, csvlog and/or jsonlog into log files."},
            {"maintenance_work_mem", "65536", "integer", "user", "Resource Usage / Memory", "kB", "64", "2147483647", null, "Sets the maximum memory to be used for maintenance operations.", "This includes operations such as VACUUM and CREATE INDEX."},
            {"max_connections", "100", "integer", "postmaster", "Connections and Authentication / Connection Settings", null, "1", "262143", null, "Sets the maximum number of concurrent connections."},
            {"max_index_keys", "32", "integer", "internal", "Preset Options", null, "32", "32", null, "Shows the maximum number of index keys."},
            {"max_locks_per_transaction", "64", "integer", "postmaster", "Lock Management", null, "10", "2147483647", null, "Sets the maximum number of locks per transaction.", "The shared lock table is sized on the assumption that at most \"max_locks_per_transaction\" objects per server process or prepared transaction will need to be locked at any one time."},
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
            {"row_security", "on", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Enables row security.", "When enabled, row security will be applied to all users."},
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
            {"statement_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed duration of any statement.", "0 disables the timeout."},
            {"synchronize_seqscans", "on", "bool", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Enables synchronized sequential scans."},
            {"synchronous_commit", "on", "enum", "user", "Write-Ahead Log / Settings", null, null, null, "local,remote_write,remote_apply,on,off", "Sets the current transaction's synchronization level."},
            {"temp_buffers", "1024", "integer", "user", "Resource Usage / Memory", "8kB", "100", "1073741823", null, "Sets the maximum number of temporary buffers used by each session."},
            {"temp_file_limit", "-1", "integer", "superuser", "Resource Usage / Disk", "kB", "-1", "2147483647", null, "Limits the total size of all temporary files used by each process."},
            {"temp_tablespaces", "", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the tablespace(s) to use for temporary tables and sort files.", "An empty string means use the database's default tablespace."},
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
            {"work_mem", "4096", "integer", "user", "Resource Usage / Memory", "kB", "64", "2147483647", null, "Sets the maximum memory to be used for query workspaces.", "This much memory can be used by each internal sort operation and hash table before switching to temporary disk files."},
            {"xmloption", "content", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "content,document", "Sets whether XML data in implicit parsing and serialization operations is to be considered as documents or content fragments."},
            {"authentication_timeout", "60", "integer", "sighup", "Connections and Authentication / Authentication", "s", "1", "600", null, "Sets the maximum allowed time to complete client authentication."},
            {"autovacuum_work_mem", "-1", "integer", "sighup", "Resource Usage / Memory", "kB", "-1", "2147483647", null, "Sets the maximum memory to be used by each autovacuum worker process.", "-1 means use \"maintenance_work_mem\"."},
            {"backend_flush_after", "0", "integer", "user", "Resource Usage / I/O", "8kB", "0", "256", null, "Number of pages after which previously performed writes are flushed to disk.", "0 disables forced writeback."},
            {"client_connection_check_interval", "0", "integer", "user", "Connections and Authentication / TCP Settings", "ms", "0", "2147483647", null, "Sets the time interval between checks for disconnection while running queries.", "0 disables connection checks."},
            {"commit_delay", "0", "integer", "superuser", "Write-Ahead Log / Settings", null, "0", "100000", null, "Sets the delay in microseconds between transaction commit and flushing WAL to disk."},
            {"commit_siblings", "5", "integer", "user", "Write-Ahead Log / Settings", null, "0", "1000", null, "Sets the minimum number of concurrent open transactions required before performing \"commit_delay\"."},
            {"constraint_exclusion", "partition", "enum", "user", "Query Tuning / Other Planner Options", null, null, null, "partition,on,off", "Enables the planner to use constraints to optimize queries.", "Table scans will be skipped if their constraints guarantee that no rows match the query."},
            {"cursor_tuple_fraction", "0.1", "real", "user", "Query Tuning / Other Planner Options", null, "0", "1", null, "Sets the planner's estimate of the fraction of a cursor's rows that will be retrieved."},
            {"debug_discard_caches", "0", "integer", "superuser", "Developer Options", null, "0", "0", null, "Aggressively flush system caches for debugging purposes.", "0 means use normal caching behavior."},
            {"default_statistics_target", "100", "integer", "user", "Query Tuning / Other Planner Options", null, "1", "10000", null, "Sets the default statistics target.", "This applies to table columns that have not had a column-specific target set via ALTER TABLE SET STATISTICS."},
            {"dynamic_library_path", "$libdir", "string", "superuser", "Client Connection Defaults / Other Defaults", null, null, null, null, "Sets the path for dynamically loadable modules.", "If a dynamically loadable module needs to be opened and the specified name does not have a directory component (i.e., the name does not contain a slash), the system will search this path for the specified file."},
            {"enable_async_append", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of async append plans."},
            {"enable_bitmapscan", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of bitmap-scan plans."},
            {"enable_distinct_reordering", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables reordering of DISTINCT keys."},
            {"enable_gathermerge", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of gather merge plans."},
            {"enable_group_by_reordering", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables reordering of GROUP BY keys."},
            {"enable_indexonlyscan", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of index-only-scan plans."},
            {"enable_material", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of materialization."},
            {"enable_memoize", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of memoization."},
            {"enable_parallel_append", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of parallel append plans."},
            {"enable_parallel_hash", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of parallel hash plans."},
            {"enable_partition_pruning", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables plan-time and execution-time partition pruning.", "Allows the query planner and executor to compare partition bounds to conditions in the query to determine which partitions must be scanned."},
            {"enable_partitionwise_aggregate", "off", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables partitionwise aggregation and grouping."},
            {"enable_self_join_elimination", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables removal of unique self-joins."},
            {"enable_sort", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of explicit sort steps."},
            {"enable_tidscan", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of TID scan plans."},
            {"from_collapse_limit", "8", "integer", "user", "Query Tuning / Other Planner Options", null, "1", "2147483647", null, "Sets the FROM-list size beyond which subqueries are not collapsed.", "The planner will merge subqueries into upper queries if the resulting FROM list would have no more than this many items."},
            {"geqo", "on", "bool", "user", "Query Tuning / Genetic Query Optimizer", null, null, null, null, "Enables genetic query optimization.", "This algorithm attempts to do planning without exhaustive searching."},
            {"geqo_effort", "5", "integer", "user", "Query Tuning / Genetic Query Optimizer", null, "1", "10", null, "GEQO: effort is used to set the default for other GEQO parameters."},
            {"geqo_generations", "0", "integer", "user", "Query Tuning / Genetic Query Optimizer", null, "0", "2147483647", null, "GEQO: number of iterations of the algorithm.", "0 means use a suitable default value."},
            {"geqo_pool_size", "0", "integer", "user", "Query Tuning / Genetic Query Optimizer", null, "0", "2147483647", null, "GEQO: number of individuals in the population.", "0 means use a suitable default value."},
            {"geqo_seed", "0", "real", "user", "Query Tuning / Genetic Query Optimizer", null, "0", "1", null, "GEQO: seed for random path selection."},
            {"geqo_selection_bias", "2", "real", "user", "Query Tuning / Genetic Query Optimizer", null, "1.5", "2", null, "GEQO: selective pressure within the population."},
            {"geqo_threshold", "12", "integer", "user", "Query Tuning / Genetic Query Optimizer", null, "2", "2147483647", null, "Sets the threshold of FROM items beyond which GEQO is used."},
            {"gin_fuzzy_search_limit", "0", "integer", "user", "Client Connection Defaults / Other Defaults", null, "0", "2147483647", null, "Sets the maximum allowed result for exact search by GIN.", "0 means no limit."},
            {"gin_pending_list_limit", "4096", "integer", "user", "Client Connection Defaults / Statement Behavior", "kB", "64", "2147483647", null, "Sets the maximum size of the pending list for GIN index."},
            {"hash_mem_multiplier", "2", "real", "user", "Resource Usage / Memory", null, "1", "1000", null, "Multiple of \"work_mem\" to use for hash tables."},
            {"huge_pages", "try", "enum", "postmaster", "Resource Usage / Memory", null, null, null, "off,on,try", "Use of huge pages on Linux or Windows."},
            {"jit_above_cost", "100000", "real", "user", "Query Tuning / Planner Cost Constants", null, "-1", "1.79769e+308", null, "Perform JIT compilation if query is more expensive.", "-1 disables JIT compilation."},
            {"jit_inline_above_cost", "500000", "real", "user", "Query Tuning / Planner Cost Constants", null, "-1", "1.79769e+308", null, "Perform JIT inlining if query is more expensive.", "-1 disables inlining."},
            {"jit_optimize_above_cost", "500000", "real", "user", "Query Tuning / Planner Cost Constants", null, "-1", "1.79769e+308", null, "Optimize JIT-compiled functions if query is more expensive.", "-1 disables optimization."},
            {"join_collapse_limit", "8", "integer", "user", "Query Tuning / Other Planner Options", null, "1", "2147483647", null, "Sets the FROM-list size beyond which JOIN constructs are not flattened.", "The planner will flatten explicit JOIN constructs into lists of FROM items whenever a list of no more than this many items would result."},
            {"krb_server_keyfile", "", "string", "sighup", "Connections and Authentication / Authentication", null, null, null, null, "Sets the location of the Kerberos server key file."},
            {"listen_addresses", "localhost", "string", "postmaster", "Connections and Authentication / Connection Settings", null, null, null, null, "Sets the host name or IP address(es) to listen to."},
            {"local_preload_libraries", "", "string", "user", "Client Connection Defaults / Shared Library Preloading", null, null, null, null, "Lists unprivileged shared libraries to preload into each backend."},
            {"log_autovacuum_min_duration", "600000", "integer", "sighup", "Reporting and Logging / What to Log", "ms", "-1", "2147483647", null, "Sets the minimum execution time above which autovacuum actions will be logged.", "-1 disables logging autovacuum actions. 0 means log all autovacuum actions."},
            {"log_checkpoints", "on", "bool", "sighup", "Reporting and Logging / What to Log", null, null, null, null, "Logs each checkpoint."},
            {"log_connections", "", "string", "superuser-backend", "Reporting and Logging / What to Log", null, null, null, null, "Logs specified aspects of connection establishment and setup."},
            {"log_destination", "stderr", "string", "sighup", "Reporting and Logging / Where to Log", null, null, null, null, "Sets the destination for server log output.", "Valid values are combinations of \"stderr\", \"syslog\", \"csvlog\", \"jsonlog\", and \"eventlog\", depending on the platform."},
            {"log_disconnections", "off", "bool", "superuser-backend", "Reporting and Logging / What to Log", null, null, null, null, "Logs end of a session, including duration."},
            {"log_duration", "off", "bool", "superuser", "Reporting and Logging / What to Log", null, null, null, null, "Logs the duration of each completed SQL statement."},
            {"log_error_verbosity", "default", "enum", "superuser", "Reporting and Logging / What to Log", null, null, null, "terse,default,verbose", "Sets the verbosity of logged messages."},
            {"log_line_prefix", "%m [%p] ", "string", "sighup", "Reporting and Logging / What to Log", null, null, null, null, "Controls information prefixed to each log line.", "An empty string means no prefix."},
            {"log_statement_stats", "off", "bool", "superuser", "Statistics / Monitoring", null, null, null, null, "Writes cumulative performance statistics to the server log."},
            {"log_temp_files", "-1", "integer", "superuser", "Reporting and Logging / What to Log", "kB", "-1", "2147483647", null, "Log the use of temporary files larger than this number of kilobytes.", "-1 disables logging temporary files. 0 means log all temporary files."},
            {"logical_decoding_work_mem", "65536", "integer", "user", "Resource Usage / Memory", "kB", "64", "2147483647", null, "Sets the maximum memory to be used for logical decoding.", "This much memory can be used by each internal reorder buffer before spilling to disk."},
            {"maintenance_io_concurrency", "16", "integer", "user", "Resource Usage / I/O", null, "0", "1000", null, "A variant of \"effective_io_concurrency\" that is used for maintenance work.", "0 disables simultaneous requests."},
            {"max_files_per_process", "1000", "integer", "postmaster", "Resource Usage / Kernel Resources", null, "64", "2147483647", null, "Sets the maximum number of files each server process is allowed to open simultaneously."},
            {"max_notify_queue_pages", "1048576", "integer", "postmaster", "Resource Usage / Disk", null, "64", "2147483647", null, "Sets the maximum number of allocated pages for NOTIFY / LISTEN queue."},
            {"max_stack_depth", "2048", "integer", "superuser", "Resource Usage / Memory", "kB", "100", "2147483647", null, "Sets the maximum stack depth, in kilobytes."},
            {"min_parallel_index_scan_size", "64", "integer", "user", "Query Tuning / Planner Cost Constants", "8kB", "0", "715827882", null, "Sets the minimum amount of index data for a parallel scan.", "If the planner estimates that it will read a number of index pages too small to reach this limit, a parallel scan will not be considered."},
            {"parallel_setup_cost", "1000", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of starting up worker processes for parallel query."},
            {"parallel_tuple_cost", "0.1", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of passing each tuple (row) from worker to leader backend."},
            {"port", "5432", "integer", "postmaster", "Connections and Authentication / Connection Settings", null, "1", "65535", null, "Sets the TCP port the server listens on."},
            {"recursive_worktable_factor", "10", "real", "user", "Query Tuning / Other Planner Options", null, "0.001", "1e+06", null, "Sets the planner's estimate of the average size of a recursive query's working table."},
            {"session_preload_libraries", "", "string", "superuser", "Client Connection Defaults / Shared Library Preloading", null, null, null, null, "Lists shared libraries to preload into each backend."},
            {"shared_preload_libraries", "", "string", "postmaster", "Client Connection Defaults / Shared Library Preloading", null, null, null, null, "Lists shared libraries to preload into server."},
            {"ssl_ciphers", "HIGH:MEDIUM:+3DES:!aNULL", "string", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Sets the list of allowed TLSv1.2 (and lower) ciphers."},
            {"stats_fetch_consistency", "cache", "enum", "user", "Statistics / Cumulative Query and Index Statistics", null, null, null, "none,cache,snapshot", "Sets the consistency of accesses to statistics data."},
            {"superuser_reserved_connections", "3", "integer", "postmaster", "Connections and Authentication / Connection Settings", null, "0", "262143", null, "Sets the number of connection slots reserved for superusers."},
            {"tcp_keepalives_idle", "0", "integer", "user", "Connections and Authentication / TCP Settings", "s", "0", "2147483647", null, "Time between issuing TCP keepalives.", "0 means use the system default."},
            {"timezone_abbreviations", "Default", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Selects a file of time zone abbreviations."},
            {"transform_null_equals", "off", "bool", "user", "Version and Platform Compatibility / Other Platforms and Clients", null, null, null, null, "Treats \"expr=NULL\" as \"expr IS NULL\".", "When turned on, expressions of the form expr = NULL (or NULL = expr) are treated as expr IS NULL, that is, they return true if expr evaluates to the null value, and false otherwise. The correct behavior of expr = NULL is to always return null (unknown)."},
            {"unix_socket_directories", "", "string", "postmaster", "Connections and Authentication / Connection Settings", null, null, null, null, "Sets the directories where Unix-domain sockets will be created."},
            {"vacuum_buffer_usage_limit", "2048", "integer", "user", "Resource Usage / Memory", "kB", "0", "16777216", null, "Sets the buffer pool size for VACUUM, ANALYZE, and autovacuum."},
            {"wal_buffers", "512", "integer", "postmaster", "Write-Ahead Log / Settings", "8kB", "-1", "262143", null, "Sets the number of disk-page buffers in shared memory for WAL.", "-1 means use a fraction of \"shared_buffers\"."},
            {"wal_sync_method", "open_datasync", "enum", "sighup", "Write-Ahead Log / Settings", null, null, null, "fsync,fdatasync,open_datasync", "Selects the method used for forcing WAL updates to disk."},
            {"wal_writer_delay", "200", "integer", "sighup", "Write-Ahead Log / Settings", "ms", "1", "10000", null, "Time between WAL flushes performed in the WAL writer."},
            {"wal_writer_flush_after", "128", "integer", "sighup", "Write-Ahead Log / Settings", "8kB", "0", "2147483647", null, "Amount of WAL written out by WAL writer that triggers a flush."},
            {"xmlbinary", "base64", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "base64,hex", "Sets how binary values are to be encoded in XML."},
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

    /**
     * The compiled-in default PostgreSQL reports in {@code pg_settings.boot_val} for parameters
     * whose running value memgres derives from its own environment instead. {@code boot_val} is
     * what the server would use with no configuration at all — the value compiled in, not the one
     * this machine arrived at — and a client comparing two servers' configurations reads it for
     * exactly that reason. The running value stays in the definition above; only the reported boot
     * value is corrected here.
     *
     * <p>Not everything PostgreSQL compiles in is the same everywhere: the flush-after parameters
     * are 0 where the platform has no way to ask for a flush and a real page count where it has,
     * so PostgreSQL on Windows boots them at 0 and on Linux at 64 and 32. memgres runs them at 64
     * and 32, so overriding the reported boot value to 0 made the row contradict itself — the
     * setting said one thing and the value it was said to have booted from said another. They are
     * left out of this table and report what memgres actually boots with.
     */
    private static final String[][] BOOT_VAL_OVERRIDES = {
            {"application_name", ""},
            {"client_encoding", "SQL_ASCII"},
            {"lc_messages", ""},
            {"lc_monetary", "C"},
            {"lc_numeric", "C"},
            {"lc_time", "C"},
            {"max_stack_depth", "100"},
            {"server_encoding", "SQL_ASCII"},
            {"timezone", "GMT"},
            {"wal_buffers", "-1"},
    };

    /** A single parameter's definition: its boot value plus the metadata pg_settings reports. */
    public static final class Def {
        public final String name;
        /** The value memgres starts the parameter at — what RESET restores and reset_val reports. */
        public final String defaultVal;
        /** The compiled-in default, as pg_settings.boot_val reports it. */
        public final String bootVal;
        /** The sentence pg_settings.extra_desc carries, or null when the parameter has none. */
        public final String extraDesc;
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

        Def(String[] row, boolean listed, String bootValOverride) {
            this.name = row[0];
            this.defaultVal = row[1];
            this.bootVal = bootValOverride != null ? bootValOverride : row[1];
            this.extraDesc = row.length > 10 ? row[10] : null;
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
        Map<String, String> bootOverrides = new HashMap<>();
        for (String[] row : BOOT_VAL_OVERRIDES) bootOverrides.put(row[0], row[1]);
        // Sorted by name: SHOW ALL lists the parameters in that order, so the order a parameter
        // is declared in above cannot change what a client sees.
        Map<String, Def> defs = new TreeMap<>();
        for (String[] row : SETTING_DEFS) defs.put(row[0], new Def(row, true, bootOverrides.get(row[0])));
        for (String[] row : HIDDEN_DEFS) defs.put(row[0], new Def(row, false, bootOverrides.get(row[0])));
        DEFINITIONS = Collections.unmodifiableMap(new LinkedHashMap<>(defs));
        for (Def d : DEFINITIONS.values()) {
            if (d.defaultVal != null) DEFAULTS.put(d.name, d.defaultVal);
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
            if (d.listed && d.defaultVal != null) result.put(d.name, d.defaultVal);
        }
        for (Map.Entry<String, String> e : sessionOverrides.entrySet()) {
            Def d = DEFINITIONS.get(e.getKey());
            if (d == null) {
                // A custom parameter this session invented: SET and current_setting answer for
                // it, but PostgreSQL keeps it out of pg_settings and SHOW ALL until an
                // extension declares it, so listing it would invent a row no server has.
                continue;
            }
            if (!d.listed) continue;
            result.put(e.getKey(), e.getValue());
        }
        return result;
    }

    /**
     * Refuse a parameter name no server carries. PostgreSQL accepts a qualified name
     * ({@code myapp.thing}) as a custom placeholder and refuses everything else with 42704 —
     * so a misspelt parameter is reported where it is written rather than silently taking
     * effect under a name nothing reads.
     */
    public static void requireKnown(String name) {
        if (name == null) return;
        String key = name.toLowerCase();
        if (key.indexOf('.') >= 0) return;
        if (DEFINITIONS.containsKey(key)) return;
        throw new MemgresException("unrecognized configuration parameter \"" + name + "\"", "42704");
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
