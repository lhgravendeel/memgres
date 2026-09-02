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
            {"allow_alter_system", "on", "bool", "sighup", "Version and Platform Compatibility / Other Platforms and Clients", null, null, null, null, "Allows running the ALTER SYSTEM command.", "Can be set to off for environments where global configuration changes should be made using a different method."},
            {"allow_in_place_tablespaces", "off", "bool", "superuser", "Developer Options", null, null, null, null, "Allows tablespaces directly inside pg_tblspc, for testing."},
            {"allow_system_table_mods", "off", "bool", "superuser", "Developer Options", null, null, null, null, "Allows modifications of the structure of system tables."},
            // A connection that names itself is named that; one that does not has no name at
            // all. Defaulting to the server's own name, RESET application_name gave every
            // connection the same name and a reader of pg_stat_activity could not tell which
            // client had said nothing from one that had said "memgres".
            {"application_name", "", "string", "user", "Reporting and Logging / What to Log", null, null, null, null, "Sets the application name to be reported in statistics and logs."},
            {"archive_cleanup_command", "", "string", "sighup", "Write-Ahead Log / Archive Recovery", null, null, null, null, "Sets the shell command that will be executed at every restart point."},
            {"archive_command", "", "string", "sighup", "Write-Ahead Log / Archiving", null, null, null, null, "Sets the shell command that will be called to archive a WAL file.", "An empty string means use \"archive_library\"."},
            {"archive_library", "", "string", "sighup", "Write-Ahead Log / Archiving", null, null, null, null, "Sets the library that will be called to archive a WAL file.", "An empty string means use \"archive_command\"."},
            {"archive_mode", "off", "enum", "postmaster", "Write-Ahead Log / Archiving", null, null, null, "always,on,off", "Allows archiving of WAL files using \"archive_command\"."},
            {"archive_timeout", "0", "integer", "sighup", "Write-Ahead Log / Archiving", "s", "0", "1073741823", null, "Sets the amount of time to wait before forcing a switch to the next WAL file.", "0 disables the timeout."},
            {"array_nulls", "on", "bool", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Enables input of NULL elements in arrays.", "When turned on, unquoted NULL in an array input value means a null value; otherwise it is taken literally."},
            {"authentication_timeout", "60", "integer", "sighup", "Connections and Authentication / Authentication", "s", "1", "600", null, "Sets the maximum allowed time to complete client authentication."},
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
            {"autovacuum_vacuum_max_threshold", "100000000", "integer", "sighup", "Vacuuming / Automatic Vacuuming", null, "-1", "2147483647", null, "Maximum number of tuple updates or deletes prior to vacuum.", "-1 disables the maximum threshold."},
            {"autovacuum_vacuum_scale_factor", "0.2", "real", "sighup", "Vacuuming / Automatic Vacuuming", null, "0", "100", null, "Number of tuple updates or deletes prior to vacuum as a fraction of reltuples."},
            {"autovacuum_vacuum_threshold", "50", "integer", "sighup", "Vacuuming / Automatic Vacuuming", null, "0", "2147483647", null, "Minimum number of tuple updates or deletes prior to vacuum."},
            {"autovacuum_work_mem", "-1", "integer", "sighup", "Resource Usage / Memory", "kB", "-1", "2147483647", null, "Sets the maximum memory to be used by each autovacuum worker process.", "-1 means use \"maintenance_work_mem\"."},
            {"autovacuum_worker_slots", "16", "integer", "postmaster", "Vacuuming / Automatic Vacuuming", null, "1", "262143", null, "Sets the number of backend slots to allocate for autovacuum workers."},
            {"backend_flush_after", "0", "integer", "user", "Resource Usage / I/O", "8kB", "0", "256", null, "Number of pages after which previously performed writes are flushed to disk.", "0 disables forced writeback."},
            {"backslash_quote", "safe_encoding", "enum", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, "safe_encoding,on,off", "Sets whether \"\\'\" is allowed in string literals."},
            {"backtrace_functions", "", "string", "superuser", "Developer Options", null, null, null, null, "Log backtrace for errors in these functions."},
            {"bgwriter_delay", "200", "integer", "sighup", "Resource Usage / Background Writer", "ms", "10", "10000", null, "Background writer sleep time between rounds."},
            {"bgwriter_flush_after", "64", "integer", "sighup", "Resource Usage / Background Writer", "8kB", "0", "256", null, "Number of pages after which previously performed writes are flushed to disk.", "0 disables forced writeback."},
            {"bgwriter_lru_maxpages", "100", "integer", "sighup", "Resource Usage / Background Writer", null, "0", "1073741823", null, "Background writer maximum number of LRU pages to flush per round.", "0 disables background writing."},
            {"bgwriter_lru_multiplier", "2", "real", "sighup", "Resource Usage / Background Writer", null, "0", "10", null, "Multiple of the average buffer usage to free per round."},
            {"block_size", "8192", "integer", "internal", "Preset Options", null, "8192", "8192", null, "Shows the size of a disk block."},
            {"bonjour", "off", "bool", "postmaster", "Connections and Authentication / Connection Settings", null, null, null, null, "Enables advertising the server via Bonjour."},
            {"bonjour_name", "", "string", "postmaster", "Connections and Authentication / Connection Settings", null, null, null, null, "Sets the Bonjour service name.", "An empty string means use the computer name."},
            {"bytea_output", "hex", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "escape,hex", "Sets the output format for bytea."},
            {"check_function_bodies", "on", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Check routine bodies during CREATE FUNCTION and CREATE PROCEDURE."},
            {"checkpoint_completion_target", "0.9", "real", "sighup", "Write-Ahead Log / Checkpoints", null, "0", "1", null, "Time spent flushing dirty buffers during checkpoint, as fraction of checkpoint interval."},
            {"checkpoint_flush_after", "32", "integer", "sighup", "Write-Ahead Log / Checkpoints", "8kB", "0", "256", null, "Number of pages after which previously performed writes are flushed to disk.", "0 disables forced writeback."},
            {"checkpoint_timeout", "300", "integer", "sighup", "Write-Ahead Log / Checkpoints", "s", "30", "86400", null, "Sets the maximum time between automatic WAL checkpoints."},
            {"checkpoint_warning", "30", "integer", "sighup", "Write-Ahead Log / Checkpoints", "s", "0", "2147483647", null, "Sets the maximum time before warning if checkpoints triggered by WAL volume happen too frequently.", "Write a message to the server log if checkpoints caused by the filling of WAL segment files happen more frequently than this amount of time. 0 disables the warning."},
            {"client_connection_check_interval", "0", "integer", "user", "Connections and Authentication / TCP Settings", "ms", "0", "2147483647", null, "Sets the time interval between checks for disconnection while running queries.", "0 disables connection checks."},
            {"client_encoding", "UTF8", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the client's character set encoding."},
            {"client_min_messages", "notice", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "debug5,debug4,debug3,debug2,debug1,log,notice,warning,error", "Sets the message levels that are sent to the client.", "Each level includes all the levels that follow it. The later the level, the fewer messages are sent."},
            {"cluster_name", "", "string", "postmaster", "Reporting and Logging / Process Title", null, null, null, null, "Sets the name of the cluster, which is included in the process title."},
            {"commit_delay", "0", "integer", "superuser", "Write-Ahead Log / Settings", null, "0", "100000", null, "Sets the delay in microseconds between transaction commit and flushing WAL to disk."},
            {"commit_siblings", "5", "integer", "user", "Write-Ahead Log / Settings", null, "0", "1000", null, "Sets the minimum number of concurrent open transactions required before performing \"commit_delay\"."},
            {"commit_timestamp_buffers", "0", "integer", "postmaster", "Resource Usage / Memory", "8kB", "0", "131072", null, "Sets the size of the dedicated buffer pool used for the commit timestamp cache.", "0 means use a fraction of \"shared_buffers\"."},
            {"compute_query_id", "auto", "enum", "superuser", "Statistics / Monitoring", null, null, null, "auto,regress,on,off", "Enables in-core computation of query identifiers."},
            {"config_file", "", "string", "postmaster", "File Locations", null, null, null, null, "Sets the server's main configuration file."},
            {"constraint_exclusion", "partition", "enum", "user", "Query Tuning / Other Planner Options", null, null, null, "partition,on,off", "Enables the planner to use constraints to optimize queries.", "Table scans will be skipped if their constraints guarantee that no rows match the query."},
            {"cpu_index_tuple_cost", "0.005", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of processing each index entry during an index scan."},
            {"cpu_operator_cost", "0.0025", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of processing each operator or function call."},
            {"cpu_tuple_cost", "0.01", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of processing each tuple (row)."},
            {"createrole_self_grant", "", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets whether a CREATEROLE user automatically grants the role to themselves, and with which options.", "An empty string disables automatic self grants."},
            {"cursor_tuple_fraction", "0.1", "real", "user", "Query Tuning / Other Planner Options", null, "0", "1", null, "Sets the planner's estimate of the fraction of a cursor's rows that will be retrieved."},
            {"data_checksums", "off", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether data checksums are turned on for this cluster."},
            {"data_directory", "", "string", "postmaster", "File Locations", null, null, null, null, "Sets the server's data directory."},
            {"data_directory_mode", "448", "integer", "internal", "Preset Options", null, "0", "511", null, "Shows the mode of the data directory.", "The parameter value is a numeric mode specification in the form accepted by the chmod and umask system calls. (To use the customary octal format the number must start with a 0 (zero).)"},
            {"data_sync_retry", "off", "bool", "postmaster", "Error Handling", null, null, null, null, "Whether to continue running after a failure to sync data files."},
            {"datestyle", "ISO, MDY", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the display format for date and time values.", "Also controls interpretation of ambiguous date inputs."},
            {"deadlock_timeout", "1000", "integer", "superuser", "Lock Management", "ms", "1", "2147483647", null, "Sets the time to wait on a lock before checking for deadlock."},
            {"debug_assertions", "off", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether the running server has assertion checks enabled."},
            {"debug_discard_caches", "0", "integer", "superuser", "Developer Options", null, "0", "0", null, "Aggressively flush system caches for debugging purposes.", "0 means use normal caching behavior."},
            {"debug_io_direct", "", "string", "postmaster", "Developer Options", null, null, null, null, "Use direct I/O for file access.", "An empty string disables direct I/O."},
            {"debug_logical_replication_streaming", "buffered", "enum", "user", "Developer Options", null, null, null, "buffered,immediate", "Forces immediate streaming or serialization of changes in large transactions.", "On the publisher, it allows streaming or serializing each change in logical decoding. On the subscriber, it allows serialization of all changes to files and notifies the parallel apply workers to read and apply them at the end of the transaction."},
            {"debug_parallel_query", "off", "enum", "user", "Developer Options", null, null, null, "off,on,regress", "Forces the planner's use parallel query nodes.", "This can be useful for testing the parallel query infrastructure by forcing the planner to generate plans that contain nodes that perform tuple communication between workers and the main process."},
            {"debug_pretty_print", "on", "bool", "user", "Reporting and Logging / What to Log", null, null, null, null, "Indents parse and plan tree displays."},
            {"debug_print_parse", "off", "bool", "user", "Reporting and Logging / What to Log", null, null, null, null, "Logs each query's parse tree."},
            {"debug_print_plan", "off", "bool", "user", "Reporting and Logging / What to Log", null, null, null, null, "Logs each query's execution plan."},
            {"debug_print_rewritten", "off", "bool", "user", "Reporting and Logging / What to Log", null, null, null, null, "Logs each query's rewritten parse tree."},
            {"default_statistics_target", "100", "integer", "user", "Query Tuning / Other Planner Options", null, "1", "10000", null, "Sets the default statistics target.", "This applies to table columns that have not had a column-specific target set via ALTER TABLE SET STATISTICS."},
            {"default_table_access_method", "heap", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the default table access method for new tables."},
            {"default_tablespace", "", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the default tablespace to create tables and indexes in.", "An empty string means use the database's default tablespace."},
            {"default_text_search_config", "pg_catalog.english", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets default text search configuration."},
            {"default_toast_compression", "pglz", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "pglz,lz4", "Sets the default compression method for compressible values."},
            {"default_transaction_deferrable", "off", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the default deferrable status of new transactions."},
            {"default_transaction_isolation", "read committed", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "serializable,repeatable read,read committed,read uncommitted", "Sets the transaction isolation level of each new transaction."},
            {"default_transaction_read_only", "off", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the default read-only status of new transactions."},
            {"dynamic_library_path", "$libdir", "string", "superuser", "Client Connection Defaults / Other Defaults", null, null, null, null, "Sets the path for dynamically loadable modules.", "If a dynamically loadable module needs to be opened and the specified name does not have a directory component (i.e., the name does not contain a slash), the system will search this path for the specified file."},
            {"dynamic_shared_memory_type", "windows", "enum", "postmaster", "Resource Usage / Memory", null, null, null, "windows", "Selects the dynamic shared memory implementation used."},
            {"effective_cache_size", "524288", "integer", "user", "Query Tuning / Planner Cost Constants", "8kB", "1", "2147483647", null, "Sets the planner's assumption about the total size of the data caches.", "That is, the total size of the caches (kernel cache and shared buffers) used for PostgreSQL data files. This is measured in disk pages, which are normally 8 kB each."},
            {"effective_io_concurrency", "16", "integer", "user", "Resource Usage / I/O", null, "0", "1000", null, "Number of simultaneous requests that can be handled efficiently by the disk subsystem.", "0 disables simultaneous requests."},
            {"enable_async_append", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of async append plans."},
            {"enable_bitmapscan", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of bitmap-scan plans."},
            {"enable_distinct_reordering", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables reordering of DISTINCT keys."},
            {"enable_gathermerge", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of gather merge plans."},
            {"enable_group_by_reordering", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables reordering of GROUP BY keys."},
            {"enable_hashagg", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of hashed aggregation plans."},
            {"enable_hashjoin", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of hash join plans."},
            {"enable_incremental_sort", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of incremental sort steps."},
            {"enable_indexonlyscan", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of index-only-scan plans."},
            {"enable_indexscan", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of index-scan plans."},
            {"enable_material", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of materialization."},
            {"enable_memoize", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of memoization."},
            {"enable_mergejoin", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of merge join plans."},
            {"enable_nestloop", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of nested-loop join plans."},
            {"enable_parallel_append", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of parallel append plans."},
            {"enable_parallel_hash", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of parallel hash plans."},
            {"enable_partition_pruning", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables plan-time and execution-time partition pruning.", "Allows the query planner and executor to compare partition bounds to conditions in the query to determine which partitions must be scanned."},
            {"enable_partitionwise_aggregate", "off", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables partitionwise aggregation and grouping."},
            {"enable_partitionwise_join", "off", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables partitionwise join."},
            {"enable_presorted_aggregate", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's ability to produce plans that provide presorted input for ORDER BY / DISTINCT aggregate functions.", "Allows the query planner to build plans that provide presorted input for aggregate functions with an ORDER BY / DISTINCT clause.  When disabled, implicit sorts are always performed during execution."},
            {"enable_self_join_elimination", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables removal of unique self-joins."},
            {"enable_seqscan", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of sequential-scan plans."},
            {"enable_sort", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of explicit sort steps."},
            {"enable_tidscan", "on", "bool", "user", "Query Tuning / Planner Method Configuration", null, null, null, null, "Enables the planner's use of TID scan plans."},
            {"escape_string_warning", "on", "bool", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Warn about backslash escapes in ordinary string literals."},
            {"event_source", "PostgreSQL", "string", "postmaster", "Reporting and Logging / Where to Log", null, null, null, null, "Sets the application name used to identify PostgreSQL messages in the event log."},
            {"event_triggers", "on", "bool", "superuser", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Enables event triggers.", "When enabled, event triggers will fire for all applicable statements."},
            {"exit_on_error", "off", "bool", "user", "Error Handling", null, null, null, null, "Terminate session on any error."},
            {"extension_control_path", "$system", "string", "superuser", "Client Connection Defaults / Other Defaults", null, null, null, null, "Sets the path for extension control files.", "The remaining extension script and secondary control files are then loaded from the same directory where the primary control file was found."},
            {"external_pid_file", "", "string", "postmaster", "File Locations", null, null, null, null, "Writes the postmaster PID to the specified file."},
            {"extra_float_digits", "1", "integer", "user", "Client Connection Defaults / Locale and Formatting", null, "-15", "3", null, "Sets the number of digits displayed for floating-point values.", "This affects real, double precision, and geometric data types. A zero or negative parameter value is added to the standard number of digits (FLT_DIG or DBL_DIG as appropriate). Any value greater than zero selects precise output mode."},
            {"file_copy_method", "copy", "enum", "user", "Resource Usage / Disk", null, null, null, "copy", "Selects the file copy method."},
            {"from_collapse_limit", "8", "integer", "user", "Query Tuning / Other Planner Options", null, "1", "2147483647", null, "Sets the FROM-list size beyond which subqueries are not collapsed.", "The planner will merge subqueries into upper queries if the resulting FROM list would have no more than this many items."},
            {"fsync", "on", "bool", "sighup", "Write-Ahead Log / Settings", null, null, null, null, "Forces synchronization of updates to disk.", "The server will use the fsync() system call in several places to make sure that updates are physically written to disk. This ensures that a database cluster will recover to a consistent state after an operating system or hardware crash."},
            {"full_page_writes", "on", "bool", "sighup", "Write-Ahead Log / Settings", null, null, null, null, "Writes full pages to WAL when first modified after a checkpoint.", "A page write in process during an operating system crash might be only partially written to disk.  During recovery, the row changes stored in WAL are not enough to recover.  This option writes pages when first modified after a checkpoint to WAL so full recovery is possible."},
            {"geqo", "on", "bool", "user", "Query Tuning / Genetic Query Optimizer", null, null, null, null, "Enables genetic query optimization.", "This algorithm attempts to do planning without exhaustive searching."},
            {"geqo_effort", "5", "integer", "user", "Query Tuning / Genetic Query Optimizer", null, "1", "10", null, "GEQO: effort is used to set the default for other GEQO parameters."},
            {"geqo_generations", "0", "integer", "user", "Query Tuning / Genetic Query Optimizer", null, "0", "2147483647", null, "GEQO: number of iterations of the algorithm.", "0 means use a suitable default value."},
            {"geqo_pool_size", "0", "integer", "user", "Query Tuning / Genetic Query Optimizer", null, "0", "2147483647", null, "GEQO: number of individuals in the population.", "0 means use a suitable default value."},
            {"geqo_seed", "0", "real", "user", "Query Tuning / Genetic Query Optimizer", null, "0", "1", null, "GEQO: seed for random path selection."},
            {"geqo_selection_bias", "2", "real", "user", "Query Tuning / Genetic Query Optimizer", null, "1.5", "2", null, "GEQO: selective pressure within the population."},
            {"geqo_threshold", "12", "integer", "user", "Query Tuning / Genetic Query Optimizer", null, "2", "2147483647", null, "Sets the threshold of FROM items beyond which GEQO is used."},
            {"gin_fuzzy_search_limit", "0", "integer", "user", "Client Connection Defaults / Other Defaults", null, "0", "2147483647", null, "Sets the maximum allowed result for exact search by GIN.", "0 means no limit."},
            {"gin_pending_list_limit", "4096", "integer", "user", "Client Connection Defaults / Statement Behavior", "kB", "64", "2147483647", null, "Sets the maximum size of the pending list for GIN index."},
            {"gss_accept_delegation", "off", "bool", "sighup", "Connections and Authentication / Authentication", null, null, null, null, "Sets whether GSSAPI delegation should be accepted from the client."},
            {"hash_mem_multiplier", "2", "real", "user", "Resource Usage / Memory", null, "1", "1000", null, "Multiple of \"work_mem\" to use for hash tables."},
            {"hba_file", "", "string", "postmaster", "File Locations", null, null, null, null, "Sets the server's \"hba\" configuration file."},
            {"hot_standby", "on", "bool", "postmaster", "Replication / Standby Servers", null, null, null, null, "Allows connections and queries during recovery."},
            {"hot_standby_feedback", "off", "bool", "sighup", "Replication / Standby Servers", null, null, null, null, "Allows feedback from a hot standby to the primary that will avoid query conflicts."},
            {"huge_page_size", "0", "integer", "postmaster", "Resource Usage / Memory", "kB", "0", "2147483647", null, "The size of huge page that should be requested.", "0 means use the system default."},
            {"huge_pages", "try", "enum", "postmaster", "Resource Usage / Memory", null, null, null, "off,on,try", "Use of huge pages on Linux or Windows."},
            {"huge_pages_status", "unknown", "enum", "internal", "Preset Options", null, null, null, "off,on,unknown", "Indicates the status of huge pages."},
            {"icu_validation_level", "warning", "enum", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, "disabled,debug5,debug4,debug3,debug2,debug1,log,notice,warning,error", "Log level for reporting invalid ICU locale strings."},
            {"ident_file", "", "string", "postmaster", "File Locations", null, null, null, null, "Sets the server's \"ident\" configuration file."},
            {"idle_in_transaction_session_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed idle time between queries, when in a transaction.", "0 disables the timeout."},
            {"idle_replication_slot_timeout", "0", "integer", "sighup", "Replication / Sending Servers", "s", "0", "2147483647", null, "Sets the duration a replication slot can remain idle before it is invalidated."},
            {"idle_session_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed idle time between queries, when not in a transaction.", "0 disables the timeout."},
            {"ignore_checksum_failure", "off", "bool", "superuser", "Developer Options", null, null, null, null, "Continues processing after a checksum failure.", "Detection of a checksum failure normally causes PostgreSQL to report an error, aborting the current transaction. Setting ignore_checksum_failure to true causes the system to ignore the failure (but still report a warning), and continue processing. This behavior could cause crashes or other serious problems. Only has an effect if checksums are enabled."},
            {"ignore_invalid_pages", "off", "bool", "postmaster", "Developer Options", null, null, null, null, "Continues recovery after an invalid pages failure.", "Detection of WAL records having references to invalid pages during recovery causes PostgreSQL to raise a PANIC-level error, aborting the recovery. Setting \"ignore_invalid_pages\" to true causes the system to ignore invalid page references in WAL records (but still report a warning), and continue recovery. This behavior may cause crashes, data loss, propagate or hide corruption, or other serious problems. Only has an effect during recovery or in standby mode."},
            {"ignore_system_indexes", "off", "bool", "backend", "Developer Options", null, null, null, null, "Disables reading from system indexes.", "It does not prevent updating the indexes, so it is safe to use.  The worst consequence is slowness."},
            {"in_hot_standby", "off", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether hot standby is currently active."},
            {"integer_datetimes", "on", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether datetimes are integer based."},
            {"intervalstyle", "postgres", "enum", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, "postgres,postgres_verbose,sql_standard,iso_8601", "Sets the display format for interval values."},
            {"io_combine_limit", "16", "integer", "user", "Resource Usage / I/O", "8kB", "1", "16", null, "Limit on the size of data reads and writes."},
            {"io_max_combine_limit", "16", "integer", "postmaster", "Resource Usage / I/O", "8kB", "1", "16", null, "Server-wide limit that clamps io_combine_limit."},
            {"io_max_concurrency", "-1", "integer", "postmaster", "Resource Usage / I/O", null, "-1", "1024", null, "Max number of IOs that one process can execute simultaneously."},
            {"io_method", "worker", "enum", "postmaster", "Resource Usage / I/O", null, null, null, "sync,worker", "Selects the method for executing asynchronous I/O."},
            {"io_workers", "3", "integer", "sighup", "Resource Usage / I/O", null, "1", "32", null, "Number of IO worker processes, for io_method=worker."},
            {"jit", "on", "bool", "user", "Query Tuning / Other Planner Options", null, null, null, null, "Allow JIT compilation."},
            {"jit_above_cost", "100000", "real", "user", "Query Tuning / Planner Cost Constants", null, "-1", "1.79769e+308", null, "Perform JIT compilation if query is more expensive.", "-1 disables JIT compilation."},
            {"jit_debugging_support", "off", "bool", "superuser-backend", "Developer Options", null, null, null, null, "Register JIT-compiled functions with debugger."},
            {"jit_dump_bitcode", "off", "bool", "superuser", "Developer Options", null, null, null, null, "Write out LLVM bitcode to facilitate JIT debugging."},
            {"jit_expressions", "on", "bool", "user", "Developer Options", null, null, null, null, "Allow JIT compilation of expressions."},
            {"jit_inline_above_cost", "500000", "real", "user", "Query Tuning / Planner Cost Constants", null, "-1", "1.79769e+308", null, "Perform JIT inlining if query is more expensive.", "-1 disables inlining."},
            {"jit_optimize_above_cost", "500000", "real", "user", "Query Tuning / Planner Cost Constants", null, "-1", "1.79769e+308", null, "Optimize JIT-compiled functions if query is more expensive.", "-1 disables optimization."},
            {"jit_profiling_support", "off", "bool", "superuser-backend", "Developer Options", null, null, null, null, "Register JIT-compiled functions with perf profiler."},
            {"jit_provider", "llvmjit", "string", "postmaster", "Client Connection Defaults / Shared Library Preloading", null, null, null, null, "JIT provider to use."},
            {"jit_tuple_deforming", "on", "bool", "user", "Developer Options", null, null, null, null, "Allow JIT compilation of tuple deforming."},
            {"join_collapse_limit", "8", "integer", "user", "Query Tuning / Other Planner Options", null, "1", "2147483647", null, "Sets the FROM-list size beyond which JOIN constructs are not flattened.", "The planner will flatten explicit JOIN constructs into lists of FROM items whenever a list of no more than this many items would result."},
            {"krb_caseins_users", "off", "bool", "sighup", "Connections and Authentication / Authentication", null, null, null, null, "Sets whether Kerberos and GSSAPI user names should be treated as case-insensitive."},
            {"krb_server_keyfile", "", "string", "sighup", "Connections and Authentication / Authentication", null, null, null, null, "Sets the location of the Kerberos server key file."},
            {"lc_messages", "en_US.UTF-8", "string", "superuser", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the language in which messages are displayed.", "An empty string means use the operating system setting."},
            {"lc_monetary", "en_US.UTF-8", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the locale for formatting monetary amounts.", "An empty string means use the operating system setting."},
            {"lc_numeric", "en_US.UTF-8", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the locale for formatting numbers.", "An empty string means use the operating system setting."},
            {"lc_time", "en_US.UTF-8", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the locale for formatting date and time values.", "An empty string means use the operating system setting."},
            {"listen_addresses", "localhost", "string", "postmaster", "Connections and Authentication / Connection Settings", null, null, null, null, "Sets the host name or IP address(es) to listen to."},
            {"lo_compat_privileges", "off", "bool", "superuser", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Enables backward compatibility mode for privilege checks on large objects.", "Skips privilege checks when reading or modifying large objects, for compatibility with PostgreSQL releases prior to 9.0."},
            {"local_preload_libraries", "", "string", "user", "Client Connection Defaults / Shared Library Preloading", null, null, null, null, "Lists unprivileged shared libraries to preload into each backend."},
            {"lock_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed duration of any wait for a lock.", "0 disables the timeout."},
            {"log_autovacuum_min_duration", "600000", "integer", "sighup", "Reporting and Logging / What to Log", "ms", "-1", "2147483647", null, "Sets the minimum execution time above which autovacuum actions will be logged.", "-1 disables logging autovacuum actions. 0 means log all autovacuum actions."},
            {"log_checkpoints", "on", "bool", "sighup", "Reporting and Logging / What to Log", null, null, null, null, "Logs each checkpoint."},
            {"log_connections", "", "string", "superuser-backend", "Reporting and Logging / What to Log", null, null, null, null, "Logs specified aspects of connection establishment and setup."},
            {"log_destination", "stderr", "string", "sighup", "Reporting and Logging / Where to Log", null, null, null, null, "Sets the destination for server log output.", "Valid values are combinations of \"stderr\", \"syslog\", \"csvlog\", \"jsonlog\", and \"eventlog\", depending on the platform."},
            {"log_directory", "log", "string", "sighup", "Reporting and Logging / Where to Log", null, null, null, null, "Sets the destination directory for log files.", "Can be specified as relative to the data directory or as absolute path."},
            {"log_disconnections", "off", "bool", "superuser-backend", "Reporting and Logging / What to Log", null, null, null, null, "Logs end of a session, including duration."},
            {"log_duration", "off", "bool", "superuser", "Reporting and Logging / What to Log", null, null, null, null, "Logs the duration of each completed SQL statement."},
            {"log_error_verbosity", "default", "enum", "superuser", "Reporting and Logging / What to Log", null, null, null, "terse,default,verbose", "Sets the verbosity of logged messages."},
            {"log_executor_stats", "off", "bool", "superuser", "Statistics / Monitoring", null, null, null, null, "Writes executor performance statistics to the server log."},
            {"log_file_mode", "384", "integer", "sighup", "Reporting and Logging / Where to Log", null, "0", "511", null, "Sets the file permissions for log files.", "The parameter value is expected to be a numeric mode specification in the form accepted by the chmod and umask system calls. (To use the customary octal format the number must start with a 0 (zero).)"},
            {"log_filename", "postgresql-%Y-%m-%d_%H%M%S.log", "string", "sighup", "Reporting and Logging / Where to Log", null, null, null, null, "Sets the file name pattern for log files."},
            {"log_hostname", "off", "bool", "sighup", "Reporting and Logging / What to Log", null, null, null, null, "Logs the host name in the connection logs.", "By default, connection logs only show the IP address of the connecting host. If you want them to show the host name you can turn this on, but depending on your host name resolution setup it might impose a non-negligible performance penalty."},
            {"log_line_prefix", "%m [%p] ", "string", "sighup", "Reporting and Logging / What to Log", null, null, null, null, "Controls information prefixed to each log line.", "An empty string means no prefix."},
            {"log_lock_failures", "off", "bool", "superuser", "Reporting and Logging / What to Log", null, null, null, null, "Logs lock failures."},
            {"log_lock_waits", "off", "bool", "superuser", "Reporting and Logging / What to Log", null, null, null, null, "Logs long lock waits."},
            {"log_min_duration_sample", "-1", "integer", "superuser", "Reporting and Logging / When to Log", "ms", "-1", "2147483647", null, "Sets the minimum execution time above which a sample of statements will be logged. Sampling is determined by \"log_statement_sample_rate\".", "-1 disables sampling. 0 means sample all statements."},
            {"log_min_duration_statement", "-1", "integer", "superuser", "Reporting and Logging / When to Log", "ms", "-1", "2147483647", null, "Sets the minimum execution time above which all statements will be logged.", "-1 disables logging statement durations. 0 means log all statement durations."},
            {"log_min_error_statement", "error", "enum", "superuser", "Reporting and Logging / When to Log", null, null, null, "debug5,debug4,debug3,debug2,debug1,info,notice,warning,error,log,fatal,panic", "Causes all statements generating error at or above this level to be logged.", "Each level includes all the levels that follow it. The later the level, the fewer messages are sent."},
            {"log_min_messages", "warning", "enum", "superuser", "Reporting and Logging / When to Log", null, null, null, "debug5,debug4,debug3,debug2,debug1,info,notice,warning,error,log,fatal,panic", "Sets the message levels that are logged.", "Each level includes all the levels that follow it. The later the level, the fewer messages are sent."},
            {"log_parameter_max_length", "-1", "integer", "superuser", "Reporting and Logging / What to Log", "B", "-1", "1073741823", null, "Sets the maximum length in bytes of data logged for bind parameter values when logging statements.", "-1 means log values in full."},
            {"log_parameter_max_length_on_error", "0", "integer", "user", "Reporting and Logging / What to Log", "B", "-1", "1073741823", null, "Sets the maximum length in bytes of data logged for bind parameter values when logging statements, on error.", "-1 means log values in full."},
            {"log_parser_stats", "off", "bool", "superuser", "Statistics / Monitoring", null, null, null, null, "Writes parser performance statistics to the server log."},
            {"log_planner_stats", "off", "bool", "superuser", "Statistics / Monitoring", null, null, null, null, "Writes planner performance statistics to the server log."},
            {"log_recovery_conflict_waits", "off", "bool", "sighup", "Reporting and Logging / What to Log", null, null, null, null, "Logs standby recovery conflict waits."},
            {"log_replication_commands", "off", "bool", "superuser", "Reporting and Logging / What to Log", null, null, null, null, "Logs each replication command."},
            {"log_rotation_age", "1440", "integer", "sighup", "Reporting and Logging / Where to Log", "min", "0", "35791394", null, "Sets the amount of time to wait before forcing log file rotation.", "0 disables time-based creation of new log files."},
            {"log_rotation_size", "10240", "integer", "sighup", "Reporting and Logging / Where to Log", "kB", "0", "2147483647", null, "Sets the maximum size a log file can reach before being rotated.", "0 disables size-based creation of new log files."},
            {"log_startup_progress_interval", "10000", "integer", "sighup", "Reporting and Logging / When to Log", "ms", "0", "2147483647", null, "Time between progress updates for long-running startup operations.", "0 disables progress updates."},
            {"log_statement", "none", "enum", "superuser", "Reporting and Logging / What to Log", null, null, null, "none,ddl,mod,all", "Sets the type of statements logged."},
            {"log_statement_sample_rate", "1", "real", "superuser", "Reporting and Logging / When to Log", null, "0", "1", null, "Fraction of statements exceeding \"log_min_duration_sample\" to be logged.", "Use a value between 0.0 (never log) and 1.0 (always log)."},
            {"log_statement_stats", "off", "bool", "superuser", "Statistics / Monitoring", null, null, null, null, "Writes cumulative performance statistics to the server log."},
            {"log_temp_files", "-1", "integer", "superuser", "Reporting and Logging / What to Log", "kB", "-1", "2147483647", null, "Log the use of temporary files larger than this number of kilobytes.", "-1 disables logging temporary files. 0 means log all temporary files."},
            {"log_timezone", "UTC", "string", "sighup", "Reporting and Logging / What to Log", null, null, null, null, "Sets the time zone to use in log messages."},
            {"log_transaction_sample_rate", "0", "real", "superuser", "Reporting and Logging / When to Log", null, "0", "1", null, "Sets the fraction of transactions from which to log all statements.", "Use a value between 0.0 (never log) and 1.0 (log all statements for all transactions)."},
            {"log_truncate_on_rotation", "off", "bool", "sighup", "Reporting and Logging / Where to Log", null, null, null, null, "Truncate existing log files of same name during log rotation."},
            {"logging_collector", "off", "bool", "postmaster", "Reporting and Logging / Where to Log", null, null, null, null, "Start a subprocess to capture stderr, csvlog and/or jsonlog into log files."},
            {"logical_decoding_work_mem", "65536", "integer", "user", "Resource Usage / Memory", "kB", "64", "2147483647", null, "Sets the maximum memory to be used for logical decoding.", "This much memory can be used by each internal reorder buffer before spilling to disk."},
            {"maintenance_io_concurrency", "16", "integer", "user", "Resource Usage / I/O", null, "0", "1000", null, "A variant of \"effective_io_concurrency\" that is used for maintenance work.", "0 disables simultaneous requests."},
            {"maintenance_work_mem", "65536", "integer", "user", "Resource Usage / Memory", "kB", "64", "2147483647", null, "Sets the maximum memory to be used for maintenance operations.", "This includes operations such as VACUUM and CREATE INDEX."},
            {"max_active_replication_origins", "10", "integer", "postmaster", "Replication / Subscribers", null, "0", "262143", null, "Sets the maximum number of active replication origins."},
            {"max_connections", "100", "integer", "postmaster", "Connections and Authentication / Connection Settings", null, "1", "262143", null, "Sets the maximum number of concurrent connections."},
            {"max_files_per_process", "1000", "integer", "postmaster", "Resource Usage / Kernel Resources", null, "64", "2147483647", null, "Sets the maximum number of files each server process is allowed to open simultaneously."},
            {"max_function_args", "100", "integer", "internal", "Preset Options", null, "100", "100", null, "Shows the maximum number of function arguments."},
            {"max_identifier_length", "63", "integer", "internal", "Preset Options", null, "63", "63", null, "Shows the maximum identifier length."},
            {"max_index_keys", "32", "integer", "internal", "Preset Options", null, "32", "32", null, "Shows the maximum number of index keys."},
            {"max_locks_per_transaction", "64", "integer", "postmaster", "Lock Management", null, "10", "2147483647", null, "Sets the maximum number of locks per transaction.", "The shared lock table is sized on the assumption that at most \"max_locks_per_transaction\" objects per server process or prepared transaction will need to be locked at any one time."},
            {"max_logical_replication_workers", "4", "integer", "postmaster", "Replication / Subscribers", null, "0", "262143", null, "Maximum number of logical replication worker processes."},
            {"max_notify_queue_pages", "1048576", "integer", "postmaster", "Resource Usage / Disk", null, "64", "2147483647", null, "Sets the maximum number of allocated pages for NOTIFY / LISTEN queue."},
            {"max_parallel_apply_workers_per_subscription", "2", "integer", "sighup", "Replication / Subscribers", null, "0", "1024", null, "Maximum number of parallel apply workers per subscription."},
            {"max_parallel_maintenance_workers", "2", "integer", "user", "Resource Usage / Worker Processes", null, "0", "1024", null, "Sets the maximum number of parallel processes per maintenance operation."},
            {"max_parallel_workers", "8", "integer", "user", "Resource Usage / Worker Processes", null, "0", "1024", null, "Sets the maximum number of parallel workers that can be active at one time."},
            {"max_parallel_workers_per_gather", "2", "integer", "user", "Resource Usage / Worker Processes", null, "0", "1024", null, "Sets the maximum number of parallel processes per executor node."},
            {"max_pred_locks_per_page", "2", "integer", "sighup", "Lock Management", null, "0", "2147483647", null, "Sets the maximum number of predicate-locked tuples per page.", "If more than this number of tuples on the same page are locked by a connection, those locks are replaced by a page-level lock."},
            {"max_pred_locks_per_relation", "-2", "integer", "sighup", "Lock Management", null, "-2147483648", "2147483647", null, "Sets the maximum number of predicate-locked pages and tuples per relation.", "If more than this total of pages and tuples in the same relation are locked by a connection, those locks are replaced by a relation-level lock."},
            {"max_pred_locks_per_transaction", "64", "integer", "postmaster", "Lock Management", null, "10", "2147483647", null, "Sets the maximum number of predicate locks per transaction.", "The shared predicate lock table is sized on the assumption that at most \"max_pred_locks_per_transaction\" objects per server process or prepared transaction will need to be locked at any one time."},
            {"max_prepared_transactions", "0", "integer", "postmaster", "Resource Usage / Memory", null, "0", "262143", null, "Sets the maximum number of simultaneously prepared transactions."},
            {"max_replication_slots", "10", "integer", "postmaster", "Replication / Sending Servers", null, "0", "262143", null, "Sets the maximum number of simultaneously defined replication slots."},
            {"max_slot_wal_keep_size", "-1", "integer", "sighup", "Replication / Sending Servers", "MB", "-1", "2147483647", null, "Sets the maximum WAL size that can be reserved by replication slots.", "Replication slots will be marked as failed, and segments released for deletion or recycling, if this much space is occupied by WAL on disk. -1 means no maximum."},
            {"max_stack_depth", "2048", "integer", "superuser", "Resource Usage / Memory", "kB", "100", "2147483647", null, "Sets the maximum stack depth, in kilobytes."},
            {"max_standby_archive_delay", "30000", "integer", "sighup", "Replication / Standby Servers", "ms", "-1", "2147483647", null, "Sets the maximum delay before canceling queries when a hot standby server is processing archived WAL data.", "-1 means wait forever."},
            {"max_standby_streaming_delay", "30000", "integer", "sighup", "Replication / Standby Servers", "ms", "-1", "2147483647", null, "Sets the maximum delay before canceling queries when a hot standby server is processing streamed WAL data.", "-1 means wait forever."},
            {"max_sync_workers_per_subscription", "2", "integer", "sighup", "Replication / Subscribers", null, "0", "262143", null, "Maximum number of table synchronization workers per subscription."},
            {"max_wal_senders", "10", "integer", "postmaster", "Replication / Sending Servers", null, "0", "262143", null, "Sets the maximum number of simultaneously running WAL sender processes."},
            {"max_wal_size", "1024", "integer", "sighup", "Write-Ahead Log / Checkpoints", "MB", "2", "2147483647", null, "Sets the WAL size that triggers a checkpoint."},
            {"max_worker_processes", "8", "integer", "postmaster", "Resource Usage / Worker Processes", null, "0", "262143", null, "Maximum number of concurrent worker processes."},
            {"md5_password_warnings", "on", "bool", "user", "Connections and Authentication / Authentication", null, null, null, null, "Enables deprecation warnings for MD5 passwords."},
            {"min_dynamic_shared_memory", "0", "integer", "postmaster", "Resource Usage / Memory", "MB", "0", "2147483647", null, "Amount of dynamic shared memory reserved at startup."},
            {"min_parallel_index_scan_size", "64", "integer", "user", "Query Tuning / Planner Cost Constants", "8kB", "0", "715827882", null, "Sets the minimum amount of index data for a parallel scan.", "If the planner estimates that it will read a number of index pages too small to reach this limit, a parallel scan will not be considered."},
            {"min_parallel_table_scan_size", "1024", "integer", "user", "Query Tuning / Planner Cost Constants", "8kB", "0", "715827882", null, "Sets the minimum amount of table data for a parallel scan.", "If the planner estimates that it will read a number of table pages too small to reach this limit, a parallel scan will not be considered."},
            {"min_wal_size", "80", "integer", "sighup", "Write-Ahead Log / Checkpoints", "MB", "2", "2147483647", null, "Sets the minimum size to shrink the WAL to."},
            {"multixact_member_buffers", "32", "integer", "postmaster", "Resource Usage / Memory", "8kB", "16", "131072", null, "Sets the size of the dedicated buffer pool used for the MultiXact member cache."},
            {"multixact_offset_buffers", "16", "integer", "postmaster", "Resource Usage / Memory", "8kB", "16", "131072", null, "Sets the size of the dedicated buffer pool used for the MultiXact offset cache."},
            {"notify_buffers", "16", "integer", "postmaster", "Resource Usage / Memory", "8kB", "16", "131072", null, "Sets the size of the dedicated buffer pool used for the LISTEN/NOTIFY message cache."},
            {"num_os_semaphores", "0", "integer", "internal", "Preset Options", null, "0", "2147483647", null, "Shows the number of semaphores required for the server."},
            {"oauth_validator_libraries", "", "string", "sighup", "Connections and Authentication / Authentication", null, null, null, null, "Lists libraries that may be called to validate OAuth v2 bearer tokens."},
            {"parallel_leader_participation", "on", "bool", "user", "Resource Usage / Worker Processes", null, null, null, null, "Controls whether Gather and Gather Merge also run subplans.", "Should gather nodes also run subplans or just gather tuples?"},
            {"parallel_setup_cost", "1000", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of starting up worker processes for parallel query."},
            {"parallel_tuple_cost", "0.1", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of passing each tuple (row) from worker to leader backend."},
            {"password_encryption", "scram-sha-256", "enum", "user", "Connections and Authentication / Authentication", null, null, null, "md5,scram-sha-256", "Chooses the algorithm for encrypting passwords."},
            {"plan_cache_mode", "auto", "enum", "user", "Query Tuning / Other Planner Options", null, null, null, "auto,force_generic_plan,force_custom_plan", "Controls the planner's selection of custom or generic plan.", "Prepared statements can have custom and generic plans, and the planner will attempt to choose which is better.  This can be set to override the default behavior."},
            {"port", "5432", "integer", "postmaster", "Connections and Authentication / Connection Settings", null, "1", "65535", null, "Sets the TCP port the server listens on."},
            {"post_auth_delay", "0", "integer", "backend", "Developer Options", "s", "0", "2147", null, "Sets the amount of time to wait after authentication on connection startup.", "This allows attaching a debugger to the process."},
            {"pre_auth_delay", "0", "integer", "sighup", "Developer Options", "s", "0", "60", null, "Sets the amount of time to wait before authentication on connection startup.", "This allows attaching a debugger to the process."},
            {"primary_conninfo", "", "string", "sighup", "Replication / Standby Servers", null, null, null, null, "Sets the connection string to be used to connect to the sending server."},
            {"primary_slot_name", "", "string", "sighup", "Replication / Standby Servers", null, null, null, null, "Sets the name of the replication slot to use on the sending server."},
            {"quote_all_identifiers", "off", "bool", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "When generating SQL fragments, quote all identifiers."},
            {"random_page_cost", "4", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of a nonsequentially fetched disk page."},
            {"recovery_end_command", "", "string", "sighup", "Write-Ahead Log / Archive Recovery", null, null, null, null, "Sets the shell command that will be executed once at the end of recovery."},
            {"recovery_init_sync_method", "fsync", "enum", "sighup", "Error Handling", null, null, null, "fsync", "Sets the method for synchronizing the data directory before crash recovery."},
            {"recovery_min_apply_delay", "0", "integer", "sighup", "Replication / Standby Servers", "ms", "0", "2147483647", null, "Sets the minimum delay for applying changes during recovery."},
            {"recovery_prefetch", "try", "enum", "sighup", "Write-Ahead Log / Recovery", null, null, null, "off,on,try", "Prefetch referenced blocks during recovery.", "Look ahead in the WAL to find references to uncached data."},
            {"recovery_target", "", "string", "postmaster", "Write-Ahead Log / Recovery Target", null, null, null, null, "Set to \"immediate\" to end recovery as soon as a consistent state is reached."},
            {"recovery_target_action", "pause", "enum", "postmaster", "Write-Ahead Log / Recovery Target", null, null, null, "pause,promote,shutdown", "Sets the action to perform upon reaching the recovery target."},
            {"recovery_target_inclusive", "on", "bool", "postmaster", "Write-Ahead Log / Recovery Target", null, null, null, null, "Sets whether to include or exclude transaction with recovery target."},
            {"recovery_target_lsn", "", "string", "postmaster", "Write-Ahead Log / Recovery Target", null, null, null, null, "Sets the LSN of the write-ahead log location up to which recovery will proceed."},
            {"recovery_target_name", "", "string", "postmaster", "Write-Ahead Log / Recovery Target", null, null, null, null, "Sets the named restore point up to which recovery will proceed."},
            {"recovery_target_time", "", "string", "postmaster", "Write-Ahead Log / Recovery Target", null, null, null, null, "Sets the time stamp up to which recovery will proceed."},
            {"recovery_target_timeline", "latest", "string", "postmaster", "Write-Ahead Log / Recovery Target", null, null, null, null, "Specifies the timeline to recover into."},
            {"recovery_target_xid", "", "string", "postmaster", "Write-Ahead Log / Recovery Target", null, null, null, null, "Sets the transaction ID up to which recovery will proceed."},
            {"recursive_worktable_factor", "10", "real", "user", "Query Tuning / Other Planner Options", null, "0.001", "1e+06", null, "Sets the planner's estimate of the average size of a recursive query's working table."},
            {"remove_temp_files_after_crash", "on", "bool", "sighup", "Developer Options", null, null, null, null, "Remove temporary files after backend crash."},
            {"reserved_connections", "0", "integer", "postmaster", "Connections and Authentication / Connection Settings", null, "0", "262143", null, "Sets the number of connection slots reserved for roles with privileges of pg_use_reserved_connections."},
            {"restart_after_crash", "on", "bool", "sighup", "Error Handling", null, null, null, null, "Reinitialize server after backend crash."},
            {"restore_command", "", "string", "sighup", "Write-Ahead Log / Archive Recovery", null, null, null, null, "Sets the shell command that will be called to retrieve an archived WAL file."},
            {"restrict_nonsystem_relation_kind", "", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Prohibits access to non-system relations of specified kinds."},
            {"row_security", "on", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Enables row security.", "When enabled, row security will be applied to all users."},
            {"scram_iterations", "4096", "integer", "user", "Connections and Authentication / Authentication", null, "1", "2147483647", null, "Sets the iteration count for SCRAM secret generation."},
            {"search_path", "\"$user\", public", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the schema search order for names that are not schema-qualified."},
            {"segment_size", "131072", "integer", "internal", "Preset Options", "8kB", "131072", "131072", null, "Shows the number of pages per disk file."},
            {"send_abort_for_crash", "off", "bool", "sighup", "Developer Options", null, null, null, null, "Send SIGABRT not SIGQUIT to child processes after backend crash."},
            {"send_abort_for_kill", "off", "bool", "sighup", "Developer Options", null, null, null, null, "Send SIGABRT not SIGKILL to stuck child processes."},
            {"seq_page_cost", "1", "real", "user", "Query Tuning / Planner Cost Constants", null, "0", "1.79769e+308", null, "Sets the planner's estimate of the cost of a sequentially fetched disk page."},
            {"serializable_buffers", "32", "integer", "postmaster", "Resource Usage / Memory", "8kB", "16", "131072", null, "Sets the size of the dedicated buffer pool used for the serializable transaction cache."},
            {"server_encoding", "UTF8", "string", "internal", "Preset Options", null, null, null, null, "Shows the server (database) character set encoding."},
            {"server_version", "18.0", "string", "internal", "Preset Options", null, null, null, null, "Shows the server version."},
            {"server_version_num", "180000", "integer", "internal", "Preset Options", null, "180000", "180000", null, "Shows the server version as an integer."},
            {"session_preload_libraries", "", "string", "superuser", "Client Connection Defaults / Shared Library Preloading", null, null, null, null, "Lists shared libraries to preload into each backend."},
            {"session_replication_role", "origin", "enum", "superuser", "Client Connection Defaults / Statement Behavior", null, null, null, "origin,replica,local", "Sets the session's behavior for triggers and rewrite rules."},
            {"shared_buffers", "16384", "integer", "postmaster", "Resource Usage / Memory", "8kB", "16", "1073741823", null, "Sets the number of shared memory buffers used by the server."},
            {"shared_memory_size", "0", "integer", "internal", "Preset Options", "MB", "0", "2147483647", null, "Shows the size of the server's main shared memory area (rounded up to the nearest MB)."},
            {"shared_memory_size_in_huge_pages", "-1", "integer", "internal", "Preset Options", null, "-1", "2147483647", null, "Shows the number of huge pages needed for the main shared memory area.", "-1 means huge pages are not supported."},
            {"shared_memory_type", "windows", "enum", "postmaster", "Resource Usage / Memory", null, null, null, "windows", "Selects the shared memory implementation used for the main shared memory region."},
            {"shared_preload_libraries", "", "string", "postmaster", "Client Connection Defaults / Shared Library Preloading", null, null, null, null, "Lists shared libraries to preload into server."},
            {"ssl", "off", "bool", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Enables SSL connections."},
            {"ssl_ca_file", "", "string", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Location of the SSL certificate authority file."},
            {"ssl_cert_file", "server.crt", "string", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Location of the SSL server certificate file."},
            {"ssl_ciphers", "HIGH:MEDIUM:+3DES:!aNULL", "string", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Sets the list of allowed TLSv1.2 (and lower) ciphers."},
            {"ssl_crl_dir", "", "string", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Location of the SSL certificate revocation list directory."},
            {"ssl_crl_file", "", "string", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Location of the SSL certificate revocation list file."},
            {"ssl_dh_params_file", "", "string", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Location of the SSL DH parameters file.", "An empty string means use compiled-in default parameters."},
            {"ssl_groups", "X25519:prime256v1", "string", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Sets the group(s) to use for Diffie-Hellman key exchange.", "Multiple groups can be specified using a colon-separated list."},
            {"ssl_key_file", "server.key", "string", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Location of the SSL server private key file."},
            {"ssl_library", "", "string", "internal", "Preset Options", null, null, null, null, "Shows the name of the SSL library."},
            {"ssl_max_protocol_version", "", "enum", "sighup", "Connections and Authentication / SSL", null, null, null, ",TLSv1,TLSv1.1,TLSv1.2,TLSv1.3", "Sets the maximum SSL/TLS protocol version to use."},
            {"ssl_min_protocol_version", "TLSv1.2", "enum", "sighup", "Connections and Authentication / SSL", null, null, null, "TLSv1,TLSv1.1,TLSv1.2,TLSv1.3", "Sets the minimum SSL/TLS protocol version to use."},
            {"ssl_passphrase_command", "", "string", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Command to obtain passphrases for SSL.", "An empty string means use the built-in prompting mechanism."},
            {"ssl_passphrase_command_supports_reload", "off", "bool", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Controls whether \"ssl_passphrase_command\" is called during server reload."},
            {"ssl_prefer_server_ciphers", "on", "bool", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Give priority to server ciphersuite order."},
            {"ssl_tls13_ciphers", "", "string", "sighup", "Connections and Authentication / SSL", null, null, null, null, "Sets the list of allowed TLSv1.3 cipher suites.", "An empty string means use the default cipher suites."},
            {"standard_conforming_strings", "on", "bool", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Causes '...' strings to treat backslashes literally."},
            {"statement_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed duration of any statement.", "0 disables the timeout."},
            {"stats_fetch_consistency", "cache", "enum", "user", "Statistics / Cumulative Query and Index Statistics", null, null, null, "none,cache,snapshot", "Sets the consistency of accesses to statistics data."},
            {"subtransaction_buffers", "0", "integer", "postmaster", "Resource Usage / Memory", "8kB", "0", "131072", null, "Sets the size of the dedicated buffer pool used for the subtransaction cache.", "0 means use a fraction of \"shared_buffers\"."},
            {"summarize_wal", "off", "bool", "sighup", "Write-Ahead Log / Summarization", null, null, null, null, "Starts the WAL summarizer process to enable incremental backup."},
            {"superuser_reserved_connections", "3", "integer", "postmaster", "Connections and Authentication / Connection Settings", null, "0", "262143", null, "Sets the number of connection slots reserved for superusers."},
            {"sync_replication_slots", "off", "bool", "sighup", "Replication / Standby Servers", null, null, null, null, "Enables a physical standby to synchronize logical failover replication slots from the primary server."},
            {"synchronize_seqscans", "on", "bool", "user", "Version and Platform Compatibility / Previous PostgreSQL Versions", null, null, null, null, "Enables synchronized sequential scans."},
            {"synchronized_standby_slots", "", "string", "sighup", "Replication / Primary Server", null, null, null, null, "Lists streaming replication standby server replication slot names that logical WAL sender processes will wait for.", "Logical WAL sender processes will send decoded changes to output plugins only after the specified replication slots have confirmed receiving WAL."},
            {"synchronous_commit", "on", "enum", "user", "Write-Ahead Log / Settings", null, null, null, "local,remote_write,remote_apply,on,off", "Sets the current transaction's synchronization level."},
            {"synchronous_standby_names", "", "string", "sighup", "Replication / Primary Server", null, null, null, null, "Number of synchronous standbys and list of names of potential synchronous ones."},
            {"syslog_facility", "none", "enum", "sighup", "Reporting and Logging / Where to Log", null, null, null, "none", "Sets the syslog \"facility\" to be used when syslog enabled."},
            {"syslog_ident", "postgres", "string", "sighup", "Reporting and Logging / Where to Log", null, null, null, null, "Sets the program name used to identify PostgreSQL messages in syslog."},
            {"syslog_sequence_numbers", "on", "bool", "sighup", "Reporting and Logging / Where to Log", null, null, null, null, "Add sequence number to syslog messages to avoid duplicate suppression."},
            {"syslog_split_messages", "on", "bool", "sighup", "Reporting and Logging / Where to Log", null, null, null, null, "Split messages sent to syslog by lines and to fit into 1024 bytes."},
            {"tcp_keepalives_count", "0", "integer", "user", "Connections and Authentication / TCP Settings", null, "0", "2147483647", null, "Maximum number of TCP keepalive retransmits.", "Number of consecutive keepalive retransmits that can be lost before a connection is considered dead. 0 means use the system default."},
            {"tcp_keepalives_idle", "0", "integer", "user", "Connections and Authentication / TCP Settings", "s", "0", "2147483647", null, "Time between issuing TCP keepalives.", "0 means use the system default."},
            {"tcp_keepalives_interval", "0", "integer", "user", "Connections and Authentication / TCP Settings", "s", "0", "2147483647", null, "Time between TCP keepalive retransmits.", "0 means use the system default."},
            {"tcp_user_timeout", "0", "integer", "user", "Connections and Authentication / TCP Settings", "ms", "0", "2147483647", null, "TCP user timeout.", "0 means use the system default."},
            {"temp_buffers", "1024", "integer", "user", "Resource Usage / Memory", "8kB", "100", "1073741823", null, "Sets the maximum number of temporary buffers used by each session."},
            {"temp_file_limit", "-1", "integer", "superuser", "Resource Usage / Disk", "kB", "-1", "2147483647", null, "Limits the total size of all temporary files used by each process.", "-1 means no limit."},
            {"temp_tablespaces", "", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the tablespace(s) to use for temporary tables and sort files.", "An empty string means use the database's default tablespace."},
            {"timezone", "UTC", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Sets the time zone for displaying and interpreting time stamps."},
            {"timezone_abbreviations", "Default", "string", "user", "Client Connection Defaults / Locale and Formatting", null, null, null, null, "Selects a file of time zone abbreviations."},
            {"trace_connection_negotiation", "off", "bool", "postmaster", "Developer Options", null, null, null, null, "Logs details of pre-authentication connection handshake."},
            {"trace_notify", "off", "bool", "user", "Developer Options", null, null, null, null, "Generates debugging output for LISTEN and NOTIFY."},
            {"trace_sort", "off", "bool", "user", "Developer Options", null, null, null, null, "Emit information about resource usage in sorting."},
            {"track_activities", "on", "bool", "superuser", "Statistics / Cumulative Query and Index Statistics", null, null, null, null, "Collects information about executing commands.", "Enables the collection of information on the currently executing command of each session, along with the time at which that command began execution."},
            {"track_activity_query_size", "1024", "integer", "postmaster", "Statistics / Cumulative Query and Index Statistics", "B", "100", "1048576", null, "Sets the size reserved for pg_stat_activity.query, in bytes."},
            {"track_commit_timestamp", "off", "bool", "postmaster", "Replication / Sending Servers", null, null, null, null, "Collects transaction commit time."},
            {"track_cost_delay_timing", "off", "bool", "superuser", "Statistics / Cumulative Query and Index Statistics", null, null, null, null, "Collects timing statistics for cost-based vacuum delay."},
            {"track_counts", "on", "bool", "superuser", "Statistics / Cumulative Query and Index Statistics", null, null, null, null, "Collects statistics on database activity."},
            {"track_functions", "none", "enum", "superuser", "Statistics / Cumulative Query and Index Statistics", null, null, null, "none,pl,all", "Collects function-level statistics on database activity."},
            {"track_io_timing", "off", "bool", "superuser", "Statistics / Cumulative Query and Index Statistics", null, null, null, null, "Collects timing statistics for database I/O activity."},
            {"track_wal_io_timing", "off", "bool", "superuser", "Statistics / Cumulative Query and Index Statistics", null, null, null, null, "Collects timing statistics for WAL I/O activity."},
            {"transaction_buffers", "0", "integer", "postmaster", "Resource Usage / Memory", "8kB", "0", "131072", null, "Sets the size of the dedicated buffer pool used for the transaction status cache.", "0 means use a fraction of \"shared_buffers\"."},
            {"transaction_deferrable", "off", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Whether to defer a read-only serializable transaction until it can be executed with no possible serialization failures."},
            {"transaction_isolation", "read committed", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "serializable,repeatable read,read committed,read uncommitted", "Sets the current transaction's isolation level."},
            {"transaction_read_only", "off", "bool", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the current transaction's read-only status."},
            {"transaction_timeout", "0", "integer", "user", "Client Connection Defaults / Statement Behavior", "ms", "0", "2147483647", null, "Sets the maximum allowed duration of any transaction within a session (not a prepared transaction).", "0 disables the timeout."},
            {"transform_null_equals", "off", "bool", "user", "Version and Platform Compatibility / Other Platforms and Clients", null, null, null, null, "Treats \"expr=NULL\" as \"expr IS NULL\".", "When turned on, expressions of the form expr = NULL (or NULL = expr) are treated as expr IS NULL, that is, they return true if expr evaluates to the null value, and false otherwise. The correct behavior of expr = NULL is to always return null (unknown)."},
            {"unix_socket_directories", "", "string", "postmaster", "Connections and Authentication / Connection Settings", null, null, null, null, "Sets the directories where Unix-domain sockets will be created."},
            {"unix_socket_group", "", "string", "postmaster", "Connections and Authentication / Connection Settings", null, null, null, null, "Sets the owning group of the Unix-domain socket.", "The owning user of the socket is always the user that starts the server. An empty string means use the user's default group."},
            {"unix_socket_permissions", "511", "integer", "postmaster", "Connections and Authentication / Connection Settings", null, "0", "511", null, "Sets the access permissions of the Unix-domain socket.", "Unix-domain sockets use the usual Unix file system permission set. The parameter value is expected to be a numeric mode specification in the form accepted by the chmod and umask system calls. (To use the customary octal format the number must start with a 0 (zero).)"},
            {"update_process_title", "off", "bool", "superuser", "Reporting and Logging / Process Title", null, null, null, null, "Updates the process title to show the active SQL command.", "Enables updating of the process title every time a new SQL command is received by the server."},
            {"vacuum_buffer_usage_limit", "2048", "integer", "user", "Resource Usage / Memory", "kB", "0", "16777216", null, "Sets the buffer pool size for VACUUM, ANALYZE, and autovacuum."},
            {"vacuum_cost_delay", "0", "real", "user", "Vacuuming / Cost-Based Vacuum Delay", "ms", "0", "100", null, "Vacuum cost delay in milliseconds."},
            {"vacuum_cost_limit", "200", "integer", "user", "Vacuuming / Cost-Based Vacuum Delay", null, "1", "10000", null, "Vacuum cost amount available before napping."},
            {"vacuum_cost_page_dirty", "20", "integer", "user", "Vacuuming / Cost-Based Vacuum Delay", null, "0", "10000", null, "Vacuum cost for a page dirtied by vacuum."},
            {"vacuum_cost_page_hit", "1", "integer", "user", "Vacuuming / Cost-Based Vacuum Delay", null, "0", "10000", null, "Vacuum cost for a page found in the buffer cache."},
            {"vacuum_cost_page_miss", "2", "integer", "user", "Vacuuming / Cost-Based Vacuum Delay", null, "0", "10000", null, "Vacuum cost for a page not found in the buffer cache."},
            {"vacuum_failsafe_age", "1600000000", "integer", "user", "Vacuuming / Freezing", null, "0", "2100000000", null, "Age at which VACUUM should trigger failsafe to avoid a wraparound outage."},
            {"vacuum_freeze_min_age", "50000000", "integer", "user", "Vacuuming / Freezing", null, "0", "1000000000", null, "Minimum age at which VACUUM should freeze a table row."},
            {"vacuum_freeze_table_age", "150000000", "integer", "user", "Vacuuming / Freezing", null, "0", "2000000000", null, "Age at which VACUUM should scan whole table to freeze tuples."},
            {"vacuum_max_eager_freeze_failure_rate", "0.03", "real", "user", "Vacuuming / Freezing", null, "0", "1", null, "Fraction of pages in a relation vacuum can scan and fail to freeze before disabling eager scanning.", "A value of 0.0 disables eager scanning and a value of 1.0 will eagerly scan up to 100 percent of the all-visible pages in the relation. If vacuum successfully freezes these pages, the cap is lower than 100 percent, because the goal is to amortize page freezing across multiple vacuums."},
            {"vacuum_multixact_failsafe_age", "1600000000", "integer", "user", "Vacuuming / Freezing", null, "0", "2100000000", null, "Multixact age at which VACUUM should trigger failsafe to avoid a wraparound outage."},
            {"vacuum_multixact_freeze_min_age", "5000000", "integer", "user", "Vacuuming / Freezing", null, "0", "1000000000", null, "Minimum age at which VACUUM should freeze a MultiXactId in a table row."},
            {"vacuum_multixact_freeze_table_age", "150000000", "integer", "user", "Vacuuming / Freezing", null, "0", "2000000000", null, "Multixact age at which VACUUM should scan whole table to freeze tuples."},
            {"vacuum_truncate", "on", "bool", "user", "Vacuuming / Default Behavior", null, null, null, null, "Enables vacuum to truncate empty pages at the end of the table."},
            {"wal_block_size", "8192", "integer", "internal", "Preset Options", null, "8192", "8192", null, "Shows the block size in the write ahead log."},
            {"wal_buffers", "512", "integer", "postmaster", "Write-Ahead Log / Settings", "8kB", "-1", "262143", null, "Sets the number of disk-page buffers in shared memory for WAL.", "-1 means use a fraction of \"shared_buffers\"."},
            {"wal_compression", "off", "enum", "superuser", "Write-Ahead Log / Settings", null, null, null, "pglz,lz4,zstd,on,off", "Compresses full-page writes written in WAL file with specified method."},
            {"wal_consistency_checking", "", "string", "superuser", "Developer Options", null, null, null, null, "Sets the WAL resource managers for which WAL consistency checks are done.", "Full-page images will be logged for all data blocks and cross-checked against the results of WAL replay."},
            {"wal_decode_buffer_size", "524288", "integer", "postmaster", "Write-Ahead Log / Recovery", "B", "65536", "1073741823", null, "Buffer size for reading ahead in the WAL during recovery.", "Maximum distance to read ahead in the WAL to prefetch referenced data blocks."},
            {"wal_init_zero", "on", "bool", "superuser", "Write-Ahead Log / Settings", null, null, null, null, "Writes zeroes to new WAL files before first use."},
            {"wal_keep_size", "0", "integer", "sighup", "Replication / Sending Servers", "MB", "0", "2147483647", null, "Sets the size of WAL files held for standby servers."},
            {"wal_level", "replica", "enum", "postmaster", "Write-Ahead Log / Settings", null, null, null, "minimal,replica,logical", "Sets the level of information written to the WAL."},
            {"wal_log_hints", "off", "bool", "postmaster", "Write-Ahead Log / Settings", null, null, null, null, "Writes full pages to WAL when first modified after a checkpoint, even for a non-critical modification."},
            {"wal_receiver_create_temp_slot", "off", "bool", "sighup", "Replication / Standby Servers", null, null, null, null, "Sets whether a WAL receiver should create a temporary replication slot if no permanent slot is configured."},
            {"wal_receiver_status_interval", "10", "integer", "sighup", "Replication / Standby Servers", "s", "0", "2147483", null, "Sets the maximum interval between WAL receiver status reports to the sending server."},
            {"wal_receiver_timeout", "60000", "integer", "sighup", "Replication / Standby Servers", "ms", "0", "2147483647", null, "Sets the maximum wait time to receive data from the sending server.", "0 disables the timeout."},
            {"wal_recycle", "on", "bool", "superuser", "Write-Ahead Log / Settings", null, null, null, null, "Recycles WAL files by renaming them."},
            {"wal_retrieve_retry_interval", "5000", "integer", "sighup", "Replication / Standby Servers", "ms", "1", "2147483647", null, "Sets the time to wait before retrying to retrieve WAL after a failed attempt."},
            {"wal_segment_size", "16777216", "integer", "internal", "Preset Options", "B", "1048576", "1073741824", null, "Shows the size of write ahead log segments."},
            {"wal_sender_timeout", "60000", "integer", "user", "Replication / Sending Servers", "ms", "0", "2147483647", null, "Sets the maximum time to wait for WAL replication."},
            {"wal_skip_threshold", "2048", "integer", "user", "Write-Ahead Log / Settings", "kB", "0", "2147483647", null, "Minimum size of new file to fsync instead of writing WAL."},
            {"wal_summary_keep_time", "14400", "integer", "sighup", "Write-Ahead Log / Summarization", "min", "0", "35791394", null, "Time for which WAL summary files should be kept.", "0 disables automatic summary file deletion."},
            {"wal_sync_method", "open_datasync", "enum", "sighup", "Write-Ahead Log / Settings", null, null, null, "fsync,fdatasync,open_datasync", "Selects the method used for forcing WAL updates to disk."},
            {"wal_writer_delay", "200", "integer", "sighup", "Write-Ahead Log / Settings", "ms", "1", "10000", null, "Time between WAL flushes performed in the WAL writer."},
            {"wal_writer_flush_after", "128", "integer", "sighup", "Write-Ahead Log / Settings", "8kB", "0", "2147483647", null, "Amount of WAL written out by WAL writer that triggers a flush."},
            {"work_mem", "4096", "integer", "user", "Resource Usage / Memory", "kB", "64", "2147483647", null, "Sets the maximum memory to be used for query workspaces.", "This much memory can be used by each internal sort operation and hash table before switching to temporary disk files."},
            {"xmlbinary", "base64", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "base64,hex", "Sets how binary values are to be encoded in XML."},
            {"xmloption", "content", "enum", "user", "Client Connection Defaults / Statement Behavior", null, null, null, "content,document", "Sets whether XML data in implicit parsing and serialization operations is to be considered as documents or content fragments."},
            {"zero_damaged_pages", "off", "bool", "superuser", "Developer Options", null, null, null, null, "Continues processing past damaged page headers.", "Detection of a damaged page header normally causes PostgreSQL to report an error, aborting the current transaction. Setting \"zero_damaged_pages\" to true causes the system to instead report a warning, zero out the damaged page, and continue processing. This behavior will destroy data, namely all the rows on the damaged page."},
    };

    /**
     * Parameters PostgreSQL compiles no default into, and reports a null {@code boot_val} for.
     *
     * <p>They name files a running server was started with, so there is nothing for a build to
     * have decided in advance. memgres has no such files and says so with an empty value; what
     * it must not do is claim the empty string was compiled in, because a client reads
     * {@code boot_val} to tell a configured value from a built-in one.
     */
    private static final Set<String> NO_COMPILED_DEFAULT = new HashSet<>(Arrays.asList(
            "config_file", "data_directory", "external_pid_file", "hba_file", "ident_file",
            "timezone_abbreviations"));

    /**
     * Parameters PostgreSQL marks {@code GUC_NO_SHOW_ALL}: {@code SHOW} and
     * {@code current_setting} answer for them, but they are deliberately absent from
     * {@code pg_settings} and from {@code SHOW ALL}.
     */
    private static final String[][] HIDDEN_DEFS = {
            {"is_superuser", "on", "bool", "internal", "Preset Options", null, null, null, null, "Shows whether the current user is a superuser."},
            // A session that has not SET ROLE is running as nobody in particular, and
            // PostgreSQL says "none" rather than naming the user it connected as.
            {"role", "none", "string", "user", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the current role."},
            {"session_authorization", "test", "string", "superuser", "Client Connection Defaults / Statement Behavior", null, null, null, null, "Sets the session user name."},
            // The seed the random generator was last given. PostgreSQL keeps it out of
            // pg_settings, and SHOW answers "unavailable" because there is nothing to read back.
            {"seed", "unavailable", "real", "user", "Client Connection Defaults / Statement Behavior", null, "-1", "1", null, "Sets the seed for random-number generation."},
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
            {"default_text_search_config", "pg_catalog.simple"},
            {"log_timezone", "GMT"},
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
            this.bootVal = NO_COMPILED_DEFAULT.contains(row[0]) ? null
                    : (bootValOverride != null ? bootValOverride : row[1]);
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
        return name == null ? null : DEFINITIONS.get(name.toLowerCase(java.util.Locale.ROOT));
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
    /** The spelling each custom parameter was first written with. */
    private final Map<String, String> customSpellings = new LinkedHashMap<>();

    /** Set a session-level parameter. */
    public void set(String name, String value) {
        // Normalize boolean-like values to lowercase (PG convention).
        // The parser uppercases keywords like ON/OFF/TRUE/FALSE/YES/NO,
        // but JDBC drivers expect lowercase for ParameterStatus messages.
        String normalized = value;
        if (normalized != null) {
            String upper = normalized.trim().toUpperCase(java.util.Locale.ROOT);
            if ("ON".equals(upper) || "OFF".equals(upper)
                    || "TRUE".equals(upper) || "FALSE".equals(upper)
                    || "YES".equals(upper) || "NO".equals(upper)) {
                normalized = normalized.trim().toLowerCase(java.util.Locale.ROOT);
            }
        }
        String key = name.toLowerCase(java.util.Locale.ROOT);
        // A text search configuration is recorded by its qualified name, whichever way it was
        // written: a configuration set as "english" reads back as "pg_catalog.english".
        if ("default_text_search_config".equals(key) && normalized != null
                && normalized.indexOf('.') < 0) {
            normalized = "pg_catalog." + normalized.trim();
        }
        // L7: remember custom (dotted) parameters so RESET keeps them as an empty placeholder.
        if (key.indexOf('.') >= 0) {
            customPlaceholders.add(key);
            // A parameter the reader made up is known by the spelling that made it, and
            // PostgreSQL answers with that spelling however it is asked for afterwards.
            if (!customSpellings.containsKey(key)) customSpellings.put(key, name);
        }
        sessionOverrides.put(key, toBaseUnit(key, canonicalValue(key, normalized)));
        // A plain SET inside a transaction is the value for the rest of that transaction too.
        // Leaving an earlier SET LOCAL in place meant the later, plainer instruction was read
        // as though it had not been given.
        transactionOverrides.remove(key);
    }

    /** Set a transaction-scoped (LOCAL) parameter that reverts on commit/rollback. */
    public void setLocal(String name, String value) {
        String key = name.toLowerCase(java.util.Locale.ROOT);
        transactionOverrides.put(key, toBaseUnit(key, canonicalValue(key, value)));
    }

    /** Clear all transaction-scoped overrides (called on commit/rollback). */
    public void clearTransactionOverrides() {
        transactionOverrides.clear();
    }

    /** Snapshot both override layers so ROLLBACK TO SAVEPOINT can undo SET / SET LOCAL. */
    /**
     * The settings that can change a plan, and that this session has moved off their default.
     * EXPLAIN (SETTINGS) names exactly these — not every parameter that happens to differ, which
     * would list whatever the client driver set on connecting.
     */
    private static final java.util.Set<String> PLAN_AFFECTING = new java.util.LinkedHashSet<>(
            java.util.Arrays.asList(
                    "search_path", "work_mem", "hash_mem_multiplier", "maintenance_work_mem",
                    "effective_cache_size", "random_page_cost", "seq_page_cost", "cpu_tuple_cost",
                    "cpu_index_tuple_cost", "cpu_operator_cost", "parallel_tuple_cost",
                    "parallel_setup_cost", "min_parallel_table_scan_size",
                    "min_parallel_index_scan_size", "effective_io_concurrency",
                    "enable_seqscan", "enable_indexscan", "enable_indexonlyscan", "enable_bitmapscan",
                    "enable_tidscan", "enable_sort", "enable_incremental_sort", "enable_hashagg",
                    "enable_material", "enable_memoize", "enable_nestloop", "enable_mergejoin",
                    "enable_hashjoin", "enable_gathermerge", "enable_partitionwise_join",
                    "enable_partitionwise_aggregate", "enable_parallel_append",
                    "enable_parallel_hash", "enable_partition_pruning", "enable_presorted_aggregate",
                    "enable_async_append", "enable_group_by_reordering",
                    "geqo", "geqo_threshold", "from_collapse_limit", "join_collapse_limit",
                    "constraint_exclusion", "cursor_tuple_fraction", "default_statistics_target",
                    "jit", "plan_cache_mode", "recursive_worktable_factor",
                    "max_parallel_workers_per_gather", "temp_buffers"));

    /**
     * The plan-affecting settings this session has moved off their built-in default, in the order
     * PostgreSQL lists them.
     */
    /** Settings measured in memory or time carry their unit when they are shown to a reader. */
    private static final java.util.Set<String> MEASURED_IN_KB = new java.util.HashSet<>(
            java.util.Arrays.asList("work_mem", "maintenance_work_mem", "effective_cache_size",
                    "temp_buffers", "min_parallel_table_scan_size", "min_parallel_index_scan_size"));

    /** The value as EXPLAIN prints it: with the unit, for the settings that are measured in one. */
    public String getWithUnit(String key) {
        String value = get(key);
        if (value == null) return null;
        if (MEASURED_IN_KB.contains(key) && value.matches("[0-9]+")) return value + "kB";
        return value;
    }

    public java.util.List<String> changedFromDefault() {
        java.util.List<String> names = new java.util.ArrayList<>();
        for (String key : PLAN_AFFECTING) {
            String value = sessionOverrides.get(key);
            if (value == null || value.isEmpty()) continue;
            String base = bootDefaults.containsKey(key) ? bootDefaults.get(key) : DEFAULTS.get(key);
            if (base == null || !base.equals(value)) names.add(key);
        }
        return names;
    }

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
        String key = name.toLowerCase(java.util.Locale.ROOT);
        // L7: a custom placeholder stays defined with an empty value after RESET (PG behavior).
        if (customPlaceholders.contains(key)) {
            sessionOverrides.put(key, "");
        } else {
            sessionOverrides.remove(key);
        }
        // RESET puts the parameter back to its default now, so a SET LOCAL still standing over it
        // is gone as well; otherwise the reset value could not be read until the transaction ended.
        transactionOverrides.remove(key);
    }

    /** Reset all session parameters. */
    public void resetAll() {
        sessionOverrides.clear();
        transactionOverrides.clear();
        // A custom parameter that has been set once exists for the rest of the session: RESET
        // returns it to its empty default, it does not make the session forget it was ever
        // named. Clearing the placeholders made current_setting(x, true) answer NULL where
        // PostgreSQL answers the empty string.
        for (String custom : customPlaceholders) {
            sessionOverrides.put(custom, "");
        }
    }

    /** Set a boot-time default that overrides the static default (e.g., for session_authorization). */
    public void setBootDefault(String name, String value) {
        bootDefaults.put(name.toLowerCase(java.util.Locale.ROOT), value);
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
        String key = name.toLowerCase(java.util.Locale.ROOT);
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
        String key = name.toLowerCase(java.util.Locale.ROOT);
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
        // The seed is given to the random generator and not kept: there is nothing to read back,
        // and PostgreSQL says so rather than echoing what it was last handed.
        if ("seed".equalsIgnoreCase(name)) return "unavailable";
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
            return val.toLowerCase(java.util.Locale.ROOT);
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
        // A parameter counted in whole units holds a whole number of them: 2500us is two and a
        // half milliseconds, and lock_timeout is a count of milliseconds, so it is two. Keeping
        // the fraction reported a setting in a unit the parameter is not measured in.
        if ("integer".equals(def.vartype)) {
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
                throw new MemgresException("invalid value for parameter \"" + reportedName(def)
                        + "\": \"" + value + "\""
                        + "\n  Hint: Available values: " + enumHint(def) + ".", "22023");
            }
            return;
        }
        if ("integer".equals(def.vartype) || "real".equals(def.vartype)) {
            Double n = numericInBaseUnit(def, value);
            if (n == null) {
                throw new MemgresException("invalid value for parameter \"" + reportedName(def)
                        + "\": \"" + value + "\"", "22023");
            }
            // An integer parameter is held in a machine integer, so a value too wide for one never
            // becomes a number the parameter's own bounds could be applied to: it is refused as a
            // value that could not be read, and the range it missed is a different complaint.
            if ("integer".equals(def.vartype)
                    && (n.doubleValue() > Integer.MAX_VALUE || n.doubleValue() < Integer.MIN_VALUE)) {
                MemgresException e = new MemgresException("invalid value for parameter \""
                        + reportedName(def) + "\": \"" + value + "\"", "22023");
                e.setHint("Value exceeds integer range.");
                throw e;
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
        // A parameter counted in whole units is held to a whole number of them: PostgreSQL
        // reports the count the value came to after it was cut to the unit, not before.
        double reported = "integer".equals(def.vartype) ? (long) n : n;
        throw new MemgresException(trimNumber(reported) + unit
                + " is outside the valid range for parameter \"" + reportedName(def) + "\" ("
                + def.minVal + unit + " .. " + def.maxVal + unit + ")", "22023");
    }

    /** The value as pg_settings would store it, or null when it is not a number at all. */
    private static Double numericInBaseUnit(Def def, String value) {
        String text = unquote(value);
        if (text == null) return null;
        text = text.trim();
        int i = 0;
        while (i < text.length() && (Character.isDigit(text.charAt(i)) || text.charAt(i) == '-'
                || text.charAt(i) == '+' || text.charAt(i) == '.'
                // A number may be written with an exponent, and the sign after the e belongs
                // to the exponent rather than starting a unit.
                || text.charAt(i) == 'e' || text.charAt(i) == 'E')) {
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
        // A parameter counted in whole units holds a whole number of them, so a value that came
        // to a fraction of one is reported as the number it is: a Java double written out in
        // exponent notation is not a count of anything.
        java.math.BigDecimal exact = new java.math.BigDecimal(n);
        return exact.setScale(6, java.math.RoundingMode.HALF_UP).stripTrailingZeros()
                .toPlainString();
    }

    /** The boolean a written value stands for, or null when it stands for neither. */
    static Boolean parseBool(String value) {
        String v = unquote(value);
        if (v == null) return null;
        v = v.trim().toLowerCase(java.util.Locale.ROOT);
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
        // Some levels the server knows are not levels it lists: a message level PostgreSQL
        // will not send to a client is still one a client may ask for, and "debug" is the
        // spelling it keeps for the level it calls debug2.
        String hidden = HIDDEN_ENUM_VALUES.get(
                def.name.toLowerCase(java.util.Locale.ROOT) + ":" + v.toLowerCase(java.util.Locale.ROOT));
        return hidden;
    }

    /** The values an enum parameter takes that its own list does not name. */
    private static final Map<String, String> HIDDEN_ENUM_VALUES;

    static {
        Map<String, String> hidden = new LinkedHashMap<>();
        hidden.put("client_min_messages:info", "info");
        hidden.put("client_min_messages:debug", "debug2");
        hidden.put("log_min_messages:info", "info");
        hidden.put("log_min_messages:debug", "debug2");
        HIDDEN_ENUM_VALUES = hidden;
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
        // A boolean parameter is on or off however it was written. PostgreSQL stores the value
        // its own way and every reader of it — SHOW, current_setting, pg_settings, the parameter
        // status the wire reports — answers with that; keeping the spelling meant "0" read back
        // as "0" and "yes" as "yes".
        if ("bool".equals(def.vartype)) {
            Boolean asked = booleanWord(value);
            if (asked != null) return asked.booleanValue() ? "on" : "off";
        }
        // A zone written as a number of hours or as an interval names a fixed displacement, and
        // PostgreSQL writes it back in the zone database's own notation for one.
        if ("timezone".equals(name.toLowerCase(java.util.Locale.ROOT))) {
            String fixed = fixedZoneText(value);
            if (fixed != null) return fixed;
        }
        if ("enum".equals(def.vartype)) {
            String match = enumMatch(def, value);
            if (match != null) return match;
        }
        // A parameter measured in whole units holds a whole number. Keeping the fraction that was
        // written meant default_statistics_target read back as 100.7, which is not a count of
        // anything; PostgreSQL rounds it to the unit the parameter is in.
        if ("integer".equals(def.vartype)) {
            String rounded = roundedToWholeUnits(value);
            if (rounded != null) return rounded;
        }
        return value;
    }

    /**
     * A zone named as a displacement rather than as a place, written the way the zone database
     * writes one: the name it goes by between angle brackets, then the POSIX offset, which counts
     * the other way round. Answers null when the value names a place after all.
     */
    private static String fixedZoneText(String value) {
        Integer read = fixedZoneMinutes(value);
        if (read == null) return null;
        int minutes = read.intValue();
        String named = offsetText(minutes);
        return "<" + named + ">" + offsetText(-minutes);
    }

    /**
     * Whether a fixed displacement counts more than the zone database holds. A zone offset is
     * less than a week either way; anything further is out of range rather than a place.
     */
    static boolean zoneOffsetOutOfRange(String value) {
        Integer minutes = fixedZoneMinutes(value);
        return minutes != null && Math.abs(minutes) >= 168 * 60;
    }

    /** The minutes a fixed-displacement zone counts, or null when the value names a place. */
    private static Integer fixedZoneMinutes(String value) {
        if (value.startsWith("HOURS:")) {
            try {
                return (int) Math.round(Double.parseDouble(value.substring(6)) * 60);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        if (value.startsWith("INTERVAL:")) {
            int minutes = intervalMinutes(value.substring(9));
            return minutes == Integer.MIN_VALUE ? null : Integer.valueOf(minutes);
        }
        return null;
    }

    /** A displacement in minutes written ±HH or ±HH:MM. */
    private static String offsetText(int minutes) {
        int abs = Math.abs(minutes);
        String sign = minutes < 0 ? "-" : "+";
        return abs % 60 == 0
                ? String.format("%s%02d", sign, abs / 60)
                : String.format("%s%02d:%02d", sign, abs / 60, abs % 60);
    }

    /** The minutes an interval written after SET TIME ZONE counts, or MIN_VALUE when it is none. */
    private static int intervalMinutes(String written) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                "\\s*(-?\\d+)(?::(\\d{2}))?\\s*(HOUR|HOURS|MINUTE|MINUTES)?\\s*",
                java.util.regex.Pattern.CASE_INSENSITIVE).matcher(written);
        if (!m.matches()) return Integer.MIN_VALUE;
        int first = Integer.parseInt(m.group(1));
        String unit = m.group(3) == null ? "HOUR" : m.group(3).toUpperCase(java.util.Locale.ROOT);
        if (unit.startsWith("MINUTE")) return first;
        int minutes = first * 60;
        if (m.group(2) != null) {
            minutes += (first < 0 ? -1 : 1) * Integer.parseInt(m.group(2));
        }
        return minutes;
    }

    /**
     * The truth a word stands for, or null when the word is not one PostgreSQL reads as a
     * boolean. A prefix of "true", "false", "on", "off", "yes" or "no" is one, which is how a
     * bare "t" and a bare "n" are read.
     */
    static Boolean booleanWord(String value) {
        String text = unquote(value);
        if (text == null) return null;
        text = text.trim().toLowerCase(java.util.Locale.ROOT);
        if (text.isEmpty()) return null;
        if ("1".equals(text)) return Boolean.TRUE;
        if ("0".equals(text)) return Boolean.FALSE;
        if ("true".startsWith(text) || "yes".startsWith(text)) return Boolean.TRUE;
        if ("false".startsWith(text) || "no".startsWith(text)) return Boolean.FALSE;
        // "on" and "off" share a prefix, so a bare "o" is neither.
        if ("on".equals(text)) return Boolean.TRUE;
        if ("off".startsWith(text) && text.length() >= 2) return Boolean.FALSE;
        return null;
    }

    /**
     * The name PostgreSQL writes a parameter by. Three of them are spelled with capitals —
     * DateStyle, IntervalStyle and TimeZone — and PostgreSQL uses that spelling in what it says
     * about them whichever spelling the statement used.
     */
    private static String reportedName(Def def) {
        String canonical = CANONICAL_NAMES.get(def.name.toLowerCase(java.util.Locale.ROOT));
        return canonical != null ? canonical : def.name;
    }

    /** A written value rounded to a whole number, or null when it is not a bare number. */
    private static String roundedToWholeUnits(String value) {
        String text = unquote(value);
        if (text == null) return null;
        text = text.trim();
        if (text.isEmpty() || text.indexOf('.') < 0) return null;
        try {
            return String.valueOf(Math.round(Double.parseDouble(text)));
        } catch (NumberFormatException e) {
            return null;   // a number with a unit after it, which toBaseUnit reads
        }
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
        return sessionOverrides.containsKey(name.toLowerCase(java.util.Locale.ROOT)) || transactionOverrides.containsKey(name.toLowerCase(java.util.Locale.ROOT));
    }

    /** Check if a parameter name is known (either in defaults or session overrides). */
    public boolean isKnown(String name) {
        String key = name.toLowerCase(java.util.Locale.ROOT);
        return DEFAULTS.containsKey(key) || sessionOverrides.containsKey(key);
    }

    /** The spelling PostgreSQL writes a parameter with, for a reader with no session in hand. */
    public static String canonicalNameOf(String name) {
        String canonical = CANONICAL_NAMES.get(name.toLowerCase(java.util.Locale.ROOT));
        return canonical != null ? canonical : name.toLowerCase(java.util.Locale.ROOT);
    }

    /** Get the canonical (display) name for a parameter, preserving PG's mixed-case conventions. */
    public String getCanonicalName(String name) {
        String canonical = CANONICAL_NAMES.get(name.toLowerCase(java.util.Locale.ROOT));
        if (canonical != null) return canonical;
        String spelled = customSpellings.get(name.toLowerCase(java.util.Locale.ROOT));
        return spelled != null ? spelled : name.toLowerCase(java.util.Locale.ROOT);
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
        String key = name.toLowerCase(java.util.Locale.ROOT);
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
        value = value.toLowerCase(java.util.Locale.ROOT);
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
