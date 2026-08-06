-- ============================================================================
-- Feature Comparison: what the catalogs say the server holds
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- pg_settings carried 226 of PostgreSQL's parameters, so a tool that reads the
-- configuration saw a server missing 171 of them -- and none at all with a
-- backend context, which is a class of parameter rather than a few names.
--
-- hstore had no array type: typarray was 0, hstore[] did not resolve, and a
-- column declared with it stored something that did not read back. Every other
-- base type has one, so the gap showed as an inconsistency rather than as a
-- missing feature: jsonb[] worked and hstore[] did not.
--
-- The count of parameters is not compared here. It depends on how PostgreSQL
-- was built, the same way io_method lists io_uring on some builds and not on
-- others; what is compared is that a named parameter is there and says the
-- right things about itself.
-- ============================================================================

SET search_path = public;

SET TimeZone = 'UTC';

-- ============================================================================
-- Every parameter, and what each says about itself
-- ============================================================================

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT (count(*) >= 390)::text AS r FROM pg_settings;

-- begin-expected
-- columns: r
-- row: backend,internal,postmaster,sighup,superuser,superuser-backend,user
-- end-expected
SELECT string_agg(DISTINCT context, ',' ORDER BY context) AS r FROM pg_settings;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT (count(*) > 0)::text AS r FROM pg_settings WHERE context = 'backend';

-- begin-expected
-- columns: r
-- row: enum|user|Client Connection Defaults / Statement Behavior
-- end-expected
SELECT vartype || '|' || context || '|' || category AS r FROM pg_settings WHERE name = 'bytea_output';

-- begin-expected
-- columns: r
-- row: escape,hex
-- end-expected
SELECT array_to_string(enumvals, ',') AS r FROM pg_settings WHERE name = 'bytea_output';

-- begin-expected
-- columns: r
-- row: integer|postmaster|8kB|16|1073741823
-- end-expected
SELECT vartype || '|' || context || '|' || unit || '|' || min_val || '|' || max_val AS r
  FROM pg_settings WHERE name = 'shared_buffers';

-- begin-expected
-- columns: r
-- row: Sets the maximum number of concurrent connections.
-- end-expected
SELECT short_desc AS r FROM pg_settings WHERE name = 'max_connections';

-- begin-expected
-- columns: r
-- row: 0 disables the timeout.
-- end-expected
SELECT extra_desc AS r FROM pg_settings WHERE name = 'transaction_timeout';

-- begin-expected
-- columns: r
-- row: -1 means no limit.
-- end-expected
SELECT extra_desc AS r FROM pg_settings WHERE name = 'temp_file_limit';

-- begin-expected
-- columns: r
-- row: Version and Platform Compatibility / Other Platforms and Clients
-- end-expected
SELECT category AS r FROM pg_settings WHERE name = 'allow_alter_system';

-- begin-expected
-- columns: r
-- row: bool|sighup
-- end-expected
SELECT vartype || '|' || context AS r FROM pg_settings WHERE name = 'allow_alter_system';

-- begin-expected
-- columns: r
-- row: on
-- end-expected
SELECT setting AS r FROM pg_settings WHERE name = 'allow_alter_system';

-- begin-expected
-- columns: r
-- row: on
-- end-expected
SELECT current_setting('allow_alter_system') AS r;

-- begin-expected
-- columns: r
-- row: 
-- end-expected
SELECT setting AS r FROM pg_settings WHERE name = 'backtrace_functions';

-- begin-expected
-- columns: r
-- row: enum|user|Query Tuning / Other Planner Options
-- end-expected
SELECT vartype || '|' || context || '|' || category AS r FROM pg_settings WHERE name = 'plan_cache_mode';

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT boot_val IS NULL AS r FROM pg_settings WHERE name = 'data_directory';

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT boot_val IS NULL AS r FROM pg_settings WHERE name = 'timezone_abbreviations';

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM pg_settings WHERE short_desc IS NULL OR short_desc = '';

-- begin-expected
-- columns: r
-- row: 12
-- end-expected
SELECT count(*)::text AS r FROM pg_settings WHERE name IN
  ('allow_alter_system', 'backtrace_functions', 'plan_cache_mode', 'transaction_timeout',
   'temp_file_limit', 'track_activities', 'max_pred_locks_per_transaction', 'log_directory',
   'ssl_ca_file', 'krb_server_keyfile', 'max_identifier_length', 'max_function_args');

-- ============================================================================
-- What an array literal quotes
-- ============================================================================

-- begin-expected
-- columns: r
-- row: {"2020-01-01 10:00:00"}
-- end-expected
SELECT (ARRAY[timestamp '2020-01-01 10:00:00'])::text AS r;

-- begin-expected
-- columns: r
-- row: {"1 day 02:00:00"}
-- end-expected
SELECT (ARRAY[interval '1 day 2 hours'])::text AS r;

-- begin-expected
-- columns: r
-- row: {"'a' 'b'"}
-- end-expected
SELECT (ARRAY['a b'::tsvector])::text AS r;

-- begin-expected
-- columns: r
-- row: {1,2}
-- end-expected
SELECT (ARRAY[1,2])::text AS r;

-- begin-expected
-- columns: r
-- row: {2020-01-01}
-- end-expected
SELECT (ARRAY[date '2020-01-01'])::text AS r;

-- begin-expected
-- columns: r
-- row: {1.50}
-- end-expected
SELECT (ARRAY[1.50::numeric])::text AS r;

-- begin-expected
-- columns: r
-- row: {"a b"}
-- end-expected
SELECT (ARRAY['a b'])::text AS r;

-- begin-expected
-- columns: r
-- row: {plain}
-- end-expected
SELECT (ARRAY['plain'])::text AS r;

-- begin-expected
-- columns: r
-- row: {"(1,2)"}
-- end-expected
SELECT (ARRAY[point '(1,2)'])::text AS r;

-- begin-expected
-- columns: r
-- row: {"[1,5)"}
-- end-expected
SELECT (ARRAY[int4range(1,5)])::text AS r;

-- begin-expected
-- columns: r
-- row: {00000000-0000-0000-0000-000000000001}
-- end-expected
SELECT (ARRAY['00000000-0000-0000-0000-000000000001'::uuid])::text AS r;

-- begin-expected
-- columns: r
-- row: 2020-01-01 10:00:00
-- end-expected
SELECT ((ARRAY[timestamp '2020-01-01 10:00:00'])::text::timestamp[])[1]::text AS r;

-- begin-expected
-- columns: r
-- row: 'a' 'b'
-- end-expected
SELECT ((ARRAY['a b'::tsvector])::text::tsvector[])[1]::text AS r;

