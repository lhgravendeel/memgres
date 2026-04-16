-- ============================================================================
-- Feature Comparison: Round 14 — Filesystem & server-side admin functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION A: File read helpers
-- ============================================================================

-- 1. pg_ls_dir resolves (any count is fine)
SELECT count(*)::int AS c FROM pg_ls_dir('base');

-- 2. pg_stat_file returns record
SELECT (pg_stat_file('postgresql.conf')).size;

-- 3. pg_read_file with (path, offset, length)
SELECT pg_read_file('postgresql.conf', 0, 0);

-- 4. pg_read_binary_file
SELECT pg_read_binary_file('postgresql.conf', 0, 0);

-- ============================================================================
-- SECTION B: Directory listings
-- ============================================================================

-- 5. pg_ls_logdir
SELECT count(*)::int AS c FROM pg_ls_logdir();

-- 6. pg_ls_waldir
SELECT count(*)::int AS c FROM pg_ls_waldir();

-- 7. pg_ls_tmpdir
SELECT count(*)::int AS c FROM pg_ls_tmpdir();

-- 8. pg_ls_archive_statusdir
SELECT count(*)::int AS c FROM pg_ls_archive_statusdir();

-- ============================================================================
-- SECTION C: Current log / settings helpers
-- ============================================================================

-- 9. pg_current_logfile
SELECT pg_current_logfile();

-- 10. current_setting with missing_ok=true on unknown GUC returns NULL
-- begin-expected
-- columns: v
-- row:
-- end-expected
SELECT current_setting('nonexistent.guc', true) AS v;

-- 11. current_setting without missing_ok on unknown GUC errors
-- begin-expected-error
-- message-like: unrecognized
-- end-expected-error
SELECT current_setting('nonexistent.guc');

-- 12. set_config round-trip
-- begin-expected
-- columns: v
-- row: hello
-- end-expected
SELECT set_config('custom.foo', 'hello', false) AS v;

-- 13. current_setting reflects set_config
-- begin-expected
-- columns: v
-- row: hello
-- end-expected
SELECT current_setting('custom.foo') AS v;

-- ============================================================================
-- SECTION D: Size helpers
-- ============================================================================

-- 14. pg_database_size returns non-negative
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (pg_database_size(current_database()) >= 0)::text AS ok;

CREATE TABLE r14_fs_size (id int);

-- 15. pg_total_relation_size
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (pg_total_relation_size('r14_fs_size') >= 0)::text AS ok;

-- 16. pg_relation_size with fork name
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (pg_relation_size('r14_fs_size', 'main') >= 0)::text AS ok;

-- 17. pg_size_bytes inverse of pg_size_pretty
-- begin-expected
-- columns: b
-- row: 1048576
-- end-expected
SELECT pg_size_bytes('1 MB')::text AS b;

-- ============================================================================
-- SECTION E: Runtime info
-- ============================================================================

-- 18. pg_postmaster_start_time is not null
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (pg_postmaster_start_time() IS NOT NULL)::text AS ok;

-- 19. pg_conf_load_time is not null
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (pg_conf_load_time() IS NOT NULL)::text AS ok;

-- 20. pg_backend_pid positive
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (pg_backend_pid() > 0)::text AS ok;
