-- ============================================================================
-- Feature Comparison: Round 16 — System / admin functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION M1: pg_log_backend_memory_contexts
-- ============================================================================

-- 1. pg_log_backend_memory_contexts returns boolean
-- begin-expected
-- columns: r
-- row: .*
-- end-expected
SELECT pg_log_backend_memory_contexts(pg_backend_pid()) AS r;

-- ============================================================================
-- SECTION M2: pg_promote / pg_wal_replay_*
-- ============================================================================

-- 2. pg_promote returns boolean
-- begin-expected
-- columns: r
-- row: .*
-- end-expected
SELECT pg_promote(false, 0) AS r;

-- 3. pg_wal_replay_pause exists in pg_proc
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='pg_wal_replay_pause';

-- ============================================================================
-- SECTION M3: pg_switch_wal / pg_backup_start
-- ============================================================================

-- 4. pg_switch_wal exists in pg_proc
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='pg_switch_wal';

-- 5. pg_backup_start exists in pg_proc
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='pg_backup_start';

-- ============================================================================
-- SECTION M4: pg_size_bytes
-- ============================================================================

-- 6. pg_size_bytes('10 MB') = 10485760
-- begin-expected
-- columns: v
-- row: 10485760
-- end-expected
SELECT pg_size_bytes('10 MB') AS v;

-- ============================================================================
-- SECTION M5: pg_tablespace_size
-- ============================================================================

-- 7. pg_tablespace_size('pg_default') returns a non-negative bigint
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (pg_tablespace_size('pg_default') >= 0) AS ok;

-- ============================================================================
-- SECTION M6: has_parameter_privilege
-- ============================================================================

-- 8. has_parameter_privilege returns boolean
-- begin-expected
-- columns: r
-- row: .*
-- end-expected
SELECT has_parameter_privilege(current_user, 'work_mem', 'SET') AS r;

-- ============================================================================
-- SECTION M7: pg_column_size consistency
-- ============================================================================

-- 9. pg_column_size for 5-byte text == for 5-byte bytea
-- begin-expected
-- columns: eq
-- row: t
-- end-expected
SELECT (pg_column_size('hello'::text) = pg_column_size('hello'::bytea)) AS eq;

-- ============================================================================
-- SECTION M8: pg_relation_size fork argument
-- ============================================================================

CREATE TABLE r16_rs (id int);

-- 10. pg_relation_size(t, 'fsm') accepts fork arg
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (pg_relation_size('r16_rs'::regclass, 'fsm') >= 0) AS ok;

-- ============================================================================
-- SECTION M9: pg_stat_reset_shared validates target
-- ============================================================================

-- 11. pg_stat_reset_shared('nonsense') must error
-- begin-expected-error
-- message-like: unrecognized
-- end-expected-error
SELECT pg_stat_reset_shared('nonsense');
