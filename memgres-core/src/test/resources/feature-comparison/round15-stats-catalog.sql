-- ============================================================================
-- Feature Comparison: Round 15 — Stats catalog views (PG 14+ / 16+ / 17+)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION A: pg_stat_io (PG 16+)
-- ============================================================================

-- 1. Columns
SELECT backend_type, object, context,
       reads, writes, extends, op_bytes,
       read_time, write_time, hits, evictions, reuses
  FROM pg_stat_io LIMIT 1;

-- ============================================================================
-- SECTION B: pg_stat_user_functions
-- ============================================================================

-- 2. Columns
SELECT funcid, schemaname, funcname, calls, total_time, self_time
  FROM pg_stat_user_functions LIMIT 1;

-- 3. Populated after call
CREATE OR REPLACE FUNCTION r15_sf_fn() RETURNS int AS 'SELECT 1' LANGUAGE SQL;
SET track_functions = 'all';

SELECT r15_sf_fn();
SELECT r15_sf_fn();

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (COALESCE(calls,0) >= 2)::text AS ok
  FROM pg_stat_user_functions WHERE funcname='r15_sf_fn';

-- ============================================================================
-- SECTION C: pg_stat_archiver
-- ============================================================================

-- 4. Columns
SELECT archived_count, last_archived_wal, last_archived_time,
       failed_count, last_failed_wal, last_failed_time, stats_reset
  FROM pg_stat_archiver LIMIT 1;

-- ============================================================================
-- SECTION D: pg_stat_database — PG 14+ columns
-- ============================================================================

-- 5. Session time columns
SELECT session_time, active_time, idle_in_transaction_time,
       sessions, sessions_abandoned, sessions_fatal, sessions_killed
  FROM pg_stat_database LIMIT 1;

-- 6. Classic columns still present
SELECT datid, datname, numbackends, xact_commit, xact_rollback,
       blks_read, blks_hit, tup_returned, tup_fetched,
       tup_inserted, tup_updated, tup_deleted, conflicts,
       temp_files, temp_bytes, deadlocks, stats_reset
  FROM pg_stat_database LIMIT 1;

-- ============================================================================
-- SECTION E: pg_stat_activity — PG 14+ columns
-- ============================================================================

-- 7. query_id, backend_xid, backend_xmin, wait_event*, leader_pid
SELECT query_id, backend_xid, backend_xmin,
       wait_event, wait_event_type, leader_pid
  FROM pg_stat_activity LIMIT 1;

SET compute_query_id = 'on';

-- 8. query_id populated when compute_query_id=on
SELECT query_id FROM pg_stat_activity WHERE pid = pg_backend_pid();

-- ============================================================================
-- SECTION F: pg_stat_user_indexes PG 16+ columns
-- ============================================================================

-- 9. idx_blks_read / idx_blks_hit
SELECT idx_blks_read, idx_blks_hit FROM pg_stat_user_indexes LIMIT 1;

-- ============================================================================
-- SECTION G: Replication stats stubs exist
-- ============================================================================

-- 10. pg_stat_replication
SELECT count(*)::int FROM pg_stat_replication;

-- 11. pg_stat_wal_receiver
SELECT count(*)::int FROM pg_stat_wal_receiver;

-- 12. pg_stat_subscription
SELECT count(*)::int FROM pg_stat_subscription;

-- ============================================================================
-- SECTION H: bgwriter / checkpointer stats
-- ============================================================================

-- 13. pg_stat_bgwriter exists
SELECT count(*)::int FROM pg_stat_bgwriter;

-- 14. pg_stat_checkpointer (PG 17+)
SELECT count(*)::int FROM pg_stat_checkpointer;

-- ============================================================================
-- SECTION I: pg_stat_reset*
-- ============================================================================

-- 15. pg_stat_reset()
SELECT pg_stat_reset();

-- 16. pg_stat_reset_shared
SELECT pg_stat_reset_shared('archiver');
SELECT pg_stat_reset_shared('bgwriter');
