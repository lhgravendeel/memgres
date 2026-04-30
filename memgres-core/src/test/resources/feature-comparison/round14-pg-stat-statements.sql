-- ============================================================================
-- Feature Comparison: Round 14 — pg_stat_statements extension
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION A: Extension setup
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
-- re-running should be idempotent
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- 1. Extension recorded in pg_extension
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS ok
  FROM pg_extension WHERE extname = 'pg_stat_statements';

-- ============================================================================
-- SECTION B: Views exist and are selectable
-- ============================================================================

-- 2. pg_stat_statements selectable (any count ≥ 0)
-- begin-expected-error
-- message-like: must be
-- end-expected-error
SELECT (count(*) >= 0)::text AS ok FROM pg_stat_statements;

-- 3. pg_stat_statements_info selectable
-- begin-expected-error
-- message-like: must be
-- end-expected-error
SELECT (count(*) >= 0)::text AS ok FROM pg_stat_statements_info;

-- ============================================================================
-- SECTION C: Required columns present
-- ============================================================================

-- 4. Core columns — queryid, query, calls, total_exec_time, rows
SELECT queryid, query, calls, total_exec_time, rows
  FROM pg_stat_statements LIMIT 0;

-- 5. Plan vs exec time split (PG 13+)
SELECT total_plan_time, total_exec_time FROM pg_stat_statements LIMIT 0;

-- 6. toplevel column (PG 14+)
SELECT toplevel FROM pg_stat_statements LIMIT 0;

-- 7. WAL counters (PG 13+)
SELECT wal_records, wal_fpi, wal_bytes FROM pg_stat_statements LIMIT 0;

-- 8. JIT counters (PG 15+)
SELECT jit_functions, jit_generation_time FROM pg_stat_statements LIMIT 0;

-- ============================================================================
-- SECTION D: Reset function
-- ============================================================================

-- 9. reset() callable
SELECT pg_stat_statements_reset();

-- 10. reset with args — PG 17+ signature
SELECT pg_stat_statements_reset(0, 0, 0);

-- ============================================================================
-- SECTION E: GUCs
-- ============================================================================

-- 11. pg_stat_statements.max
SHOW pg_stat_statements.max;

-- 12. compute_query_id — PG 14+
SHOW compute_query_id;

-- 13. track_activity_query_size
SHOW track_activity_query_size;
