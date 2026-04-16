-- ============================================================================
-- Feature Comparison: Round 14 — EXPLAIN & planner controls
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- EXPLAIN output shape is version-dependent; we only check presence-of-keyword.
-- ============================================================================

DROP SCHEMA IF EXISTS r14_ep CASCADE;
CREATE SCHEMA r14_ep;
SET search_path = r14_ep, public;

CREATE TABLE r14_ex (id int PRIMARY KEY, v int);
INSERT INTO r14_ex SELECT g, g*2 FROM generate_series(1, 100) g;

-- ============================================================================
-- SECTION A: EXPLAIN option matrix
-- ============================================================================

-- 1. EXPLAIN (COSTS OFF) — plan runs, no cost= sections
EXPLAIN (COSTS OFF) SELECT * FROM r14_ex;

-- 2. EXPLAIN (ANALYZE, BUFFERS)
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM r14_ex WHERE id = 1;

-- 3. EXPLAIN (ANALYZE, WAL) on write
BEGIN;
EXPLAIN (ANALYZE, WAL) INSERT INTO r14_ex VALUES (999, 0);
ROLLBACK;

-- 4. EXPLAIN (MEMORY) — PG 17+
EXPLAIN (MEMORY) SELECT * FROM r14_ex;

-- 5. EXPLAIN (ANALYZE, SERIALIZE TEXT) — PG 17+
EXPLAIN (ANALYZE, SERIALIZE TEXT) SELECT * FROM r14_ex;

-- 6. EXPLAIN (GENERIC_PLAN) — PG 16+
EXPLAIN (GENERIC_PLAN) SELECT * FROM r14_ex WHERE id = $1;

-- 7. EXPLAIN (FORMAT JSON)
EXPLAIN (FORMAT JSON) SELECT * FROM r14_ex;

-- 8. EXPLAIN (FORMAT YAML)
EXPLAIN (FORMAT YAML) SELECT * FROM r14_ex;

-- 9. EXPLAIN (FORMAT XML)
EXPLAIN (FORMAT XML) SELECT * FROM r14_ex;

-- 10. EXPLAIN (SETTINGS)
EXPLAIN (SETTINGS) SELECT * FROM r14_ex;

-- ============================================================================
-- SECTION B: plan_cache_mode
-- ============================================================================

SET plan_cache_mode = 'force_generic_plan';

-- 11. plan_cache_mode value
-- begin-expected
-- columns: plan_cache_mode
-- row: force_generic_plan
-- end-expected
SHOW plan_cache_mode;

RESET plan_cache_mode;

SET plan_cache_mode = 'force_custom_plan';

-- 12.
-- begin-expected
-- columns: plan_cache_mode
-- row: force_custom_plan
-- end-expected
SHOW plan_cache_mode;

RESET plan_cache_mode;

-- ============================================================================
-- SECTION C: enable_* toggles
-- ============================================================================

SET enable_seqscan = off;

-- 13.
-- begin-expected
-- columns: enable_seqscan
-- row: off
-- end-expected
SHOW enable_seqscan;

RESET enable_seqscan;

SET enable_partitionwise_join = on;

-- 14.
-- begin-expected
-- columns: enable_partitionwise_join
-- row: on
-- end-expected
SHOW enable_partitionwise_join;

RESET enable_partitionwise_join;

-- 15. PG 16+ enable_presorted_aggregate should exist
SHOW enable_presorted_aggregate;

-- ============================================================================
-- SECTION D: Parallel-query GUCs
-- ============================================================================

-- 16. PG has max_parallel_workers_per_gather
SHOW max_parallel_workers_per_gather;

-- 17. parallel_leader_participation
SHOW parallel_leader_participation;

-- ============================================================================
-- SECTION E: Planner cost constants
-- ============================================================================

SET random_page_cost = 1.1;

-- 18.
-- begin-expected
-- columns: random_page_cost
-- row: 1.1
-- end-expected
SHOW random_page_cost;

RESET random_page_cost;

-- 19. effective_cache_size is present
SHOW effective_cache_size;
