-- ============================================================================
-- Feature Comparison: Sequence CACHE Semantics
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG 18: When CACHE n > 1, each session pre-allocates n values from the
-- sequence. If a session disconnects without using all cached values, those
-- values are "lost" (gaps appear). CACHE 1 (default) has no gaps.
--
-- Memgres: CACHE is parsed and stored but nextval() always hits the
-- AtomicLong counter directly with no session-level caching. Sequences are
-- strictly sequential regardless of CACHE setting.
--
-- NOTE: This file can only test single-session CACHE behavior. The main
-- gap (inter-session gaps from cached-but-unused values) requires concurrent
-- sessions and is tested in the Java unit test SequenceCacheCompatTest.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- 1. Basic sequence with CACHE should be creatable
-- ============================================================================

DROP SEQUENCE IF EXISTS seq_cache_basic;
CREATE SEQUENCE seq_cache_basic START 1 CACHE 10;

-- begin-expected
-- columns: v1
-- row: 1
-- end-expected
SELECT nextval('seq_cache_basic') AS v1;

-- begin-expected
-- columns: v2
-- row: 2
-- end-expected
SELECT nextval('seq_cache_basic') AS v2;

-- ============================================================================
-- 2. Sequence metadata should reflect CACHE value
-- ============================================================================

-- begin-expected
-- columns: cache_size
-- row: 10
-- end-expected
SELECT cache_size FROM pg_sequences WHERE sequencename = 'seq_cache_basic';

-- ============================================================================
-- 3. CACHE 1 should behave normally (no gaps in single session)
-- ============================================================================

DROP SEQUENCE IF EXISTS seq_cache_one;
CREATE SEQUENCE seq_cache_one START 100 CACHE 1;

-- begin-expected
-- columns: v1
-- row: 100
-- end-expected
SELECT nextval('seq_cache_one') AS v1;

-- begin-expected
-- columns: v2
-- row: 101
-- end-expected
SELECT nextval('seq_cache_one') AS v2;

-- begin-expected
-- columns: v3
-- row: 102
-- end-expected
SELECT nextval('seq_cache_one') AS v3;

-- ============================================================================
-- 4. CYCLE with CACHE should work
-- ============================================================================

DROP SEQUENCE IF EXISTS seq_cache_cycle;
CREATE SEQUENCE seq_cache_cycle START 1 MAXVALUE 5 CACHE 3 CYCLE;

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT nextval('seq_cache_cycle') AS v;

-- begin-expected
-- columns: v
-- row: 2
-- end-expected
SELECT nextval('seq_cache_cycle') AS v;

-- begin-expected
-- columns: v
-- row: 3
-- end-expected
SELECT nextval('seq_cache_cycle') AS v;

-- begin-expected
-- columns: v
-- row: 4
-- end-expected
SELECT nextval('seq_cache_cycle') AS v;

-- begin-expected
-- columns: v
-- row: 5
-- end-expected
SELECT nextval('seq_cache_cycle') AS v;

-- note: Next call should cycle back to 1

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT nextval('seq_cache_cycle') AS v;

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP SEQUENCE seq_cache_basic;
DROP SEQUENCE seq_cache_one;
DROP SEQUENCE seq_cache_cycle;
