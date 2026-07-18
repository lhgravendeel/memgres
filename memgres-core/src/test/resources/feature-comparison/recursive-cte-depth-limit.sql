-- ============================================================================
-- Feature Comparison: Recursive CTE should not silently truncate
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG has no hard iteration limit on recursive CTEs. A well-formed CTE runs
-- until the working table is empty. Memgres silently caps at 1000 iterations
-- and 10000 rows.
-- ============================================================================

-- ============================================================================
-- 1. Recursive CTE with 2000 iterations
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 2000
-- end-expected
WITH RECURSIVE r(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM r WHERE n < 2000
) SELECT count(*)::text AS cnt FROM r;

-- ============================================================================
-- 2. Recursive CTE with 5000 iterations — max value correct
-- ============================================================================

-- begin-expected
-- columns: mx
-- row: 5000
-- end-expected
WITH RECURSIVE r(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM r WHERE n < 5000
) SELECT max(n)::text AS mx FROM r;

-- ============================================================================
-- 3. Small CTE (sanity check)
-- ============================================================================

-- begin-expected
-- columns: cnt|mx
-- row: 10|10
-- end-expected
WITH RECURSIVE r(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM r WHERE n < 10
) SELECT count(*)::text AS cnt, max(n)::text AS mx FROM r;
