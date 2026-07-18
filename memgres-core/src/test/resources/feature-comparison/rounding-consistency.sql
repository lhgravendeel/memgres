-- ============================================================================
-- Feature Comparison: Rounding consistency (HALF_UP, not HALF_EVEN)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG uses "round half away from zero" for both casts and round().
-- Banker's rounding (HALF_EVEN) gives different results at midpoints.
-- ============================================================================

-- ============================================================================
-- 1. 2.5::int should be 3 (not 2)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 3
-- end-expected
SELECT 2.5::int AS result;

-- ============================================================================
-- 2. 3.5::int should be 4
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 4
-- end-expected
SELECT 3.5::int AS result;

-- ============================================================================
-- 3. (-2.5)::int should be -3 (not -2)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: -3
-- end-expected
SELECT (-2.5)::int AS result;

-- ============================================================================
-- 4. 2.5::bigint should be 3 (consistent with ::int)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 3
-- end-expected
SELECT 2.5::bigint AS result;

-- ============================================================================
-- 5. round(-2.5) should be -3
-- ============================================================================

-- begin-expected
-- columns: result
-- row: -3
-- end-expected
SELECT round(-2.5) AS result;

-- ============================================================================
-- 6. round(2.5) should be 3
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 3
-- end-expected
SELECT round(2.5) AS result;

-- ============================================================================
-- 7. round(2.55, 1) should be 2.6
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 2.6
-- end-expected
SELECT round(2.55, 1) AS result;
