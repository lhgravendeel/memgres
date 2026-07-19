-- ============================================================================
-- Feature Comparison: trunc() precision and regexp_split_to_array trailing empties
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG: trunc(0.29, 2) = 0.29 (not 0.28 from double-precision artifact).
-- PG: regexp_split_to_array('a,b,', ',') = {a,b,""} (preserves trailing empty).
-- ============================================================================

-- ============================================================================
-- 1. trunc(0.29, 2) precision
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 0.29
-- end-expected
SELECT trunc(0.29, 2) AS result;

-- ============================================================================
-- 2. trunc(1.005, 2)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 1.00
-- end-expected
SELECT trunc(1.005, 2) AS result;

-- ============================================================================
-- 3. regexp_split_to_array trailing empty
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {a,b,""}
-- end-expected
SELECT regexp_split_to_array('a,b,', ',') AS result;

-- ============================================================================
-- 4. regexp_split_to_array multiple trailing empties
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {a,"",""}
-- end-expected
SELECT regexp_split_to_array('a,,', ',') AS result;
