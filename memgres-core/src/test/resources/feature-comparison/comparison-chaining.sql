-- ============================================================================
-- Feature Comparison: Comparison operators chain after IN/BETWEEN/LIKE
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG: SELECT 1 IN (1,2) = false → (1 IN (1,2)) = false → true = false → false.
-- ============================================================================

-- ============================================================================
-- 1. IN followed by = false
-- ============================================================================

-- begin-expected
-- columns: result
-- row: f
-- end-expected
SELECT 1 IN (1,2) = false AS result;

-- ============================================================================
-- 2. IN followed by = true
-- ============================================================================

-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT 1 IN (1,2) = true AS result;

-- ============================================================================
-- 3. NOT IN followed by = true
-- ============================================================================

-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT 3 NOT IN (1,2) = true AS result;

-- ============================================================================
-- 4. BETWEEN followed by = true
-- ============================================================================

-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT 5 BETWEEN 1 AND 10 = true AS result;
