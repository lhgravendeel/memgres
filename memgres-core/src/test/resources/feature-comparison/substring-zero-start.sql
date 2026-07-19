-- ============================================================================
-- Feature Comparison: substring() with zero/negative start position
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG: substring('hello', 0, 2) → 'h'. Start position 0 means 1 char of the
-- requested length is consumed before the string begins.
-- ============================================================================

-- ============================================================================
-- 1. Zero start, length 2
-- ============================================================================

-- begin-expected
-- columns: result
-- row: h
-- end-expected
SELECT substring('hello', 0, 2) AS result;

-- ============================================================================
-- 2. Zero start, length 3
-- ============================================================================

-- begin-expected
-- columns: result
-- row: he
-- end-expected
SELECT substring('hello', 0, 3) AS result;

-- ============================================================================
-- 3. Zero start, length 1 → empty
-- ============================================================================

-- begin-expected
-- columns: result
-- row: (empty)
-- end-expected
SELECT substring('hello', 0, 1) AS result;

-- ============================================================================
-- 4. Negative start
-- ============================================================================

-- begin-expected
-- columns: result
-- row: he
-- end-expected
SELECT substring('hello', -1, 4) AS result;
