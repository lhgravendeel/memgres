-- ============================================================================
-- Feature Comparison: Binary NUMERIC decoding for values with weight < -1
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG binary NUMERIC format: weight field positions first digit group.
-- weight=-2 means value < 0.0001. Must insert leading zero groups in fraction.
-- ============================================================================

-- ============================================================================
-- 1. Value 0.00001 (weight = -2)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 0.00001
-- end-expected
SELECT 0.00001::numeric AS result;

-- ============================================================================
-- 2. Value 0.0001 (weight = -1, boundary)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 0.0001
-- end-expected
SELECT 0.0001::numeric AS result;

-- ============================================================================
-- 3. Value 0.00000001 (weight = -3)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 0.00000001
-- end-expected
SELECT 0.00000001::numeric AS result;

-- ============================================================================
-- 4. Negative tiny value
-- ============================================================================

-- begin-expected
-- columns: result
-- row: -0.00001
-- end-expected
SELECT -0.00001::numeric AS result;
