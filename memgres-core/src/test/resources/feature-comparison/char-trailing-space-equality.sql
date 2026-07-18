-- ============================================================================
-- Feature Comparison: Trailing-space string comparison consistency
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- The = and < operators must be consistent: they cannot both be true for the
-- same pair. Trailing-space-insensitive comparison (as used by CHAR(n)) must
-- apply uniformly to both = and < so ordering is coherent.
-- ============================================================================

-- ============================================================================
-- 1. CHAR(5) padded equality: 'a'::char(5) = 'a' should be true
-- ============================================================================

DROP TABLE IF EXISTS cse_char CASCADE;
CREATE TABLE cse_char (c char(5));
INSERT INTO cse_char VALUES ('a');

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM cse_char WHERE c = 'a';

-- ============================================================================
-- 2. Consistency: = and < cannot both be true
-- ============================================================================

-- begin-expected
-- columns: eq_and_lt
-- row: f
-- end-expected
SELECT ('a' = 'a ') AND ('a' < 'a ') AS eq_and_lt;

-- ============================================================================
-- 3. Non-space trailing chars still differ
-- ============================================================================

-- begin-expected
-- columns: eq
-- row: f
-- end-expected
SELECT 'a' = 'ab' AS eq;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS cse_char CASCADE;
