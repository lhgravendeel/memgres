-- ============================================================================
-- Feature Comparison: PG 16+ numeric underscores, E-string escapes, $ identifiers
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- 1. Numeric underscore separators
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 1000000
-- end-expected
SELECT 1_000_000 AS result;

-- ============================================================================
-- 2. E-string octal escape
-- ============================================================================

-- begin-expected
-- columns: result
-- row: A
-- end-expected
SELECT E'\101' AS result;

-- ============================================================================
-- 3. E-string hex escape
-- ============================================================================

-- begin-expected
-- columns: result
-- row: A
-- end-expected
SELECT E'\x41' AS result;

-- ============================================================================
-- 4. E-string unicode escape
-- ============================================================================

-- begin-expected
-- columns: result
-- row: A
-- end-expected
SELECT E'\u0041' AS result;
