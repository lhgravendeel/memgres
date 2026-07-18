-- ============================================================================
-- Feature Comparison: LIKE default backslash escape
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG uses backslash as the default escape character in LIKE patterns.
-- \% matches a literal %, \_ matches a literal _.
-- ============================================================================

-- ============================================================================
-- 1. Escaped percent matches literal %
-- ============================================================================

-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT '100%' LIKE '100\%' AS result;

-- ============================================================================
-- 2. Escaped percent rejects non-%
-- ============================================================================

-- begin-expected
-- columns: result
-- row: f
-- end-expected
SELECT '100x' LIKE '100\%' AS result;

-- ============================================================================
-- 3. Escaped underscore matches literal _
-- ============================================================================

-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT 'foo_bar' LIKE 'foo\_bar' AS result;

-- ============================================================================
-- 4. Escaped underscore rejects non-_
-- ============================================================================

-- begin-expected
-- columns: result
-- row: f
-- end-expected
SELECT 'fooxbar' LIKE 'foo\_bar' AS result;

-- ============================================================================
-- 5. Unescaped percent still acts as wildcard
-- ============================================================================

-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT 'hello world' LIKE 'hello%' AS result;

-- ============================================================================
-- 6. NOT LIKE respects escape
-- ============================================================================

-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT '100x' NOT LIKE '100\%' AS result;
