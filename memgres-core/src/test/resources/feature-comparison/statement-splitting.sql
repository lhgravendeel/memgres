-- ============================================================================
-- Feature Comparison: Statement splitting with $$ bodies and E-strings
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Dollar-quoted bodies are fully literal — apostrophes inside do not affect
-- statement splitting. E-strings use backslash escaping: E'\'' is an apostrophe.
-- ============================================================================

-- ============================================================================
-- 1. E-string with backslash-escaped apostrophe
-- ============================================================================

-- begin-expected
-- columns: result
-- row: it's
-- end-expected
SELECT E'it\'s' AS result;

-- ============================================================================
-- 2. E-string with backslash-backslash (literal backslash)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: \
-- end-expected
SELECT E'\\' AS result;

-- ============================================================================
-- 3. Normal doubled apostrophe still works
-- ============================================================================

-- begin-expected
-- columns: result
-- row: it's fine
-- end-expected
SELECT 'it''s fine' AS result;
