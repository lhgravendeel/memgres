-- ============================================================================
-- Feature Comparison: decode() returns bytea, not text
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG: decode('aGVsbG8=', 'base64') returns bytea \x68656c6c6f.
-- Must not convert to UTF-8 String (corrupts non-UTF-8 bytes).
-- ============================================================================

-- ============================================================================
-- 1. decode base64 roundtrip
-- ============================================================================

-- begin-expected
-- columns: result
-- row: AQID
-- end-expected
SELECT encode(decode('AQID', 'base64'), 'base64') AS result;

-- ============================================================================
-- 2. decode base64 to hex representation
-- ============================================================================

-- begin-expected
-- columns: result
-- row: \x68656c6c6f
-- end-expected
SELECT decode('aGVsbG8=', 'base64') AS result;
