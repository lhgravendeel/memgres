-- ============================================================================
-- Feature Comparison: Targetless ON CONFLICT DO NOTHING checks all unique constraints
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG: INSERT ... ON CONFLICT DO NOTHING (no target) suppresses violation of
-- ANY unique constraint, not just PK.
-- ============================================================================

-- Setup
CREATE TABLE ocdn_test (id serial PRIMARY KEY, email text UNIQUE, name text);
INSERT INTO ocdn_test (email, name) VALUES ('a@b.com', 'Alice');

-- ============================================================================
-- 1. Targetless DO NOTHING suppresses unique violation on non-PK column
-- ============================================================================

INSERT INTO ocdn_test (email, name) VALUES ('a@b.com', 'Bob') ON CONFLICT DO NOTHING;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM ocdn_test;

-- Cleanup
DROP TABLE ocdn_test;
