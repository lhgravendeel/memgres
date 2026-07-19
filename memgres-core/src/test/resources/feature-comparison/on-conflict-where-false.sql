-- ============================================================================
-- Feature Comparison: ON CONFLICT DO UPDATE WHERE false skips row silently
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG: when WHERE evaluates to false, no update happens, no row returned.
-- ============================================================================

-- Setup
CREATE TABLE ocwf_test (id int PRIMARY KEY, val text);
INSERT INTO ocwf_test VALUES (1, 'orig');

-- ============================================================================
-- 1. WHERE false → row unchanged
-- ============================================================================

INSERT INTO ocwf_test VALUES (1, 'new') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE false;

-- begin-expected
-- columns: val
-- row: orig
-- end-expected
SELECT val FROM ocwf_test WHERE id = 1;

-- ============================================================================
-- 2. WHERE false → RETURNING produces no rows
-- ============================================================================

-- begin-expected
-- columns: id|val
-- end-expected
INSERT INTO ocwf_test VALUES (1, 'newer') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE false RETURNING *;

-- Cleanup
DROP TABLE ocwf_test;
