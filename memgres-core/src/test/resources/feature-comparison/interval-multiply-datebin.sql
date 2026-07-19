-- ============================================================================
-- Feature Comparison: interval * 1.5 cascades; date_bin pre-origin; age(ts) midnight
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- 1. interval * 1.5 cascades fractional months to days
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 1 mon 15 days
-- end-expected
SELECT ('1 month'::interval * 1.5)::text AS result;

-- ============================================================================
-- 2. interval * 1.5 cascades fractional days to hours
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 1 day 12:00:00
-- end-expected
SELECT ('1 day'::interval * 1.5)::text AS result;

-- ============================================================================
-- 3. date_bin pre-origin floors correctly
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 2024-01-01 00:00:00
-- end-expected
SELECT date_bin('1 hour'::interval, TIMESTAMP '2024-01-01 00:30:00', TIMESTAMP '2024-01-01 02:00:00') AS result;
