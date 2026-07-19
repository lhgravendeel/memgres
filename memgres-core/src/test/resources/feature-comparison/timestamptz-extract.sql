-- ============================================================================
-- Feature Comparison: timestamptz EXTRACT/date_trunc use session timezone
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG converts timestamptz to session timezone (default UTC) before extracting.
-- '2024-01-15 10:30:00+05' → UTC is 05:30, so EXTRACT(HOUR) = 5, not 10.
-- ============================================================================

-- ============================================================================
-- 1. EXTRACT(HOUR) converts +05 to UTC
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 5
-- end-expected
SELECT EXTRACT(HOUR FROM TIMESTAMPTZ '2024-01-15 10:30:00+05') AS result;

-- ============================================================================
-- 2. EXTRACT(DAY) crosses midnight boundary
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 14
-- end-expected
SELECT EXTRACT(DAY FROM TIMESTAMPTZ '2024-01-15 02:00:00+05') AS result;

-- ============================================================================
-- 3. EXTRACT(HOUR) with negative offset
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 1
-- end-expected
SELECT EXTRACT(HOUR FROM TIMESTAMPTZ '2024-01-15 20:00:00-05') AS result;

-- ============================================================================
-- 4. UTC input is unchanged
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 10
-- end-expected
SELECT EXTRACT(HOUR FROM TIMESTAMPTZ '2024-01-15 10:30:00+00') AS result;
