-- ============================================================================
-- Feature Comparison: Round 18 — ParameterStatus auto-push
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
--
-- The actual ParameterStatus observation happens via PGConnection.getParameterStatus().
-- These SQL statements just drive the GUC changes that should trigger the push.

-- ============================================================================
-- SECTION AD1-AD5: SET var → ParameterStatus
-- ============================================================================

-- 1. application_name round-trips via current_setting
SET application_name = 'r18_app';
SELECT current_setting('application_name') AS app;

-- 2. DateStyle
SET DateStyle = 'ISO, MDY';
SELECT (upper(current_setting('DateStyle')) LIKE '%MDY%') AS ok;

-- 3. IntervalStyle
SET IntervalStyle = 'iso_8601';
SELECT current_setting('IntervalStyle') AS v;

-- 4. TimeZone
SET TimeZone = 'UTC';
SELECT current_setting('TimeZone') AS v;

-- 5. client_encoding
SET client_encoding = 'UTF8';
SELECT current_setting('client_encoding') AS v;

-- ============================================================================
-- SECTION AD6: startup-pushed params readable via current_setting
-- ============================================================================

-- 6. server_encoding readable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (current_setting('server_encoding') IS NOT NULL) AS ok;

-- 7. standard_conforming_strings readable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (current_setting('standard_conforming_strings') IS NOT NULL) AS ok;

-- 8. integer_datetimes readable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (current_setting('integer_datetimes') IS NOT NULL) AS ok;

-- 9. is_superuser readable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (current_setting('is_superuser') IS NOT NULL) AS ok;
