-- ============================================================================
-- Feature Comparison: Round 15 — Wire-protocol GUC side effects
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION A: extra_float_digits
-- ============================================================================

-- 1. Default value
-- begin-expected
-- columns: extra_float_digits
-- row: 1
-- end-expected
SHOW extra_float_digits;

-- 2. Lossless float8 roundtrip at default = 3
SET extra_float_digits = 3;

SELECT (3.141592653589793::float8)::text AS v;

-- 3. Low-precision truncation
SET extra_float_digits = -15;

SELECT (3.141592653589793::float8)::text AS v;

SET extra_float_digits = 3;

-- ============================================================================
-- SECTION B: bytea_output
-- ============================================================================

-- 4. Default format
-- begin-expected
-- columns: bytea_output
-- row: hex
-- end-expected
SHOW bytea_output;

-- 5. hex format
SET bytea_output = 'hex';

-- begin-expected
-- columns: v
-- row: \x41
-- end-expected
SELECT E'\\x41'::bytea::text AS v;

-- 6. escape format (printable)
SET bytea_output = 'escape';

-- begin-expected
-- columns: v
-- row: A
-- end-expected
SELECT E'\\x41'::bytea::text AS v;

-- 7. escape format (non-printable)
-- begin-expected
-- columns: v
-- row: \000
-- end-expected
SELECT E'\\x00'::bytea::text AS v;

SET bytea_output = 'hex';

-- ============================================================================
-- SECTION C: IntervalStyle
-- ============================================================================

-- 8. Default
-- begin-expected
-- columns: IntervalStyle
-- row: postgres
-- end-expected
SHOW IntervalStyle;

-- 9. postgres
SET IntervalStyle = 'postgres';

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days 04:05:06
-- end-expected
SELECT (interval '1 year 2 months 3 days 04:05:06')::text AS v;

-- 10. iso_8601
SET IntervalStyle = 'iso_8601';

SELECT (interval '1 year 2 months 3 days 4 hours')::text AS v;

-- 11. postgres_verbose
SET IntervalStyle = 'postgres_verbose';

SELECT (interval '1 day 2 hours')::text AS v;

-- 12. sql_standard
SET IntervalStyle = 'sql_standard';

SELECT (interval '1-2')::text AS v;

SET IntervalStyle = 'postgres';

-- ============================================================================
-- SECTION D: DateStyle
-- ============================================================================

-- 13. ISO
SET DateStyle = 'ISO, MDY';

-- begin-expected
-- columns: v
-- row: 2025-03-14
-- end-expected
SELECT (DATE '2025-03-14')::text AS v;

-- 14. Postgres, MDY
SET DateStyle = 'Postgres, MDY';

SELECT (DATE '2025-03-14')::text AS v;

-- 15. SQL, MDY
SET DateStyle = 'SQL, MDY';

SELECT (DATE '2025-03-14')::text AS v;

-- 16. German
SET DateStyle = 'German';

SELECT (DATE '2025-03-14')::text AS v;

SET DateStyle = 'ISO, MDY';

-- ============================================================================
-- SECTION E: server_version + is_superuser + related GUCs
-- ============================================================================

-- 17. standard_conforming_strings default on
-- begin-expected
-- columns: standard_conforming_strings
-- row: on
-- end-expected
SHOW standard_conforming_strings;

-- 18. server_version_num
SELECT (current_setting('server_version_num')::int >= 170000)::text AS ok;

-- 19. is_superuser GUC is set
SELECT (current_setting('is_superuser') IN ('on','off'))::text AS ok;

-- 20. search_path GUC readable
SELECT (current_setting('search_path') IS NOT NULL)::text AS ok;
