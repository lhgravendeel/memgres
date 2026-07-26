-- ============================================================================
-- Feature Comparison: wide date_trunc units, interval truncation, BC dates
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL counts centuries and millennia from year 1, has no year zero (so
-- a proleptic year of 0 or less prints with a BC era marker), and lets
-- date_trunc work on an interval as well as on a timestamp.
-- ============================================================================

-- ============================================================================
-- 1. The wide truncation units
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 2020-01-01 00:00:00
-- end-expected
SELECT date_trunc('decade', TIMESTAMP '2026-06-25 13:04:05')::text AS a;

-- A century starts in year 1, so 2026 truncates to 2001 and 2000 to 1901
-- begin-expected
-- columns: a
-- row: 2001-01-01 00:00:00
-- end-expected
SELECT date_trunc('century', TIMESTAMP '2026-06-25 13:04:05')::text AS a;

-- begin-expected
-- columns: a
-- row: 1901-01-01 00:00:00
-- end-expected
SELECT date_trunc('century', TIMESTAMP '2000-06-25')::text AS a;

-- begin-expected
-- columns: a
-- row: 2001-01-01 00:00:00
-- end-expected
SELECT date_trunc('millennium', TIMESTAMP '2026-06-25 13:04:05')::text AS a;

-- ============================================================================
-- 2. date_trunc over an interval
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 3 days 04:00:00
-- end-expected
SELECT date_trunc('hour', INTERVAL '3 days 4 hours 30 minutes')::text AS a;

-- begin-expected
-- columns: a
-- row: 3 days
-- end-expected
SELECT date_trunc('day', INTERVAL '3 days 4 hours 30 minutes')::text AS a;

-- begin-expected
-- columns: a
-- row: 5 years
-- end-expected
SELECT date_trunc('year', INTERVAL '5 years 3 mons 2 days')::text AS a;

-- ============================================================================
-- 3. epoch from an interval uses a 30-day month
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 2592000.000000
-- end-expected
SELECT extract(epoch FROM INTERVAL '1 mon')::text AS a;

-- ============================================================================
-- 4. BC dates read and print with their era
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 0044-03-15 00:00:00 BC
-- end-expected
SELECT (TIMESTAMP '0044-03-15 BC')::text AS a;

-- begin-expected
-- columns: a
-- row: 0044-03-15 BC
-- end-expected
SELECT (DATE '0044-03-15 BC')::text AS a;

-- A negative year argument means the same BC year
-- begin-expected
-- columns: a
-- row: 0044-03-15 BC
-- end-expected
SELECT make_date(-44,3,15)::text AS a;

-- begin-expected
-- columns: a
-- row: -44
-- end-expected
SELECT extract(year FROM TIMESTAMP '0044-03-15 BC')::text AS a;

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (DATE '0044-03-15 BC' < DATE '0001-01-01') AS a;

-- ============================================================================
-- 5. Infinity compares as the extreme value, for date as well as timestamp
-- ============================================================================

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (TIMESTAMP 'infinity' > TIMESTAMP '2026-01-01') AS a;

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('infinity'::date > DATE '2026-01-01') AS a;

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('-infinity'::date < DATE '2026-01-01') AS a;
