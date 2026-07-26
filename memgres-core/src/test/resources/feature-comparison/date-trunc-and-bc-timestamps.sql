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

-- ============================================================================
-- 6. An interval can be infinite too
-- ============================================================================

-- begin-expected
-- columns: a | b
-- row: infinity, -infinity
-- end-expected
SELECT (INTERVAL 'infinity')::text AS a, (INTERVAL '-infinity')::text AS b;

-- The difference of two timestamps is infinite when either endpoint is
-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT (TIMESTAMP 'infinity' - TIMESTAMP '-infinity')::text AS a;

-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT (TIMESTAMP 'infinity' - TIMESTAMP '2026-01-01')::text AS a;

-- The sign follows which side the infinity sits on
-- begin-expected
-- columns: a
-- row: -infinity
-- end-expected
SELECT (TIMESTAMP '2026-01-01' - TIMESTAMP 'infinity')::text AS a;

-- begin-expected
-- columns: a | b | c
-- row: infinity, -infinity, infinity
-- end-expected
SELECT (INTERVAL 'infinity' + INTERVAL '1 day')::text AS a,
       (INTERVAL '-infinity' + INTERVAL '1 day')::text AS b,
       (INTERVAL 'infinity' * 2)::text AS c;

-- begin-expected
-- columns: a | b
-- row: false, true
-- end-expected
SELECT isfinite(INTERVAL 'infinity') AS a, isfinite(INTERVAL '1 day') AS b;

-- An infinity sits outside every finite interval
-- begin-expected
-- columns: a | b | c
-- row: true, true, true
-- end-expected
SELECT (INTERVAL 'infinity' > INTERVAL '1000 years') AS a,
       (INTERVAL '-infinity' < INTERVAL '-1000 years') AS b,
       (INTERVAL 'infinity' = INTERVAL 'infinity') AS c;

-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT (TIMESTAMP '2026-01-01' + INTERVAL 'infinity')::text AS a;

-- An indeterminate result is an error, not a guess
-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT (INTERVAL 'infinity' - INTERVAL 'infinity');

-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT (INTERVAL 'infinity' * 0);

-- begin-expected
-- columns: a | b | c
-- row: Infinity, infinity, infinity
-- end-expected
SELECT extract(epoch FROM INTERVAL 'infinity')::text AS a,
       justify_hours(INTERVAL 'infinity')::text AS b,
       date_trunc('day', INTERVAL 'infinity')::text AS c;

-- ============================================================================
-- 7. A BC date is a date, not text with a suffix
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 366
-- end-expected
SELECT (DATE '0001-01-01' - DATE '0001-01-01 BC')::text AS a;

-- begin-expected
-- columns: a
-- row: 0044-03-16 BC
-- end-expected
SELECT (DATE '0044-03-15 BC' + 1)::text AS a;

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (DATE '0044-03-15 BC' < DATE '0043-01-01 BC') AS a;
