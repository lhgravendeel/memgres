-- ============================================================================
-- Feature Comparison: Extended Date/Time Functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests date_bin, make_date, make_time, make_timestamp, make_timestamptz,
-- make_interval, extract() edge cases, date_trunc with timezone, and
-- AT TIME ZONE conversions.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS dt_test CASCADE;
CREATE SCHEMA dt_test;
SET search_path = dt_test, public;
SET timezone = 'UTC';

-- ============================================================================
-- SECTION A: date_bin (PG 14+)
-- ============================================================================

-- ============================================================================
-- 1. date_bin: 15-minute bins
-- ============================================================================

-- begin-expected
-- columns: binned
-- row: 2024-02-11 15:30:00
-- end-expected
SELECT date_bin(
  '15 minutes'::interval,
  '2024-02-11 15:44:17'::timestamp,
  '2024-02-11 15:00:00'::timestamp
) AS binned;

-- ============================================================================
-- 2. date_bin: hourly bins
-- ============================================================================

-- begin-expected
-- columns: binned
-- row: 2024-02-11 15:00:00
-- end-expected
SELECT date_bin(
  '1 hour'::interval,
  '2024-02-11 15:44:17'::timestamp,
  '2024-02-11 00:00:00'::timestamp
) AS binned;

-- ============================================================================
-- 3. date_bin: daily bins
-- ============================================================================

-- begin-expected
-- columns: binned
-- row: 2024-02-11 00:00:00
-- end-expected
SELECT date_bin(
  '1 day'::interval,
  '2024-02-11 15:44:17'::timestamp,
  '2024-01-01 00:00:00'::timestamp
) AS binned;

-- ============================================================================
-- 4. date_bin: 5-minute bins
-- ============================================================================

-- begin-expected
-- columns: binned
-- row: 2024-02-11 15:40:00
-- end-expected
SELECT date_bin(
  '5 minutes'::interval,
  '2024-02-11 15:44:17'::timestamp,
  '2024-02-11 00:00:00'::timestamp
) AS binned;

-- ============================================================================
-- 5. date_bin: value at exact bin boundary
-- ============================================================================

-- begin-expected
-- columns: binned
-- row: 2024-02-11 15:00:00
-- end-expected
SELECT date_bin(
  '1 hour'::interval,
  '2024-02-11 15:00:00'::timestamp,
  '2024-02-11 00:00:00'::timestamp
) AS binned;

-- ============================================================================
-- SECTION B: make_date, make_time, make_timestamp
-- ============================================================================

-- ============================================================================
-- 6. make_date
-- ============================================================================

-- begin-expected
-- columns: d
-- row: 2024-03-15
-- end-expected
SELECT make_date(2024, 3, 15) AS d;

-- ============================================================================
-- 7. make_date: edge cases
-- ============================================================================

-- begin-expected
-- columns: d
-- row: 2024-02-29
-- end-expected
SELECT make_date(2024, 2, 29) AS d;

-- begin-expected-error
-- message-like: date
-- end-expected-error
SELECT make_date(2023, 2, 29);

-- ============================================================================
-- 8. make_time
-- ============================================================================

-- begin-expected
-- columns: t
-- row: 14:30:45
-- end-expected
SELECT make_time(14, 30, 45) AS t;

-- ============================================================================
-- 9. make_time: with fractional seconds
-- ============================================================================

-- begin-expected
-- columns: t
-- row: 14:30:45.123
-- end-expected
SELECT make_time(14, 30, 45.123) AS t;

-- ============================================================================
-- 10. make_timestamp
-- ============================================================================

-- begin-expected
-- columns: ts
-- row: 2024-07-04 12:00:00
-- end-expected
SELECT make_timestamp(2024, 7, 4, 12, 0, 0) AS ts;

-- ============================================================================
-- 11. make_timestamp: with fractional seconds
-- ============================================================================

-- begin-expected
-- columns: ts
-- row: 2024-07-04 12:00:30.5
-- end-expected
SELECT make_timestamp(2024, 7, 4, 12, 0, 30.5) AS ts;

-- ============================================================================
-- 12. make_timestamptz
-- ============================================================================

-- begin-expected
-- columns: ts
-- row: 2024-07-04 12:00:00+00
-- end-expected
SELECT make_timestamptz(2024, 7, 4, 12, 0, 0) AS ts;

-- ============================================================================
-- SECTION C: make_interval
-- ============================================================================

-- ============================================================================
-- 13. make_interval: basic
-- ============================================================================

-- begin-expected
-- columns: i
-- row: 1 year 2 mons
-- end-expected
SELECT make_interval(years => 1, months => 2) AS i;

-- ============================================================================
-- 14. make_interval: days and hours
-- ============================================================================

-- begin-expected
-- columns: i
-- row: 3 days 04:00:00
-- end-expected
SELECT make_interval(days => 3, hours => 4) AS i;

-- ============================================================================
-- 15. make_interval: all components
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 2024-02-20 14:30:45
-- end-expected
SELECT '2024-01-15 10:00:00'::timestamp +
  make_interval(months => 1, days => 5, hours => 4, mins => 30, secs => 45) AS result;

-- ============================================================================
-- SECTION D: EXTRACT edge cases
-- ============================================================================

-- ============================================================================
-- 16. EXTRACT from interval
-- ============================================================================

-- begin-expected
-- columns: h, m, s
-- row: 4, 30, 15
-- end-expected
SELECT
  extract(hour FROM interval '4:30:15') AS h,
  extract(minute FROM interval '4:30:15') AS m,
  extract(second FROM interval '4:30:15') AS s;

-- ============================================================================
-- 17. EXTRACT epoch
-- ============================================================================

-- begin-expected
-- columns: epoch
-- row: 0
-- end-expected
SELECT extract(epoch FROM timestamp '1970-01-01 00:00:00')::integer AS epoch;

-- ============================================================================
-- 18. EXTRACT isodow (1=Monday, 7=Sunday)
-- ============================================================================

-- note: 2024-01-15 is a Monday
-- begin-expected
-- columns: isodow
-- row: 1
-- end-expected
SELECT extract(isodow FROM date '2024-01-15')::integer AS isodow;

-- ============================================================================
-- 19. EXTRACT week (ISO week number)
-- ============================================================================

-- begin-expected
-- columns: week
-- row: 3
-- end-expected
SELECT extract(week FROM date '2024-01-15')::integer AS week;

-- ============================================================================
-- 20. EXTRACT century and millennium
-- ============================================================================

-- begin-expected
-- columns: century, millennium
-- row: 21, 3
-- end-expected
SELECT
  extract(century FROM date '2024-01-01')::integer AS century,
  extract(millennium FROM date '2024-01-01')::integer AS millennium;

-- ============================================================================
-- SECTION E: date_trunc
-- ============================================================================

-- ============================================================================
-- 21. date_trunc: various precisions
-- ============================================================================

-- begin-expected
-- columns: to_hour
-- row: 2024-02-11 15:00:00
-- end-expected
SELECT date_trunc('hour', timestamp '2024-02-11 15:44:17') AS to_hour;

-- begin-expected
-- columns: to_day
-- row: 2024-02-11 00:00:00
-- end-expected
SELECT date_trunc('day', timestamp '2024-02-11 15:44:17') AS to_day;

-- begin-expected
-- columns: to_month
-- row: 2024-02-01 00:00:00
-- end-expected
SELECT date_trunc('month', timestamp '2024-02-11 15:44:17') AS to_month;

-- begin-expected
-- columns: to_year
-- row: 2024-01-01 00:00:00
-- end-expected
SELECT date_trunc('year', timestamp '2024-02-11 15:44:17') AS to_year;

-- ============================================================================
-- 22. date_trunc: week (truncates to Monday)
-- ============================================================================

-- note: 2024-02-11 is a Sunday, so truncating to week gives previous Monday
-- begin-expected
-- columns: to_week
-- row: 2024-02-05 00:00:00
-- end-expected
SELECT date_trunc('week', timestamp '2024-02-11 15:44:17') AS to_week;

-- ============================================================================
-- 23. date_trunc: quarter
-- ============================================================================

-- begin-expected
-- columns: to_quarter
-- row: 2024-04-01 00:00:00
-- end-expected
SELECT date_trunc('quarter', timestamp '2024-05-15 12:00:00') AS to_quarter;

-- ============================================================================
-- SECTION F: AT TIME ZONE
-- ============================================================================

-- ============================================================================
-- 24. AT TIME ZONE: timestamp to timestamptz
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 2024-02-11 20:00:00+00
-- end-expected
SELECT timestamp '2024-02-11 15:00:00' AT TIME ZONE 'US/Eastern' AS result;

-- ============================================================================
-- 25. AT TIME ZONE: timestamptz to timestamp
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 2024-02-11 10:00:00
-- end-expected
SELECT timestamptz '2024-02-11 15:00:00+00' AT TIME ZONE 'US/Eastern' AS result;

-- ============================================================================
-- SECTION G: Interval arithmetic
-- ============================================================================

-- ============================================================================
-- 26. Interval addition
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 2024-03-15
-- end-expected
SELECT (date '2024-01-15' + interval '2 months')::date AS result;

-- ============================================================================
-- 27. Interval subtraction
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 2023-11-15
-- end-expected
SELECT (date '2024-01-15' - interval '2 months')::date AS result;

-- ============================================================================
-- 28. Date difference
-- ============================================================================

-- begin-expected
-- columns: days
-- row: 31
-- end-expected
SELECT date '2024-02-15' - date '2024-01-15' AS days;

-- ============================================================================
-- 29. Interval multiplication
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 06:00:00
-- end-expected
SELECT interval '2 hours' * 3 AS result;

-- ============================================================================
-- 30. age() function
-- ============================================================================

-- begin-expected
-- columns: age_val
-- row: 1 year 1 mon
-- end-expected
SELECT age(timestamp '2025-02-15', timestamp '2024-01-15') AS age_val;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA dt_test CASCADE;
SET search_path = public;
