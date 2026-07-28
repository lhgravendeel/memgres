-- ============================================================================
-- Feature Comparison: field extraction and the date/time unit vocabulary
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- extract() and date_part() read a field out of a temporal value, and
-- date_trunc() zeroes everything below one. All three name the field with a
-- unit, which PostgreSQL matches case-insensitively against a fixed token
-- table, comparing only the first ten characters -- so "microseconds" is a
-- unit and "microsecs" is not. A unit that is a known word but has no meaning
-- for this type raises 0A000; a word the table does not hold at all raises
-- 22023, and both errors name the type that could not answer.
--
-- Covers: every extraction from time and timetz; the plural and abbreviated
-- spellings; the units each type does and does not take; extract() and
-- date_part() disagreeing about a date; date_trunc resolving a date through
-- the timestamptz form, a time through the interval form, and a timetz not at
-- all; date_bin's stride validation and origin; the numeric scale extract()
-- reports against date_part()'s float8; infinite values; and the session
-- TimeZone still deciding what a timestamptz's fields are.
-- ============================================================================

SET TimeZone = 'UTC';

-- ============================================================================
-- 1. extract() from a time
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 36000.000000
-- end-expected
SELECT extract(epoch from time '10:00:00') AS a;

-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT extract(hour from time '10:20:30') AS a;

-- begin-expected
-- columns: a
-- row: 20
-- end-expected
SELECT extract(minute from time '10:20:30') AS a;

-- extract() reports a numeric of a fixed scale, so the zeros stay
-- begin-expected
-- columns: a
-- row: 30.500000
-- end-expected
SELECT extract(second from time '10:20:30.5') AS a;

-- begin-expected
-- columns: a
-- row: 30.000000
-- end-expected
SELECT extract(second from time '10:20:30') AS a;

-- begin-expected
-- columns: a
-- row: 30500000
-- end-expected
SELECT extract(microseconds from time '10:20:30.5') AS a;

-- begin-expected
-- columns: a
-- row: 30500.000
-- end-expected
SELECT extract(milliseconds from time '10:20:30.5') AS a;

-- begin-expected
-- columns: a
-- row: 30000.000
-- end-expected
SELECT extract(milliseconds from time '10:20:30') AS a;

-- date_part() reports a float8 of the same value, so the zeros go
-- begin-expected
-- columns: a
-- row: 30
-- end-expected
SELECT date_part('second', time '10:20:30') AS a;

-- begin-expected
-- columns: a
-- row: 36000
-- end-expected
SELECT date_part('epoch', time '10:00:00') AS a;

-- ============================================================================
-- 2. extract() from a timetz
-- ============================================================================

-- a timetz's epoch is its time of day taken back to UTC
-- begin-expected
-- columns: a
-- row: 28800.000000
-- end-expected
SELECT extract(epoch from timetz '10:00:00+02') AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT extract(timezone_hour from timetz '10:00:00+02') AS a;

-- begin-expected
-- columns: a
-- row: -5
-- end-expected
SELECT extract(timezone_hour from timetz '10:00:00-05') AS a;

-- begin-expected
-- columns: a
-- row: 30
-- end-expected
SELECT extract(timezone_minute from timetz '10:00:00+02:30') AS a;

-- begin-expected
-- columns: a
-- row: -30
-- end-expected
SELECT extract(timezone_minute from timetz '10:00:00-02:30') AS a;

-- begin-expected
-- columns: a
-- row: 7200
-- end-expected
SELECT extract(timezone from timetz '10:00:00+02') AS a;

-- begin-expected
-- columns: a
-- row: -18000
-- end-expected
SELECT extract(timezone from timetz '10:00:00-05') AS a;

-- the time of day itself is the literal's, not the one UTC would show
-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT extract(hour from timetz '10:20:30+02') AS a;

-- begin-expected
-- columns: a
-- row: 28800
-- end-expected
SELECT date_part('epoch', timetz '10:00:00+02') AS a;

-- ============================================================================
-- 3. extract() from time columns
-- ============================================================================

DROP TABLE IF EXISTS dtu_times;
CREATE TABLE dtu_times (t time, tz timetz);
INSERT INTO dtu_times VALUES ('10:20:30.5', '10:20:30.5+02');

-- begin-expected
-- columns: h, s
-- row: 10 | 30.500000
-- end-expected
SELECT extract(hour from t) AS h, extract(second from t) AS s FROM dtu_times;

-- begin-expected
-- columns: e, tzh
-- row: 30030.500000 | 2
-- end-expected
SELECT extract(epoch from tz) AS e, extract(timezone_hour from tz) AS tzh FROM dtu_times;

-- begin-expected
-- columns: a
-- row: 10:00:00
-- end-expected
SELECT date_trunc('hour', t)::text AS a FROM dtu_times;

DROP TABLE dtu_times;

-- ============================================================================
-- 4. A time has no calendar fields, and no reserved word but epoch
-- ============================================================================

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "year" not supported for type time without time zone
-- end-expected-error
SELECT extract(year from time '10:20:30');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "dow" not supported for type time without time zone
-- end-expected-error
SELECT extract(dow from time '10:20:30');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "julian" not supported for type time without time zone
-- end-expected-error
SELECT extract(julian from time '10:20:30');

-- a plain time has no zone to report either
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "timezone_hour" not supported for type time without time zone
-- end-expected-error
SELECT extract(timezone_hour from time '10:20:30');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "year" not supported for type time with time zone
-- end-expected-error
SELECT extract(year from timetz '10:20:30+02');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "bogus" not recognized for type time without time zone
-- end-expected-error
SELECT extract('bogus' from time '10:20:30');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "bogus" not recognized for type time with time zone
-- end-expected-error
SELECT extract('bogus' from timetz '10:20:30+02');

-- "now" is a word the unit table holds, but a time cannot be asked about it
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "now" not recognized for type time without time zone
-- end-expected-error
SELECT extract('now' from time '10:20:30');

-- ============================================================================
-- 5. Plural and abbreviated units
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 2026-07-28 00:00:00
-- end-expected
SELECT date_trunc('days', timestamp '2026-07-28 10:20:30')::text AS a;

-- begin-expected
-- columns: a
-- row: 2026-07-28 10:00:00
-- end-expected
SELECT date_trunc('hrs', timestamp '2026-07-28 10:20:30')::text AS a;

-- begin-expected
-- columns: a
-- row: 2026-07-01 00:00:00
-- end-expected
SELECT date_trunc('qtr', timestamp '2026-07-28 10:20:30')::text AS a;

-- begin-expected
-- columns: a
-- row: 2026-07-27 00:00:00
-- end-expected
SELECT date_trunc('w', timestamp '2026-07-28 10:20:30')::text AS a;

-- begin-expected
-- columns: a
-- row: 2026-01-01 00:00:00
-- end-expected
SELECT date_trunc('years', timestamp '2026-07-28 10:20:30')::text AS a;

-- begin-expected
-- columns: a
-- row: 2026-07-28 10:20:00
-- end-expected
SELECT date_trunc('minutes', timestamp '2026-07-28 10:20:30')::text AS a;

-- begin-expected
-- columns: a
-- row: 7
-- end-expected
SELECT date_part('mons', timestamp '2026-07-28 10:20:30') AS a;

-- begin-expected
-- columns: a
-- row: 2026
-- end-expected
SELECT date_part('yrs', timestamp '2026-07-28 10:20:30') AS a;

-- begin-expected
-- columns: a
-- row: 30123456
-- end-expected
SELECT date_part('usec', timestamp '2026-07-28 10:20:30.123456') AS a;

-- begin-expected
-- columns: y, d, h, mi, s, w
-- row: 2026 | 28 | 10 | 20 | 30 | 31
-- end-expected
SELECT date_part('y', timestamp '2026-07-28 10:20:30') AS y,
       date_part('d', timestamp '2026-07-28 10:20:30') AS d,
       date_part('h', timestamp '2026-07-28 10:20:30') AS h,
       date_part('min', timestamp '2026-07-28 10:20:30') AS mi,
       date_part('s', timestamp '2026-07-28 10:20:30') AS s,
       date_part('w', timestamp '2026-07-28 10:20:30') AS w;

-- begin-expected
-- columns: a
-- row: 2461250.4309027777
-- end-expected
SELECT date_part('j', timestamp '2026-07-28 10:20:30') AS a;

-- ============================================================================
-- 6. A unit matches on its first ten characters only
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 30123456
-- end-expected
SELECT date_part('microsecondsfoo', timestamp '2026-07-28 10:20:30.123456') AS a;

-- begin-expected
-- columns: a
-- row: 30123.456
-- end-expected
SELECT date_part('millisecondfoo', timestamp '2026-07-28 10:20:30.123456') AS a;

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT date_part('millenniums', timestamp '2026-07-28 10:20:30') AS a;

-- a shorter spelling that is not itself a token is simply unknown
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "microsecs" not recognized for type timestamp without time zone
-- end-expected-error
SELECT date_part('microsecs', timestamp '2026-07-28 10:20:30');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "millisecs" not recognized for type timestamp without time zone
-- end-expected-error
SELECT date_part('millisecs', timestamp '2026-07-28 10:20:30');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "quarters" not recognized for type timestamp without time zone
-- end-expected-error
SELECT date_part('quarters', timestamp '2026-07-28 10:20:30');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "yy" not recognized for type timestamp without time zone
-- end-expected-error
SELECT date_part('yy', timestamp '2026-07-28 10:20:30');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "wk" not recognized for type timestamp without time zone
-- end-expected-error
SELECT date_part('wk', timestamp '2026-07-28 10:20:30');

-- ============================================================================
-- 7. Units are case-insensitive, and the error reports them lower-cased
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 2026-01-01 00:00:00
-- end-expected
SELECT date_trunc('YEAR', timestamp '2026-07-28 10:20:30')::text AS a;

-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT date_part('Hour', timestamp '2026-07-28 10:20:30') AS a;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "timezone_hour" not supported for type timestamp without time zone
-- end-expected-error
SELECT date_part('TimeZone_Hour', timestamp '2026-07-28 10:20:30');

-- ============================================================================
-- 8. The error names the type that could not answer
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "bogus" not recognized for type timestamp with time zone
-- end-expected-error
SELECT date_part('bogus', timestamptz '2026-07-28 10:20:30+00');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "bogus" not recognized for type interval
-- end-expected-error
SELECT date_part('bogus', interval '13 days');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "bogus" not recognized for type date
-- end-expected-error
SELECT extract('bogus' from date '2026-07-28');

-- date_trunc does not read the field-only units at all, so epoch is unknown to it
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "epoch" not recognized for type timestamp without time zone
-- end-expected-error
SELECT date_trunc('epoch', timestamp '2026-07-28 10:20:30');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "timezone" not supported for type timestamp without time zone
-- end-expected-error
SELECT date_trunc('timezone', timestamp '2026-07-28 10:20:30');

-- ============================================================================
-- 9. date_trunc resolves a date and a time through other functions
-- ============================================================================

-- there is no date_trunc(text, date): a date reaches the timestamptz form
-- begin-expected
-- columns: a
-- row: 2026-07-28 00:00:00+00
-- end-expected
SELECT date_trunc('day', date '2026-07-28')::text AS a;

-- begin-expected
-- columns: a
-- row: 2026-07-27 00:00:00+00
-- end-expected
SELECT date_trunc('week', date '2026-07-28')::text AS a;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "bogus" not recognized for type timestamp with time zone
-- end-expected-error
SELECT date_trunc('bogus', date '2026-07-28');

-- nor date_trunc(text, time): a time reaches the interval form
-- begin-expected
-- columns: a
-- row: 10:00:00
-- end-expected
SELECT date_trunc('hour', time '10:20:30.5')::text AS a;

-- begin-expected
-- columns: a
-- row: 10:20:00
-- end-expected
SELECT date_trunc('minute', time '10:20:30.5')::text AS a;

-- an interval has no calendar, so a year zeroes the whole thing
-- begin-expected
-- columns: a
-- row: 00:00:00
-- end-expected
SELECT date_trunc('year', time '10:20:30.5')::text AS a;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "week" not supported for type interval
-- end-expected-error
SELECT date_trunc('week', time '10:20:30.5');

-- and no form at all takes a timetz
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function date_trunc(unknown, time with time zone) does not exist
-- end-expected-error
SELECT date_trunc('hour', timetz '10:20:30+02');

-- ============================================================================
-- 10. extract() and date_part() disagree about a date
-- ============================================================================

-- extract() has its own entry point for date and refuses every sub-day unit
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "hour" not supported for type date
-- end-expected-error
SELECT extract(hour from date '2026-07-28');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "second" not supported for type date
-- end-expected-error
SELECT extract(second from date '2026-07-28');

-- date_part() has none, so the date reaches the timestamp code and answers zero
-- begin-expected
-- columns: h, s, ms
-- row: 0 | 0 | 0
-- end-expected
SELECT date_part('hour', date '2026-07-28') AS h,
       date_part('second', date '2026-07-28') AS s,
       date_part('microseconds', date '2026-07-28') AS ms;

-- begin-expected
-- columns: y, e
-- row: 2026 | 1785196800
-- end-expected
SELECT extract(year from date '2026-07-28') AS y,
       extract(epoch from date '2026-07-28') AS e;

-- ============================================================================
-- 11. An interval answers the larger units too
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT extract(quarter from interval '7 months') AS a;

-- begin-expected
-- columns: c, m, d
-- row: 20 | 2 | 200
-- end-expected
SELECT extract(century from interval '2001 years') AS c,
       extract(millennium from interval '2001 years') AS m,
       extract(decade from interval '2001 years') AS d;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT extract(week from interval '13 days 24 hours') AS a;

-- a negative interval counts away from zero in every field
-- begin-expected
-- columns: w, q5, q12
-- row: -1 | -2 | -1
-- end-expected
SELECT extract(week from interval '-13 days') AS w,
       extract(quarter from interval '-5 months') AS q5,
       extract(quarter from interval '-12 months') AS q12;

-- begin-expected
-- columns: us, ms
-- row: 30500000 | 30500.000
-- end-expected
SELECT extract(microseconds from interval '5 days 10:20:30.5') AS us,
       extract(milliseconds from interval '5 days 10:20:30.5') AS ms;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "dow" not supported for type interval
-- end-expected-error
SELECT extract(dow from interval '13 days');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "julian" not supported for type interval
-- end-expected-error
SELECT extract(julian from interval '13 days');

-- ============================================================================
-- 12. date_bin validates its stride
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 2026-07-28 10:15:00
-- end-expected
SELECT date_bin(interval '15 minutes', timestamp '2026-07-28 10:20:30',
                timestamp '2026-07-28 00:00:00')::text AS a;

-- a bin before the origin floors the same way
-- begin-expected
-- columns: a
-- row: 2026-07-28 10:15:00
-- end-expected
SELECT date_bin(interval '10 minutes', timestamp '2026-07-28 10:20:30',
                timestamp '2026-07-28 10:25:00')::text AS a;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: stride must be greater than zero
-- end-expected-error
SELECT date_bin(interval '-2 hours', timestamp '2026-07-28 10:20:30',
                timestamp '2026-07-28 00:00:00');

-- begin-expected-error
-- sqlstate: 22008
-- message-like: stride must be greater than zero
-- end-expected-error
SELECT date_bin(interval '0 hours', timestamp '2026-07-28 10:20:30',
                timestamp '2026-07-28 00:00:00');

-- a month is not a fixed number of microseconds, so there is no bin width
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: timestamps cannot be binned into intervals containing months or years
-- end-expected-error
SELECT date_bin(interval '1 mon 1 day', timestamp '2026-07-28 10:20:30',
                timestamp '2026-07-28 00:00:00');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: timestamps cannot be binned into intervals containing months or years
-- end-expected-error
SELECT date_bin(interval '1 year', timestamp '2026-07-28 10:20:30',
                timestamp '2026-07-28 00:00:00');

-- ============================================================================
-- 13. A NULL argument makes the whole call NULL
-- ============================================================================

-- begin-expected
-- columns: a, b, c, d
-- row: NULL | NULL | NULL | NULL
-- end-expected
SELECT date_part(NULL, timestamp '2026-07-28 10:00:00') AS a,
       date_trunc(NULL, timestamp '2026-07-28 10:00:00') AS b,
       date_part('hour', NULL::timestamp) AS c,
       extract(hour from NULL::time) AS d;

-- begin-expected
-- columns: a, b, c
-- row: NULL | NULL | NULL
-- end-expected
SELECT date_bin(interval '15 minutes', NULL, timestamp '2026-07-28 00:00:00') AS a,
       date_bin(NULL, timestamp '2026-07-28 10:20:30', timestamp '2026-07-28 00:00:00') AS b,
       date_bin(interval '15 minutes', timestamp '2026-07-28 10:20:30', NULL) AS c;

-- ============================================================================
-- 14. The session zone still decides a timestamptz's fields
-- ============================================================================

SET TimeZone = 'Pacific/Kiritimati';

-- begin-expected
-- columns: h, tz, tzh
-- row: 15 | 50400 | 14
-- end-expected
SELECT extract(hour from timestamptz '2026-07-28 01:00:00+00') AS h,
       extract(timezone from timestamptz '2026-07-28 01:00:00+00') AS tz,
       extract(timezone_hour from timestamptz '2026-07-28 01:00:00+00') AS tzh;

SET TimeZone = 'Pacific/Niue';

-- +14 and -11 are a full calendar day apart, so a zone-blind answer cannot pass both
-- begin-expected
-- columns: h, d, tz
-- row: 14 | 27 | -39600
-- end-expected
SELECT extract(hour from timestamptz '2026-07-28 01:00:00+00') AS h,
       extract(day from timestamptz '2026-07-28 01:00:00+00') AS d,
       extract(timezone from timestamptz '2026-07-28 01:00:00+00') AS tz;

-- the instant itself does not move with the zone
-- begin-expected
-- columns: e, tzh
-- row: 1785200400.000000 | 2
-- end-expected
SELECT extract(epoch from timestamptz '2026-07-28 01:00:00+00') AS e,
       extract(timezone_hour from timetz '10:00:00+02') AS tzh;

SET TimeZone = 'UTC';

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT extract(timezone from timestamptz '2026-07-28 01:00:00+00') AS a;

-- ============================================================================
-- 15. Infinite values answer only the fields that keep growing
-- ============================================================================

-- begin-expected
-- columns: a, b, c
-- row: Infinity | -Infinity | Infinity
-- end-expected
SELECT extract(year from timestamp 'infinity') AS a,
       extract(year from timestamp '-infinity') AS b,
       extract(epoch from timestamp 'infinity') AS c;

-- begin-expected
-- columns: a, b
-- row: Infinity | -Infinity
-- end-expected
SELECT extract(year from interval 'infinity') AS a,
       extract(day from interval '-infinity') AS b;

-- a cycling field says nothing about an infinite value
-- begin-expected
-- columns: a, b
-- row: NULL | NULL
-- end-expected
SELECT extract(day from timestamp 'infinity') AS a,
       extract(month from interval 'infinity') AS b;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "dow" not supported for type interval
-- end-expected-error
SELECT extract(dow from interval 'infinity');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "bogus" not recognized for type interval
-- end-expected-error
SELECT extract('bogus' from interval 'infinity');

-- ============================================================================
-- 16. Neighbours that must keep working
-- ============================================================================

-- begin-expected
-- columns: w, c, m, d
-- row: 2026-07-27 00:00:00 | 2001-01-01 00:00:00 | 2001-01-01 00:00:00 | 2020-01-01 00:00:00
-- end-expected
SELECT date_trunc('week', timestamp '2026-07-28 10:20:30')::text AS w,
       date_trunc('century', timestamp '2026-07-28 10:20:30')::text AS c,
       date_trunc('millennium', timestamp '2026-07-28 10:20:30')::text AS m,
       date_trunc('decade', timestamp '2026-07-28 10:20:30')::text AS d;

-- begin-expected
-- columns: a
-- row: 3 days 04:00:00
-- end-expected
SELECT date_trunc('hour', interval '3 days 4 hours 30 minutes')::text AS a;

-- begin-expected
-- columns: a, b
-- row: infinity | infinity
-- end-expected
SELECT date_trunc('day', interval 'infinity')::text AS a,
       date_trunc('day', timestamp 'infinity')::text AS b;

-- begin-expected
-- columns: y, c
-- row: -44 | -1
-- end-expected
SELECT extract(year from timestamp '0044-03-15 BC') AS y,
       extract(century from timestamp '0044-03-15 BC') AS c;

-- ============================================================================
-- 17. An interval's printed sign, and ISO 8601's alternative form
-- ============================================================================

-- only a field that directly follows a negative one carries an explicit plus
-- begin-expected
-- columns: a
-- row: -10 mons +3 days 04:05:06
-- end-expected
SELECT (interval '-10 mons 3 days 4:05:06')::text AS a;

-- begin-expected
-- columns: a
-- row: -1 years -10 mons +3 days 04:05:06
-- end-expected
SELECT (interval '-2 years 2 mons 3 days 4:05:06')::text AS a;

-- a negative day is the last signed field here, so the time part carries a plus
-- begin-expected
-- columns: a
-- row: 1 year -3 days +04:05:06
-- end-expected
SELECT (interval '1 year -3 days 4:05:06')::text AS a;

-- begin-expected
-- columns: a
-- row: 1 day -04:05:06
-- end-expected
SELECT (interval '1 day -4:05:06')::text AS a;

-- begin-expected
-- columns: a
-- row: 1 year 2 mons 3 days 04:05:06
-- end-expected
SELECT (interval 'P0001-02-03T04:05:06')::text AS a;

-- begin-expected
-- columns: a
-- row: 1 year 2 mons 3 days 04:05:06.5
-- end-expected
SELECT (interval 'P0001-02-03T04:05:06.5')::text AS a;

-- begin-expected
-- columns: a
-- row: 1 year 2 mons 3 days 04:05:06
-- end-expected
SELECT (interval 'P1Y2M3DT4H5M6S')::text AS a;
