-- ============================================================================
-- -- An instant, a length, and the two ends of time.
-- --
-- -- The date/time types hold two values no instant answers to, and every operation over them
-- -- has to say what it does with those: an age across one of them is endless, a bin holding one
-- -- is that value, and a count of days between two dates cannot reach one at all. What is left
-- -- is the reading and the writing: a literal in each of the spellings the type takes, a
-- -- displacement no further than sixteen hours out, a fraction rounded to the microsecond the
-- -- type holds rather than cut there, a precision written on the type itself, and a length of
-- -- time that the aggregates and the percentiles know how to total and to halve.
--
-- ============================================================================

-- ============================================================================
-- 1. The ends of time are values, and the arithmetic keeps them
-- ============================================================================
SET TimeZone = 'UTC';
-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT date_bin(interval '1 day', timestamp 'infinity', timestamp '2020-01-01')::text AS a;
-- begin-expected
-- columns: a
-- row: -infinity
-- end-expected
SELECT date_bin(interval '1 day', timestamp '-infinity', timestamp '2020-01-01')::text AS a;
-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT date_bin(interval '1 day', timestamptz 'infinity', timestamptz '2020-01-01')::text AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: origin out of range
-- end-expected-error
SELECT date_bin(interval '1 day', timestamp '2020-01-01', timestamp 'infinity') AS a;
-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT age(timestamp 'infinity', timestamp '2020-01-01')::text AS a;
-- begin-expected
-- columns: a
-- row: -infinity
-- end-expected
SELECT age(timestamp '-infinity', timestamp '2020-01-01')::text AS a;
-- begin-expected
-- columns: a
-- row: -infinity
-- end-expected
SELECT age(timestamp '2020-01-01', timestamp 'infinity')::text AS a;
-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT age(timestamp 'infinity', timestamp '-infinity')::text AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT age(timestamp 'infinity', timestamp 'infinity') AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: cannot subtract infinite dates
-- end-expected-error
SELECT (date 'infinity' - date '2020-01-01') AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: cannot subtract infinite dates
-- end-expected-error
SELECT (date '2020-01-01' - date 'infinity') AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: cannot subtract infinite dates
-- end-expected-error
SELECT (date 'infinity' - date 'infinity') AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: cannot subtract infinite dates
-- end-expected-error
SELECT (date 'infinity' - date '-infinity') AS a;
-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT (timestamp 'infinity' - timestamp '2020-01-01')::text AS a;
-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT (timestamp 'infinity' - timestamp '-infinity')::text AS a;
-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT (date 'infinity' - 1)::text AS a;
-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT (date 'infinity' + interval '1 day')::text AS a;
-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT date_trunc('day', timestamp 'infinity')::text AS a;
-- begin-expected
-- columns: a
-- row: Infinity
-- end-expected
SELECT extract(epoch from timestamp 'infinity')::text AS a;
-- begin-expected
-- columns: a
-- row: Infinity
-- end-expected
SELECT extract(year from timestamp 'infinity')::text AS a;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid value "infi" for "YYYY"
-- end-expected-error
SELECT to_timestamp('infinity','YYYY')::text AS a;
-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT justify_days(interval 'infinity')::text AS a;
-- begin-expected
-- columns: a
-- row: f
-- end-expected
SELECT isfinite(timestamp 'infinity') AS a;
-- ============================================================================
-- 2. An age is subtracted field by field, borrowing the month it started in
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 43 years 9 mons 27 days
-- end-expected
SELECT age(timestamp '2001-04-10', timestamp '1957-06-13')::text AS a;
-- begin-expected
-- columns: a
-- row: -43 years -9 mons -27 days
-- end-expected
SELECT age(timestamp '1957-06-13', timestamp '2001-04-10')::text AS a;
-- begin-expected
-- columns: a
-- row: 1 mon 2 days
-- end-expected
SELECT age(timestamp '2020-03-31', timestamp '2020-02-29')::text AS a;
-- begin-expected
-- columns: a
-- row: 1 mon 30 days
-- end-expected
SELECT age(timestamp '2020-01-31', timestamp '2019-12-01')::text AS a;
-- begin-expected
-- columns: a
-- row: 10 mons 1 day
-- end-expected
SELECT age(timestamp '2021-01-01', timestamp '2020-02-29')::text AS a;
-- begin-expected
-- columns: a
-- row: 1 mon 1 day
-- end-expected
SELECT age(timestamp '2020-02-01', timestamp '2019-12-31')::text AS a;
-- begin-expected
-- columns: a
-- row: 1 mon 1 day
-- end-expected
SELECT age(timestamp '2020-05-31', timestamp '2020-04-30')::text AS a;
-- begin-expected
-- columns: a
-- row: 1 mon 19:29:44.5
-- end-expected
SELECT age(timestamp '2020-03-01 05:00', timestamp '2020-01-31 09:30:15.5')::text AS a;
-- begin-expected
-- columns: a
-- row: 00:00:00.000002
-- end-expected
SELECT age(timestamp '2020-07-01 00:00:00.000001', timestamp '2020-06-30 23:59:59.999999')::text AS a;
-- begin-expected
-- columns: a
-- row: 3 years 6 mons 16 days
-- end-expected
SELECT age(timestamp '0001-01-01 BC', timestamp '0005-06-15 BC')::text AS a;
-- begin-expected
-- columns: a
-- row: 1 mon 2 days
-- end-expected
SELECT age(date '2020-03-31', date '2020-02-29')::text AS a;
-- ============================================================================
-- 3. A literal is read in every spelling the type takes
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT date '06/15/2020' AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT date 'June 15, 2020' AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT date '15 Jun 2020' AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT date 'Jun 15 2020' AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT date '2020-Jun-15' AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT date '20200615' AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT date '06.15.2020' AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range: "15.06.2020"
-- end-expected-error
SELECT date '15.06.2020' AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range: "2020-15-06"
-- end-expected-error
SELECT date '2020-15-06' AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range: "2020-02-30"
-- end-expected-error
SELECT date '2020-02-30' AS a;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type date: "2020-006-005"
-- end-expected-error
SELECT date '2020-006-005' AS a;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type date: "2020-06-15 garbage"
-- end-expected-error
SELECT date '2020-06-15 garbage' AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT date '2020-06-15 12:00:00' AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT date '2020-06-15 UTC' AS a;
-- begin-expected
-- columns: a
-- row: 2001-01-01
-- end-expected
SELECT date '2001-01-01+02' AS a;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type date: "2001-01-01-05"
-- end-expected-error
SELECT date '2001-01-01-05' AS a;
-- begin-expected
-- columns: a
-- row: 01:02:03
-- end-expected
SELECT time '1:2:3' AS a;
-- begin-expected
-- columns: a
-- row: 01:02:03
-- end-expected
SELECT time '010203' AS a;
-- begin-expected
-- columns: a
-- row: 01:02:00
-- end-expected
SELECT time '0102' AS a;
-- begin-expected
-- columns: a
-- row: 01:02:00
-- end-expected
SELECT time '1:02 AM' AS a;
-- begin-expected
-- columns: a
-- row: 13:02:00
-- end-expected
SELECT time '1:02 PM' AS a;
-- begin-expected
-- columns: a
-- row: 00:00:00
-- end-expected
SELECT time '12:00 AM' AS a;
-- begin-expected
-- columns: a
-- row: 12:00:00
-- end-expected
SELECT time '12:00 PM' AS a;
-- begin-expected
-- columns: a
-- row: 00:02:00
-- end-expected
SELECT time '0:02 AM' AS a;
-- begin-expected
-- columns: a
-- row: 01:02:00
-- end-expected
SELECT time '1:02am' AS a;
-- begin-expected
-- columns: a
-- row: 13:02:03
-- end-expected
SELECT time '1:02:03pm' AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range: "13:02 PM"
-- end-expected-error
SELECT time '13:02 PM' AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: time zone "a.m." not recognized
-- end-expected-error
SELECT time '1:02 a.m.' AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 12:00:00
-- end-expected
SELECT timestamp '20200615T120000' AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 12:00:00
-- end-expected
SELECT timestamp 'Jun 15 12:00:00 2020' AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 06:29:45+00
-- end-expected
SELECT timestamptz '2020-06-15 12:00:00+05:30:15' AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-14 20:01:00+00
-- end-expected
SELECT timestamptz '2020-06-15 12:00:00+15:59' AS a;
-- begin-expected-error
-- sqlstate: 22009
-- message-like: time zone displacement out of range: "2020-06-15 12:00:00+16"
-- end-expected-error
SELECT timestamptz '2020-06-15 12:00:00+16' AS a;
-- begin-expected-error
-- sqlstate: 22009
-- message-like: time zone displacement out of range: "2020-06-15 12:00:00-16"
-- end-expected-error
SELECT timestamptz '2020-06-15 12:00:00-16' AS a;
-- begin-expected-error
-- sqlstate: 22009
-- message-like: time zone displacement out of range: "2020-06-15 12:00:00+15:60"
-- end-expected-error
SELECT timestamptz '2020-06-15 12:00:00+15:60' AS a;
-- begin-expected-error
-- sqlstate: 22009
-- message-like: time zone displacement out of range: "12:00:00+16"
-- end-expected-error
SELECT timetz '12:00:00+16' AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 12:00:01+00
-- end-expected
SELECT timestamptz '2020-01-01 12:00:00.9999995+00' AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 12:00:00
-- end-expected
SELECT timestamp '2020-01-01 12:00:00.0000005' AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 12:00:00.000002
-- end-expected
SELECT timestamp '2020-01-01 12:00:00.0000015' AS a;
-- begin-expected
-- columns: a
-- row: 12:00:01
-- end-expected
SELECT time '12:00:00.9999995' AS a;
-- ============================================================================
-- 4. A type's own precision is written where the type is
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 2020-01-01 12:00:00.99
-- end-expected
SELECT timestamp(2) '2020-01-01 12:00:00.987654' AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 12:00:01
-- end-expected
SELECT timestamp(0) '2020-01-01 12:00:00.987654' AS a;
-- begin-expected
-- columns: a
-- row: 12:00:00.99
-- end-expected
SELECT time(2) '12:00:00.987654' AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 12:00:00.988+00
-- end-expected
SELECT timestamptz(3) '2020-01-01 12:00:00.987654+00' AS a;
-- begin-expected
-- columns: a
-- row: 12:00:01+00
-- end-expected
SELECT timetz(1) '12:00:00.987654+00' AS a;
-- begin-expected
-- columns: a
-- row: 00:00:01.99
-- end-expected
SELECT interval(2) '1.987654 seconds' AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 00:00:00
-- end-expected
SELECT timestamp(7) '2020-01-01' AS a;
-- begin-expected
-- columns: a
-- row: timestamp with time zone
-- end-expected
SELECT pg_typeof(current_timestamp(0))::text AS a;
-- begin-expected
-- columns: a
-- row: timestamp without time zone
-- end-expected
SELECT pg_typeof(localtimestamp(2))::text AS a;
-- begin-expected
-- columns: a
-- row: time with time zone
-- end-expected
SELECT pg_typeof(current_time)::text AS a;
-- begin-expected
-- columns: a
-- row: time with time zone
-- end-expected
SELECT pg_typeof(current_time(2))::text AS a;
-- begin-expected
-- columns: a
-- row: time without time zone
-- end-expected
SELECT pg_typeof(localtime)::text AS a;
-- begin-expected
-- columns: a
-- row: time without time zone
-- end-expected
SELECT pg_typeof(localtime(2))::text AS a;
-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT date_trunc('sec', current_timestamp(0)) = date_trunc('sec', current_timestamp(0)) AS a;
-- ============================================================================
-- 5. A date and a time of day are the two halves of a timestamp
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 2020-01-01 12:00:00
-- end-expected
SELECT (date '2020-01-01' + time '12:00:00')::text AS a;
-- begin-expected
-- columns: a
-- row: timestamp without time zone
-- end-expected
SELECT pg_typeof(date '2020-01-01' + time '12:00:00')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 12:00:00
-- end-expected
SELECT (time '12:00:00' + date '2020-01-01')::text AS a;
-- begin-expected
-- columns: a
-- row: 2019-12-31 12:00:00
-- end-expected
SELECT (date '2020-01-01' - time '12:00:00')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 12:00:00+00
-- end-expected
SELECT (date '2020-01-01' + timetz '12:00:00+00')::text AS a;
-- begin-expected
-- columns: a
-- row: timestamp with time zone
-- end-expected
SELECT pg_typeof(date '2020-01-01' + timetz '12:00:00+00')::text AS a;
SET TimeZone = 'America/New_York';
-- begin-expected
-- columns: a
-- row: 12:00:00-04
-- end-expected
SELECT timetz '12:00:00' AS a;
SET TimeZone = 'UTC';
-- ============================================================================
-- 6. A year is made of fields, and there is no year nought
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 0001-01-01 00:00:00 BC
-- end-expected
SELECT make_timestamp(-1, 1, 1, 0, 0, 0)::text AS a;
-- begin-expected
-- columns: a
-- row: 4713-01-01 00:00:00 BC
-- end-expected
SELECT make_timestamp(-4713, 1, 1, 0, 0, 0)::text AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date field value out of range: 0-01-01
-- end-expected-error
SELECT make_timestamp(0, 1, 1, 0, 0, 0) AS a;
-- begin-expected
-- columns: a
-- row: 0001-01-01 BC
-- end-expected
SELECT make_date(-1, 1, 1)::text AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date field value out of range: 0-01-01
-- end-expected-error
SELECT make_date(0, 1, 1) AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: time zone "Nowhere/Nothing" not recognized
-- end-expected-error
SELECT make_timestamptz(2020, 1, 1, 0, 0, 0, 'Nowhere/Nothing') AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 00:00:00+00
-- end-expected
SELECT make_timestamptz(2020, 1, 1, 0, 0, 0, 'UTC')::text AS a;
-- ============================================================================
-- 7. A length of time is a value the aggregates know
-- ============================================================================
CREATE TABLE zz_span(i interval, t time);
INSERT INTO zz_span VALUES (interval '1 day', time '01:00'), (interval '3 days', time '03:00');
-- begin-expected
-- columns: a
-- row: 4 days
-- end-expected
SELECT sum(i)::text AS a FROM zz_span;
-- begin-expected
-- columns: a
-- row: 2 days
-- end-expected
SELECT avg(i)::text AS a FROM zz_span;
-- begin-expected
-- columns: a
-- row: 04:00:00
-- end-expected
SELECT sum(t)::text AS a FROM zz_span;
-- begin-expected
-- columns: a
-- row: 02:00:00
-- end-expected
SELECT avg(t)::text AS a FROM zz_span;
-- begin-expected
-- columns: a
-- row: interval
-- end-expected
SELECT pg_typeof(sum(i))::text AS a FROM zz_span;
-- begin-expected
-- columns: a
-- row: interval
-- end-expected
SELECT pg_typeof(avg(t))::text AS a FROM zz_span;
-- begin-expected
-- columns: a
-- row: 2 days
-- end-expected
SELECT (percentile_cont(0.5) WITHIN GROUP (ORDER BY i))::text AS a FROM zz_span;
-- begin-expected
-- columns: a
-- row: 02:00:00
-- end-expected
SELECT (percentile_cont(0.5) WITHIN GROUP (ORDER BY t))::text AS a FROM zz_span;
-- begin-expected
-- columns: a
-- row: 4 days
-- end-expected
SELECT sum(DISTINCT i)::text AS a FROM zz_span;
INSERT INTO zz_span VALUES (interval '1 mon 5 days 6 hours', NULL);
-- begin-expected
-- columns: a
-- row: 1 mon 9 days 06:00:00
-- end-expected
SELECT sum(i)::text AS a FROM zz_span;
-- begin-expected
-- columns: a
-- row: 13 days 02:00:00
-- end-expected
SELECT avg(i)::text AS a FROM zz_span;
-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT sum(i) IS NULL AS a FROM zz_span WHERE false;
DELETE FROM zz_span;
INSERT INTO zz_span VALUES (interval '1 mon', NULL), (interval '2 mons', NULL), (interval '1 day', NULL);
-- begin-expected
-- columns: a
-- row: 1 mon 08:00:00
-- end-expected
SELECT avg(i)::text AS a FROM zz_span;
DROP TABLE zz_span;
-- ============================================================================
-- 8. The declared result type is what the catalogue says it is
-- ============================================================================
-- begin-expected
-- columns: a
-- row: timestamp without time zone
-- end-expected
SELECT pg_typeof(date_bin(interval '1 day', timestamp '2020-01-01', timestamp '2020-01-01'))::text AS a;
-- begin-expected
-- columns: a
-- row: interval
-- end-expected
SELECT pg_typeof(age(timestamp '2020-01-01', timestamp '2019-01-01'))::text AS a;
-- begin-expected
-- columns: a
-- row: interval
-- end-expected
SELECT pg_typeof(justify_days(interval '1 day'))::text AS a;
-- begin-expected
-- columns: a
-- row: interval
-- end-expected
SELECT pg_typeof(justify_hours(interval '1 day'))::text AS a;
-- begin-expected
-- columns: a
-- row: interval
-- end-expected
SELECT pg_typeof(justify_interval(interval '1 day'))::text AS a;
-- begin-expected
-- columns: a
-- row: timestamp without time zone
-- end-expected
SELECT pg_typeof(make_timestamp(2020,1,1,0,0,0))::text AS a;
-- begin-expected
-- columns: a
-- row: -29 days -23:00:00
-- end-expected
SELECT justify_interval(interval '-1 mon 1 hour')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 04:00:00+00
-- end-expected
SELECT date_trunc('day', timestamptz '2020-06-15 12:00:00+00', 'America/New_York')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-14 14:00:00+00
-- end-expected
SELECT date_trunc('week', timestamptz '2020-06-15 12:00:00+00', 'Australia/Sydney')::text AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: time zone "Nowhere/Nothing" not recognized
-- end-expected-error
SELECT date_trunc('day', timestamptz '2020-06-15 12:00:00+00', 'Nowhere/Nothing') AS a;
-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT extract(decade from date '0001-01-01 BC')::text AS a;
-- begin-expected
-- columns: a
-- row: -1
-- end-expected
SELECT extract(century from date '0001-01-01 BC')::text AS a;
