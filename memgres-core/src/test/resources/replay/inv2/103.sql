-- source: investigation-2026-08.md
-- finding: 103
-- title: Unrelated singletons in this area
-- begin-expected
-- columns: age:interval
-- row: 43 years 9 mons 27 days
-- rowcount: 1
-- end-expected
SELECT age(timestamp '2001-04-10', timestamp '1957-06-13');
-- begin-expected
-- columns: pg_typeof:text
-- row: timestamp with time zone
-- rowcount: 1
-- end-expected
SELECT pg_typeof(current_timestamp(0))::text;
-- begin-expected
-- columns: timestamp:timestamp
-- row: 2001-01-01 04:05:06.79
-- rowcount: 1
-- end-expected
SELECT timestamp(2) '2001-01-01 04:05:06.789';
-- begin-expected
-- columns: time:time
-- row: 04:05:06.789
-- rowcount: 1
-- end-expected
SELECT time(3) '04:05:06.7891';
-- begin-expected
-- columns: pg_typeof:text
-- row: time with time zone
-- rowcount: 1
-- end-expected
SELECT pg_typeof(current_time)::text;
-- begin-expected
-- columns: text:text
-- row: 2001-09-28 03:00:00
-- rowcount: 1
-- end-expected
SELECT (date '2001-09-28' + time '03:00')::text;
-- begin-expected
-- columns: pg_typeof:text
-- row: timestamp without time zone
-- rowcount: 1
-- end-expected
SELECT pg_typeof(date '2001-09-28' + time '03:00')::text;
-- begin-expected
-- columns: make_timestamp:text
-- row: 0044-03-15 00:00:00 BC
-- rowcount: 1
-- end-expected
SELECT make_timestamp(-44, 3, 15, 0, 0, 0)::text;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date field value out of range: 0-01-01
-- end-expected-error
SELECT make_date(0,1,1)::text;
-- begin-expected
-- columns: make_time:text
-- row: 24:00:00
-- rowcount: 1
-- end-expected
SELECT make_time(24,0,0)::text;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: time zone "Nowhere/Bogus" not recognized
-- end-expected-error
SELECT make_timestamptz(2013, 7, 15, 8, 15, 23.5, 'Nowhere/Bogus');
-- begin-expected
-- ok: 0
-- end-expected
SET intervalstyle = 'sql_standard';
-- begin-expected
-- columns: text:text
-- row: +1-2 +3 +4:05:06
-- rowcount: 1
-- end-expected
SELECT (interval '1 year 2 mons 3 days 04:05:06')::text;
-- begin-expected
-- columns: text:text
-- row: -1 0:00:00
-- rowcount: 1
-- end-expected
SELECT '-1 day'::interval::text;
-- begin-expected
-- ok: 0
-- end-expected
SET intervalstyle = 'postgres_verbose';
-- begin-expected
-- columns: text:text
-- row: @ 1 year 2 mons 3 days 4 hours 5 mins 6 secs ago
-- rowcount: 1
-- end-expected
SELECT (interval '-1 year -2 mons -3 days -04:05:06')::text;
-- begin-expected
-- columns: slept:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT clock_timestamp() - now() > interval '1 second' AS slept FROM (SELECT pg_sleep_for('2 seconds')) t;
