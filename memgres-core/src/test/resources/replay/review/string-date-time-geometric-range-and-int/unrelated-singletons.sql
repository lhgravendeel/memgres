-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: String, date/time, geometric, range and interval implementations
-- title: Unrelated singletons
-- begin-expected
-- columns: string_to_array:_text
-- row: {a,NULL,c}
-- rowcount: 1
-- end-expected
SELECT string_to_array('abc', NULL, 'b');
-- begin-expected
-- columns: string_to_array:_text
-- row: {a,NULL,c}
-- rowcount: 1
-- end-expected
SELECT string_to_array('a.b.c', '.', 'b');
-- begin-expected
-- columns: extract:numeric
-- row: -1
-- rowcount: 1
-- end-expected
SELECT extract(decade from date '0010-01-01 BC');
-- begin-expected
-- columns: extract:numeric
-- row: -10
-- rowcount: 1
-- end-expected
SELECT extract(decade from date '0100-01-01 BC');
-- begin-expected
-- columns: date_part:float8
-- row: -1
-- rowcount: 1
-- end-expected
SELECT date_part('decade', date '0010-01-01 BC');
-- begin-expected
-- ok: 0
-- end-expected
SET TIME ZONE 'UTC';
-- begin-expected
-- columns: pg_typeof:text
-- row: timestamp with time zone
-- rowcount: 1
-- end-expected
SELECT pg_typeof(date_trunc('day', TIMESTAMP '2020-05-15 10:00:00', 'UTC'))::text;
-- begin-expected
-- columns: date_trunc:text
-- row: 2020-05-14 15:00:00+00
-- rowcount: 1
-- end-expected
SELECT date_trunc('day', TIMESTAMP '2020-05-15 10:00:00', 'Asia/Tokyo')::text;
-- begin-expected
-- columns: substring:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT substring('abcdef', '2', '3');
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(DISTINCT r) FROM (VALUES ('[1,2)'::numrange), ('[1.0,2.0)'::numrange)) v(r);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_r (r numrange);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_r VALUES ('[1,2)'), ('[1.0,2.0)'), ('[1.000,2.000)');
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (SELECT r FROM zz_vf2_r GROUP BY r) g;
-- begin-expected
-- ok: 0
-- end-expected
SET bytea_output = 'escape';
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: COPY commands are only supported using the CopyManager API.
-- end-expected-error
COPY (SELECT '\x0102'::bytea) TO STDOUT;
-- begin-expected
-- ok: 0
-- end-expected
SET IntervalStyle = 'sql_standard';
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: COPY commands are only supported using the CopyManager API.
-- end-expected-error
COPY (SELECT interval '1 year 2 mons') TO STDOUT;
-- begin-expected
-- ok: 0
-- end-expected
SET extra_float_digits = 0;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: COPY commands are only supported using the CopyManager API.
-- end-expected-error
COPY (SELECT 0.1::float8 + 0.2::float8) TO STDOUT;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: must request at least 2 points
-- end-expected-error
SELECT polygon(0, '<(0,0),1>'::circle);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: must request at least 2 points
-- end-expected-error
SELECT polygon(1, '<(0,0),1>'::circle);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: must request at least 2 points
-- end-expected-error
SELECT polygon(-3, '<(0,0),1>'::circle);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "-"
-- end-expected-error
SELECT '1.5 seconds'::interval(-1);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
SELECT interval '1.5 seconds' (-1);
-- begin-expected
-- columns: btrim:text
-- row: abc
-- rowcount: 1
-- end-expected
SELECT trim('xabcx', 'x');
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "hour"
-- end-expected-error
SELECT interval '5' hour to hour;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "hour"
-- end-expected-error
SELECT interval '5' minute to hour;
