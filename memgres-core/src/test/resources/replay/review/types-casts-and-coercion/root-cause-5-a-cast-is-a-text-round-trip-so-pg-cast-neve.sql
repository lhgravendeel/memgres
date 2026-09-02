-- source: review-2026-08.md
-- finding: Root cause 5: a cast is a text round-trip, so `pg_cast` never decides anything
-- area: Types, casts and coercion
-- title: Root cause 5: a cast is a text round-trip, so `pg_cast` never decides anything
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type boolean to bigint
-- end-expected-error
SELECT true::int8;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type integer to json
-- end-expected-error
SELECT 1::int::json;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type point to money
-- end-expected-error
SELECT '(1,1)'::point::money;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type date to interval
-- end-expected-error
SELECT '2020-01-01'::date::interval;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type integer to date
-- end-expected-error
SELECT 1::int::date;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type date to integer
-- end-expected-error
SELECT DATE '2020-01-01'::int;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type interval to integer
-- end-expected-error
SELECT interval '1 day'::int;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type money to integer
-- end-expected-error
SELECT '1234'::money::int;
-- begin-expected
-- columns: polygon:polygon
-- row: ((0,0),(0,1),(1,1),(1,0))
-- rowcount: 1
-- end-expected
SELECT CAST(CAST('(0,0),(1,1)' AS box) AS polygon);
-- begin-expected
-- columns: point:point
-- row: (0.5,0.5)
-- rowcount: 1
-- end-expected
SELECT CAST(CAST('(0,0),(1,1)' AS box) AS point);
-- begin-expected
-- columns: time:time
-- row: 02:03:04
-- rowcount: 1
-- end-expected
SELECT interval '1 day 02:03:04'::time;
-- begin-expected
-- columns: time:time
-- row: 01:03:04
-- rowcount: 1
-- end-expected
SELECT interval '25:03:04'::time;
-- begin-expected
-- columns: timetz:timetz
-- row: 20:38:40.5+00
-- rowcount: 1
-- end-expected
SELECT timestamptz '2001-02-16 20:38:40.5+00'::timetz;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type timestamp without time zone to time with time zone
-- end-expected-error
SELECT timestamp '2001-02-16 20:38:40.5'::timetz;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type tsvector to tsquery
-- end-expected-error
SELECT 'a'::tsvector::tsquery;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: tsvector @@ text
-- end-expected-error
SELECT 'fox:1'::tsvector @@ 'fox'::text;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function max(tsvector) does not exist
-- end-expected-error
SELECT max(v)::text FROM (VALUES ('a'::tsvector),('b'::tsvector)) t(v);
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type smallint to uuid
-- end-expected-error
SELECT 1::int2::uuid;
-- begin-expected
-- columns: bpchar:bpchar
-- row: a
-- rowcount: 1
-- end-expected
SELECT 'abcdef'::char;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_cc2 AS (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_cto(int) RETURNS zz_cc2 LANGUAGE sql IMMUTABLE AS $$ SELECT ROW($1*2)::zz_cc2 $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE CAST (int AS zz_cc2) WITH FUNCTION zz_cto(int) AS ASSIGNMENT;
-- begin-expected
-- columns: a:int4
-- row: 10
-- rowcount: 1
-- end-expected
SELECT (5::zz_cc2).a;
-- begin-expected
-- columns: castcontext:char
-- row: a
-- rowcount: 1
-- end-expected
SELECT castcontext FROM pg_cast WHERE castsource='int4'::regtype AND casttarget='zz_cc2'::regtype;
