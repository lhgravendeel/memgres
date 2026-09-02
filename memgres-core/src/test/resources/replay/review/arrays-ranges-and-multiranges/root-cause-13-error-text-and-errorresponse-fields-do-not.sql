-- source: review-2026-08.md
-- finding: Root cause 13: error text and ErrorResponse fields do not match PG
-- area: Arrays, ranges and multiranges
-- title: Root cause 13: error text and ErrorResponse fields do not match PG
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT 'x'::json;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value "32768" is out of range for type smallint
-- end-expected-error
SELECT '32768'::int2;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: smallint out of range
-- end-expected-error
SELECT 1e30::numeric::int2;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: smallint out of range
-- end-expected-error
SELECT 'NaN'::float8::int2;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: "a" is not a valid binary digit
-- end-expected-error
SELECT 'abc'::bit(3);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT (ARRAY[1,2,3])['x'];
-- begin-expected-error
-- sqlstate: 42601
-- message-like: invalid range bound flags
-- end-expected-error
SELECT int4range(1,5,'x');
-- begin-expected-error
-- sqlstate: 42P18
-- message-like: cannot determine type of empty array
-- end-expected-error
SELECT ARRAY[];
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: cannot accumulate empty arrays
-- end-expected-error
SELECT array_agg(a) FROM (VALUES ('{}'::int[]),('{1}'::int[])) v(a);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "statement_timeout": "2147483648ms"
-- end-expected-error
SET statement_timeout = '2147483648ms';
-- begin-expected-error
-- sqlstate: 54000
-- message-like: number of array dimensions exceeds the maximum allowed (6)
-- end-expected-error
SELECT (repeat('{', 1000) || '1' || repeat('}', 1000))::int[];
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_g5() RETURNS text AS $$ declare a int[] := array[1,2,3]; begin a[null] := 9; return a::text; end $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 22004
-- message-like: array subscript in assignment must not be null
-- end-expected-error
SELECT zz_vf_g5();
