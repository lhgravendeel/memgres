-- source: review-2026-08.md
-- finding: Root cause 6: operator resolution is by value shape, not by signature
-- area: Types, casts and coercion
-- title: Root cause 6: operator resolution is by value shape, not by signature
-- begin-expected-error
-- sqlstate: 42725
-- message-like: operator is not unique: unknown + unknown
-- end-expected-error
SELECT NULL + NULL;
-- begin-expected-error
-- sqlstate: 42725
-- message-like: operator is not unique: - unknown
-- end-expected-error
SELECT -'2';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer || integer
-- end-expected-error
SELECT 1 || 2 + 3;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: - text
-- end-expected-error
SELECT -1::text;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: date ~~ unknown
-- end-expected-error
SELECT '2020-01-01'::date LIKE '2020%';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: unknown ~~ integer
-- end-expected-error
SELECT 'abc' LIKE 5;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: text @> text
-- end-expected-error
SELECT u @> v FROM unnest(ARRAY['{a,b}']) AS u, unnest(ARRAY['{a}']) AS v;
-- begin-expected-error
-- sqlstate: 42725
-- message-like: operator is not unique: unknown @> unknown
-- end-expected-error
SELECT '{a,b}' @> '{a}';
-- begin-expected
-- columns: pg_typeof:regtype
-- row: money
-- rowcount: 1
-- end-expected
SELECT pg_typeof('1.00'::money + '1.00'::money);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: money + integer
-- end-expected-error
SELECT '1.00'::money + 1;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: - money
-- end-expected-error
SELECT -('1.00'::money);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: money = integer
-- end-expected-error
SELECT NULL::money = 1;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: @ date
-- end-expected-error
SELECT @ (date '2020-01-01');
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: bit << bigint
-- end-expected-error
SELECT B'1101' << 4294967296;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT B'101' = '101';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_ct AS (x int, y text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c (v zz_ct);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_c VALUES (ROW(1,'a')::zz_ct);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT v = ROW(1,'a')::zz_ct FROM zz_c;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_nullfn(a text, b text) RETURNS text AS $$ SELECT NULL::text; $$ LANGUAGE sql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR # (leftarg = text, rightarg = text, function = zz_nullfn);
-- begin-expected
-- columns: ?column?:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT 'a'::text # 'b'::text;
