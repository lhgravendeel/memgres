-- source: review-2026-08.md
-- finding: Root cause 9: raw Java exceptions escape as XX000
-- area: Types, casts and coercion
-- title: Root cause 9: raw Java exceptions escape as XX000
-- begin-expected-error
-- sqlstate: 2201B
-- message-like: invalid regular expression: parentheses () not balanced
-- end-expected-error
SELECT 'abc' SIMILAR TO 'a(';
-- begin-expected-error
-- sqlstate: 2201B
-- message-like: invalid regular expression: brackets [] not balanced
-- end-expected-error
SELECT 'abc' SIMILAR TO 'a[';
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'a{b}' SIMILAR TO 'a{b}';
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'abc' SIMILAR TO 'a\bc';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: trailing junk after numeric literal at or near "1e"
-- end-expected-error
SELECT 1e;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value overflows numeric format
-- end-expected-error
SELECT 1e99999999999999999999;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range: "J999999999999999999999999"
-- end-expected-error
SELECT 'J999999999999999999999999'::date;
-- begin-expected
-- columns: float4:float4
-- row: Infinity
-- rowcount: 1
-- end-expected
SELECT 'inf'::float(24);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f1fn() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: COST must be positive
-- end-expected-error
ALTER FUNCTION zz_f1fn() COST -1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "abc"
-- end-expected-error
ALTER FUNCTION zz_f1fn() COST abc;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
CREATE FUNCTION zz_numbad() RETURNS numeric AS $$
DECLARE v numeric(5,abc) := 1;
BEGIN RETURN v; END $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_numbad() does not exist
-- end-expected-error
SELECT zz_numbad();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_gen (a int, g int GENERATED ALWAYS AS (a * 2) STORED);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_gen (a) VALUES (1);
-- begin-expected-error
-- sqlstate: 428C9
-- message-like: column "g" can only be updated to DEFAULT
-- end-expected-error
UPDATE zz_gen SET g = a + 1;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM generate_series(NULL::int, 10);
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM generate_series(NULL::numeric, 10::numeric, 1);
