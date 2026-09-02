-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: Aggregates, window functions and grouping
-- title: Unrelated singletons
-- begin-expected
-- columns: lag:int4
-- row: NULL
-- row: NULL
-- row: NULL
-- rowcount: 3
-- end-expected
SELECT lag(v, v) OVER (ORDER BY v) FROM (VALUES (10),(20),(30)) t(v) ORDER BY 1;
-- begin-expected
-- columns: lead:int4
-- row: NULL
-- row: NULL
-- row: NULL
-- rowcount: 3
-- end-expected
SELECT lead(v, v) OVER (ORDER BY v) FROM (VALUES (10),(20),(30)) t(v) ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: CASE/WHEN could not convert type integer[] to text[]
-- end-expected-error
SELECT CASE WHEN false THEN ARRAY[1] ELSE ARRAY['a'] END;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "a"
-- end-expected-error
SELECT CASE 1 WHEN 'a' THEN 1 ELSE 2 END;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: text = integer
-- end-expected-error
SELECT CASE 'a' WHEN 1 THEN 1 ELSE 2 END;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "a"
-- end-expected-error
SELECT CASE WHEN 1=1 THEN 'a' ELSE (SELECT 1/0) END;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: - text
-- end-expected-error
SELECT -('abc'::text);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: - boolean
-- end-expected-error
SELECT -(true);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: - date
-- end-expected-error
SELECT -(date '2020-01-01');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_alt (a text);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in transform expression
-- end-expected-error
ALTER TABLE zz_vf_alt ALTER COLUMN a TYPE int USING (SELECT 1);
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in transform expressions
-- end-expected-error
ALTER TABLE zz_vf_alt ALTER COLUMN a TYPE int USING count(*);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_cadd(int,int) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE($1,0)+COALESCE($2,0) $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE AGGREGATE zz_vf_cag(int) (SFUNC = zz_vf_cadd, STYPE = int);
-- begin-expected
-- columns: text:text
-- row: zz_vf_cag(integer)
-- rowcount: 1
-- end-expected
SELECT 'zz_vf_cag(int)'::regprocedure::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE AGGREGATE zz_vf_cag2(BASETYPE = int, SFUNC = zz_vf_cadd, STYPE = int);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT sum(v) OVER (ORDER BY v ROWS 99999999999999999999 PRECEDING)
  FROM (VALUES (10),(20),(30)) t(v) ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function percentile_cont(numeric) does not exist
-- end-expected-error
SELECT percentile_cont(0.5) FROM (VALUES (1)) t(v);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: percentile value 1.5 is not between 0 and 1
-- end-expected-error
SELECT percentile_disc(1.5) WITHIN GROUP (ORDER BY v) FROM (VALUES (1)) t(v);
