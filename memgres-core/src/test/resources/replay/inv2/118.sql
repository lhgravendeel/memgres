-- source: investigation-2026-08.md
-- finding: 118
-- title: Unrelated singletons in this area
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
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_a (v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_a VALUES (10),(20),(30);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT sum(v) OVER (ORDER BY v ROWS 99999999999999999999 PRECEDING) FROM zz_vf_a ORDER BY 1;
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
-- begin-expected-error
-- sqlstate: 42723
-- message-like: function "zz_vf_cadd" already exists with same argument types
-- end-expected-error
CREATE FUNCTION zz_vf_cadd(int,int) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE($1,0)+COALESCE($2,0) $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE AGGREGATE zz_vf_cag2(BASETYPE = int, SFUNC = zz_vf_cadd, STYPE = int);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function percentile_cont(numeric) does not exist
-- end-expected-error
SELECT percentile_cont(0.5) FROM (VALUES (1)) t(v);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function mode() does not exist
-- end-expected-error
SELECT mode() FROM (VALUES (1)) t(v);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: percentile value 1.5 is not between 0 and 1
-- end-expected-error
SELECT percentile_disc(1.5) WITHIN GROUP (ORDER BY v) FROM (VALUES (1)) t(v);
