-- source: review-2026-08.md
-- finding: Root cause 10: search_path is ignored by the name-resolution helpers
-- area: System catalogs, information_schema and security
-- title: Root cause 10: search_path is ignored by the name-resolution helpers
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_s3;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_s3.zz_off (id int);
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = public;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT to_regclass('zz_off') IS NULL;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION public.zz_add(integer,integer) RETURNS integer AS 'SELECT $1+$2' LANGUAGE sql IMMUTABLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR public.@#@ (LEFTARG=integer, RIGHTARG=integer, FUNCTION = zz_add);
-- begin-expected
-- ok: 0
-- end-expected
SET search_path TO pg_catalog;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer @#@ integer
-- end-expected-error
SELECT 1 @#@ 2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_s2;
-- begin-expected
-- ok: 0
-- end-expected
SET search_path TO zz_s2, public;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "pg_catalog"
-- end-expected-error
SELECT pg_catalog.current_schema;
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = 'zz_no_such_schema';
-- begin-expected
-- columns: current_schema:name
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT current_schema();
