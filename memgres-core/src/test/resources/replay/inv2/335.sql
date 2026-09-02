-- source: investigation-2026-08.md
-- finding: 335
-- title: A schema qualifier is either thrown away by the parser or carried whole into a lookup that expects a bare name; no path round-trips schema.name. DdlParser's mul
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_s2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_keep2 (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_gone2 (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_s2.zz_keep2 (id int);
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_gone2, zz_s2.zz_keep2;
-- begin-expected
-- columns: table_schema:name
-- row: public
-- rowcount: 1
-- end-expected
SELECT table_schema FROM information_schema.tables WHERE table_name = 'zz_keep2' ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_qs;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_qs2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_qs.who() RETURNS text LANGUAGE sql AS $$ SELECT 'one'::text $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_qs2.who() RETURNS text LANGUAGE sql AS $$ SELECT 'two'::text $$;
-- begin-expected-error
-- sqlstate: 42723
-- message-like: function who() already exists in schema "zz_qs2"
-- end-expected-error
ALTER FUNCTION zz_qs.who() SET SCHEMA zz_qs2;
-- begin-expected
-- columns: who:text
-- row: two
-- rowcount: 1
-- end-expected
SELECT zz_qs2.who();
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION public.zz_add(integer,integer) RETURNS integer AS 'SELECT $1+$2' LANGUAGE sql IMMUTABLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR public.@#@ (LEFTARG=integer, RIGHTARG=integer, FUNCTION = public.zz_add);
-- begin-expected
-- columns: ?column?:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT 1 @#@ 2;
