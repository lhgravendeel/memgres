-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: Joins, CTEs, subqueries — and the DDL that came with them
-- title: Unrelated singletons
-- begin-expected
-- columns: generate_series:int4
-- row: 1
-- row: 2
-- row: 3
-- rowcount: 3
-- end-expected
SELECT generate_series(1, 20000000) LIMIT 3;
-- begin-expected
-- columns: g:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT g FROM (SELECT generate_series(1, 20000000) AS g) t LIMIT 1;
-- begin-expected
-- ok: 0
-- end-expected
SET statement_timeout = '1s';
-- begin-expected-error
-- sqlstate: 57014
-- message-like: canceling statement due to statement timeout
-- end-expected-error
SELECT count(*) FROM (SELECT pg_sleep(3) FROM generate_series(1,5)) t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "9999999999"
-- end-expected-error
CREATE FUNCTION zz_g1(x float(9999999999)) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "9999999999"
-- end-expected-error
CREATE FUNCTION zz_g1(x float(9999999999)) RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_rls (id int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_rls VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_rls ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_pol ON zz_rls FOR SELECT USING (id = 1);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_rls FORCE ROW LEVEL SECURITY;
-- begin-expected
-- columns: id:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT id FROM zz_rls ORDER BY id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_role LOGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_st" does not exist
-- end-expected-error
GRANT SELECT, INSERT, UPDATE, DELETE ON zz_st TO zz_role;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_role;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_st" does not exist
-- end-expected-error
CREATE TRIGGER zz_trg BEFORE INSERT ON zz_st FOR EACH ROW EXECUTE FUNCTION zz_tf();
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r3;
-- begin-expected
-- ok: 0
-- end-expected
ALTER ROLE zz_r3 SET search_path = public;
-- begin-expected
-- ok: 0
-- end-expected
ALTER ROLE zz_r3 RESET search_path;
-- begin-expected
-- columns: rolconfig:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT rolconfig::text FROM pg_roles WHERE rolname='zz_r3';
-- begin-expected
-- ok: 0
-- end-expected
ALTER ROLE zz_r3 SET search_path = 'a', 'b';
-- begin-expected
-- columns: rolconfig:text
-- row: {"search_path=a, b"}
-- rowcount: 1
-- end-expected
SELECT rolconfig::text FROM pg_roles WHERE rolname='zz_r3';
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_sc;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_sc.zz_x (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_x (b int);
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = zz_sc, public;
-- begin-expected
-- columns: pg_table_is_visible:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT pg_table_is_visible('public.zz_x'::regclass);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pa (id int PRIMARY KEY);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ch (p int REFERENCES zz_pa(id));
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_pa, zz_ch;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_la" does not exist
-- end-expected-error
SELECT j.id FROM zz_la JOIN zz_lb USING (id) AS j ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "g"
-- end-expected-error
SELECT count(*) FROM zz_f f, LATERAL zz_g g WHERE g.fid = f.id;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_z1" does not exist
-- end-expected-error
SELECT a FROM zz_z1 ORDER BY a USING +;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_z1" does not exist
-- end-expected-error
TABLE ONLY zz_z1 ORDER BY a;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_z1" does not exist
-- end-expected-error
SELECT count(*) FROM zz_z1 * a;
-- begin-expected
-- columns: 
-- row: 
-- rowcount: 1
-- end-expected
SELECT UNION SELECT;
-- begin-expected
-- columns: 
-- row: 
-- row: 
-- rowcount: 2
-- end-expected
SELECT UNION ALL SELECT;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: VALUES lists must all be the same length
-- end-expected-error
VALUES (1,2),(3);
-- begin-expected
-- columns: pg_typeof:text
-- row: text
-- rowcount: 1
-- end-expected
SELECT pg_typeof(column1)::text FROM (VALUES (NULL),(NULL)) v LIMIT 1;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = boolean
-- end-expected-error
SELECT 1 = 1 IN (1,2);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: boolean = integer
-- end-expected-error
SELECT (1 = 1) IN (1,2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_q3 (id int, "ID" text);
-- begin-expected
-- columns: text:text
-- row: integer
-- rowcount: 1
-- end-expected
SELECT 'pg_catalog.int4'::regtype::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_ts;
-- begin-expected
-- ok: 0
-- end-expected
CREATE COLLATION zz_ts.zz_co (LOCALE = 'C');
-- begin-expected
-- columns: ?column?:text
-- row: a
-- rowcount: 1
-- end-expected
SELECT 'a' COLLATE zz_ts.zz_co;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_fp (s text) PARTITION BY RANGE (s COLLATE "C");
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_fp2 (s text) PARTITION BY RANGE (s text_pattern_ops);
-- begin-expected
-- columns: format_type:text
-- row: bpchar
-- rowcount: 1
-- end-expected
SELECT format_type('bpchar'::regtype, -1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_nc AS (a int, b text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_k10() RETURNS text AS $$ declare v zz_nc := row(7,'q'); begin v.a := 8; return v.a::text; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_k10:text
-- row: 8
-- rowcount: 1
-- end-expected
SELECT zz_k10();
-- begin-expected
-- ok: 0
-- end-expected
DO $$ declare v text; begin perform 1/0; exception when others then get stacked diagnostics v = pg_context; end $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_p1() LANGUAGE plpgsql AS $$ BEGIN SAVEPOINT sp1; END $$;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type char cannot exceed 10485760
-- end-expected-error
CREATE FUNCTION zz_charbig() RETURNS int AS $$ DECLARE v char(20000000) := 'a'; BEGIN RETURN length(v); END $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_charbig() does not exist
-- end-expected-error
SELECT zz_charbig();
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in DEFAULT expression
-- end-expected-error
CREATE DOMAIN zz_dom1 AS int DEFAULT (SELECT 1);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: operator family "zz_nofam" does not exist for access method "btree"
-- end-expected-error
ALTER OPERATOR FAMILY zz_nofam USING btree ADD OPERATOR 1 = (int4, int4);
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR FAMILY zz_fam1 USING btree;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_norole" does not exist
-- end-expected-error
ALTER OPERATOR FAMILY zz_fam1 USING btree OWNER TO zz_norole;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_noschema" does not exist
-- end-expected-error
ALTER OPERATOR FAMILY zz_fam1 USING btree SET SCHEMA zz_noschema;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "zz_nosuch_coll" for encoding "UTF8" does not exist
-- end-expected-error
DROP COLLATION zz_nosuch_coll;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: conversion "zz_nosuch_conv" does not exist
-- end-expected-error
DROP CONVERSION zz_nosuch_conv;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: channel name too long
-- end-expected-error
SELECT pg_notify(repeat('a', 100), 'x');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: payload string too long
-- end-expected-error
SELECT pg_notify('zz_ch', repeat(U&'\00E9', 5000));
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (SELECT 1 AS c1, 2 AS c2 /* ... 1665 columns ... */) t;
