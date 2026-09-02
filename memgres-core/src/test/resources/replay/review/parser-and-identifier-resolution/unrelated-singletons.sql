-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: Parser and identifier resolution
-- title: Unrelated singletons
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_m1 (a int, b int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_m1 VALUES (1,10),(2,20);
-- begin-expected
-- columns: a:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT (zz_m1).a   FROM zz_m1 ORDER BY 1;
-- begin-expected
-- columns: a:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT (zz_m1.*).a FROM zz_m1 ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_q11 (a int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_q11 VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_qf(a int) RETURNS bigint LANGUAGE sql
  AS $$ SELECT count(*) FROM zz_q11 WHERE zz_q11.a = $1 $$;
-- begin-expected
-- columns: zz_qf:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT zz_qf(1);
-- begin-expected
-- columns: column1:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
VALUES (1),(2) LIMIT 1+1;
-- begin-expected
-- columns: column1:int4
-- row: 3
-- rowcount: 1
-- end-expected
VALUES (1),(2),(3) OFFSET 1+1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_q1 (a int);
-- begin-expected
-- columns: text:text
-- row: zz_q1
-- rowcount: 1
-- end-expected
SELECT 'ZZ_Q1'::regclass::text;
-- begin-expected
-- columns: float8:float8
-- row: 1.5
-- rowcount: 1
-- end-expected
SELECT double precision '1.5';
-- begin-expected
-- columns: varchar:varchar
-- row: 1.5
-- rowcount: 1
-- end-expected
SELECT character varying '1.5';
-- begin-expected
-- columns: varbit:varbit
-- row: 101
-- rowcount: 1
-- end-expected
SELECT bit varying '101';
-- begin-expected
-- columns: pg_collation_for:text
-- row: "C"
-- rowcount: 1
-- end-expected
SELECT collation for ('a' COLLATE "C");
-- begin-expected
-- columns: pg_collation_for:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT collation for ('a');
-- begin-expected
-- ok: 0
-- end-expected
SET SCHEMA 'public';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "$"
-- end-expected-error
SET search_path = $user, public;
-- begin-expected
-- columns: search_path:text
-- row: public
-- rowcount: 1
-- end-expected
SHOW search_path;
-- begin-expected-error
-- sqlstate: 42939
-- message-like: unacceptable schema name "pg_zz_ns"
-- end-expected-error
CREATE SCHEMA pg_zz_ns;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "EXISTS"
-- end-expected-error
ALTER SCHEMA IF EXISTS zz_nosuch RENAME TO zz_x;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t1 (id int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "AS"
-- end-expected-error
SELECT a.id FROM zz_t1 a AS b ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_q1" already exists
-- end-expected-error
CREATE TABLE zz_q1 (a int);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cross-database references are not implemented: "nosuchdb.public.zz_q1"
-- end-expected-error
SELECT * FROM nosuchdb.public.zz_q1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_s (a int, b int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_s VALUES (1,10);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "x" does not exist
-- end-expected-error
SELECT a AS x FROM zz_s HAVING x > 0;
