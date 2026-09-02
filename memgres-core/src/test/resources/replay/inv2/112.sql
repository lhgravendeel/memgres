-- source: investigation-2026-08.md
-- finding: 112
-- title: Every name resolver except the base-table one compares with equalsIgnoreCase — views, enum types, functions, constraints, table aliases and WINDOW names — so a 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_cc (a int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_cc VALUES (1),(2),(3);
-- begin-expected
-- columns: a:int4 | count:int8 | count:int8
-- row: 1 | 3 | 1
-- row: 2 | 3 | 1
-- row: 3 | 3 | 1
-- rowcount: 3
-- end-expected
SELECT a, count(*) OVER "W", count(*) OVER "w" FROM zz_vf_cc
WINDOW "W" AS (), "w" AS (PARTITION BY a) ORDER BY a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW "ZzVfView" AS SELECT 1 AS a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE "ZzVfEnum" AS ENUM ('Red');
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION "ZzVfFn"() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf_cc" already exists
-- end-expected-error
CREATE TABLE zz_vf_cc (a int CONSTRAINT "ZzCk" CHECK (a > 0));
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_cc VALUES (1),(2),(3);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zzvfview" does not exist
-- end-expected-error
SELECT count(*) FROM zzvfview;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zzvfenum" does not exist
-- end-expected-error
SELECT 'Red'::zzvfenum;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zzvffn() does not exist
-- end-expected-error
SELECT zzvffn();
-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "zzck" of relation "zz_vf_cc" does not exist
-- end-expected-error
ALTER TABLE zz_vf_cc DROP CONSTRAINT zzck;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "X"
-- end-expected-error
SELECT "X".a FROM zz_vf_cc x ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "q"
-- end-expected-error
SELECT q.a FROM zz_vf_cc AS "Q" ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: window "W" does not exist
-- end-expected-error
SELECT sum(a) OVER "W" FROM zz_vf_cc WINDOW w AS (ORDER BY a) ORDER BY 1;
