-- source: investigation-2026-08.md
-- finding: 338
-- title: The search path is treated as a list to validate rather than as the authority on what a bare name means. TypeNamespace.resolve falls back to find(), which scans
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_shadow (src text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_shadow VALUES ('permanent');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TEMP TABLE zz_shadow (src text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_shadow VALUES ('temp');
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = public, pg_temp;
-- begin-expected
-- columns: src:text
-- row: permanent
-- rowcount: 1
-- end-expected
SELECT src FROM zz_shadow;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_qv;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_qv.t (a int);
-- begin-expected
-- columns: pg_table_is_visible:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT pg_table_is_visible('zz_qv.t'::regclass);
-- begin-expected
-- ok: 0
-- end-expected
SET search_path TO zz_nosuch, public;
-- begin-expected
-- columns: ?column?:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT 1 OPERATOR(pg_catalog.+) 2;
-- begin-expected
-- columns: ?column?:text
-- row: ab
-- rowcount: 1
-- end-expected
SELECT 'a' OPERATOR(pg_catalog.||) 'b';
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_n1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_n1.zz_e1 AS ENUM ('a');
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_e1" does not exist
-- end-expected-error
SELECT 'a'::zz_e1;
