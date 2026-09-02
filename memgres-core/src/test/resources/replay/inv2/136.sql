-- source: investigation-2026-08.md
-- finding: 136
-- title: DDL undo entries are keyed by the object's bare name, so undoing a create deletes every object that shares the name and never restores what was replaced; the sa
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_sf(a int) RETURNS text AS $$ SELECT 'int:' || a $$ LANGUAGE sql;
-- begin-expected
-- columns: zz_vf_sf:text
-- row: int:1
-- rowcount: 1
-- end-expected
SELECT zz_vf_sf(1);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_sf(a text) RETURNS text AS $$ SELECT 'text:' || a $$ LANGUAGE sql;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: zz_vf_sf:text
-- row: int:2
-- rowcount: 1
-- end-expected
SELECT zz_vf_sf(2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_sg() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
-- begin-expected
-- columns: zz_vf_sg:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT zz_vf_sg();
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
CREATE OR REPLACE FUNCTION zz_vf_sg() RETURNS int AS $$ SELECT 2 $$ LANGUAGE sql;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: zz_vf_sg:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT zz_vf_sg();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TEMP TABLE zz_vf_tt (x int) ON COMMIT DELETE ROWS;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf_tt;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TEMP TABLE zz_vf_tt (x int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_tt VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_tt;
