-- source: review-2026-08.md
-- finding: DDL undo is a name-keyed delete, and session registrations outlive their object
-- area: DML, MERGE, partitioning, rules and the COPY/extended-protocol surface
-- title: DDL undo is a name-keyed delete, and session registrations outlive their object
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_sf(a int) RETURNS text AS $$ SELECT 'int:' || a $$ LANGUAGE sql;
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
-- no ON COMMIT clause
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
