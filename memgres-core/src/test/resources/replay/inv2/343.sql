-- source: investigation-2026-08.md
-- finding: 343
-- title: A relation reference is matched by its bare name, with no schema and no scope: AstRelationRenamer.matches returns true whenever either side's schema is null, an
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pp (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_pp VALUES (7);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TEMP TABLE zz_vf2_pp (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_tv AS SELECT a FROM public.zz_vf2_pp;
-- begin-expected
-- columns: in_public:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT (table_schema = 'public') AS in_public FROM information_schema.views WHERE table_name='zz_vf2_tv';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_base (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_base VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_cv AS WITH zz_vf2_base AS (SELECT 42 AS a) SELECT a FROM zz_vf2_base;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf2_base RENAME TO zz_vf2_base2;
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_vf2_base2 SET a = 99;
-- begin-expected
-- columns: a:int4
-- row: 42
-- rowcount: 1
-- end-expected
SELECT a FROM zz_vf2_cv;
