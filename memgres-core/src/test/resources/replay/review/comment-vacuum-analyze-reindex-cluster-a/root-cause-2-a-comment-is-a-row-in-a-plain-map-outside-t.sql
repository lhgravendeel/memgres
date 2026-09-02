-- source: review-2026-08.md
-- finding: Root cause 2: a comment is a row in a plain map, outside the undo log and keyed by a lower-cased name
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Root cause 2: a comment is a row in a plain map, outside the undo log and keyed by a lower-cased name
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c (id int);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
COMMENT ON TABLE zz_c IS 'rolled back';
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: undone:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT obj_description('zz_c'::regclass) IS NULL AS undone;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE "zz_Cap" (id int);
-- begin-expected
-- ok: 0
-- end-expected
COMMENT ON TABLE "zz_Cap" IS 'kept';
-- begin-expected
-- columns: d:text | n:int4
-- row: kept | 1
-- rowcount: 1
-- end-expected
SELECT obj_description('"zz_Cap"'::regclass) AS d,
       (SELECT count(*)::int FROM pg_description WHERE description='kept') AS n;
