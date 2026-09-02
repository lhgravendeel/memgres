-- source: review-2026-08.md
-- finding: Root cause 3: pg_description is re-synthesised from the comment map by a switch that knows nine object kinds, and pg_shdescription is hardcoded empty
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Root cause 3: pg_description is re-synthesised from the comment map by a switch that knows nine object kinds, and pg_shdescription is hardcoded empty
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c (a int, b int, c int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_c DROP COLUMN b;
-- begin-expected
-- ok: 0
-- end-expected
COMMENT ON COLUMN zz_c.c IS 'ccc';
-- begin-expected
-- columns: objsubid:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT objsubid FROM pg_description WHERE objoid='zz_c'::regclass;
-- begin-expected
-- columns: at3:text | at2:text
-- row: ccc | NULL
-- rowcount: 1
-- end-expected
SELECT col_description('zz_c'::regclass, 3) AS at3, col_description('zz_c'::regclass, 2) AS at2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_add(int, int) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT $1 + $2 $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR ###@ (LEFTARG = int, RIGHTARG = int, FUNCTION = zz_add);
-- begin-expected
-- ok: 0
-- end-expected
COMMENT ON OPERATOR ###@ (int, int) IS 'op comment';
-- begin-expected
-- columns: d:text
-- row: op comment
-- rowcount: 1
-- end-expected
SELECT obj_description(oid, 'pg_operator') AS d FROM pg_operator WHERE oprname = '###@';
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r NOLOGIN;
-- begin-expected
-- ok: 0
-- end-expected
COMMENT ON ROLE zz_r IS 'rolec';
-- begin-expected
-- columns: count:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_shdescription WHERE description='rolec';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a int, b int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE STATISTICS zz_st ON a, b FROM zz_t;
-- begin-expected
-- ok: 0
-- end-expected
COMMENT ON STATISTICS zz_st IS 'stc';
-- begin-expected
-- columns: count:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_description WHERE description='stc';
