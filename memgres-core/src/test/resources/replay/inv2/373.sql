-- source: investigation-2026-08.md
-- finding: 373
-- title: Dependencies, immutability and column references are decided by searching SQL text rather than the parse tree: a \b<name>\b regex over a function body, a %ROWTY
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_c1 (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf2_fn() RETURNS text LANGUAGE sql AS $$ SELECT 'zz_vf2_c1'::text $$;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf2_c1 CASCADE;
-- begin-expected
-- columns: zz_vf2_fn:text
-- row: zz_vf2_c1
-- rowcount: 1
-- end-expected
SELECT zz_vf2_fn();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_x (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf2_xf() RETURNS int LANGUAGE plpgsql AS $$ DECLARE v zz_vf2_x.a%TYPE; BEGIN v := 1; RETURN v; END $$;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf2_x;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_gg (current_date_col int, g int GENERATED ALWAYS AS (current_date_col + 1) STORED);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ii (localtime_col int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_ix ON zz_vf2_ii ((localtime_col + 1));
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_dd (a int DEFAULT length('current_timestamp'));
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_dd2 (a int DEFAULT length('snow(1)'));
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_gu (s text, g uuid GENERATED ALWAYS AS (s::uuid) STORED);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_gi (a int, g smallint GENERATED ALWAYS AS (a::int2) STORED);
