-- source: investigation-2026-08.md
-- finding: 67
-- title: Structure is decided by scanning text rather than the parse tree: unanchored substring tests over a generated column's expression and over a view AST's toString
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_lit() RETURNS text AS $$
DECLARE v text;
BEGIN
  SELECT ' into me ' INTO v;
  RETURN v;
END $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_lit:text
-- row:  into me 
-- rowcount: 1
-- end-expected
SELECT zz_vf_lit();
-- begin-expected
-- ok: 0
-- end-expected
DROP FUNCTION zz_vf_lit();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_g1 (id int, d int, total int GENERATED ALWAYS AS (id * 2) STORED);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_g1 DROP COLUMN d;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf_g1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_t3 (id int, d varchar(10), total int GENERATED ALWAYS AS (id * 2) STORED);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_t3 ALTER COLUMN d TYPE varchar(30);
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf_t3;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_t1 (a int, e int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_v1 AS SELECT a FROM zz_vf_t1;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_t1 DROP COLUMN e;
-- begin-expected
-- ok: 0
-- end-expected
DROP VIEW zz_vf_v1;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf_t1;
