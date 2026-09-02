-- source: investigation-2026-08.md
-- finding: 291
-- title: A window function's name is matched with its schema prefix still attached, so every schema-qualified window call falls through both the ranking switch and the e
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_m (id int);
-- begin-expected
-- ok: 4
-- end-expected
INSERT INTO zz_vf2_m VALUES (1),(2),(3),(4);
-- begin-expected
-- columns: id:int4 | count:int8
-- row: 1 | 0
-- row: 2 | 0
-- row: 3 | 1
-- row: 4 | 2
-- rowcount: 4
-- end-expected
SELECT id, pg_catalog.count(*) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) FROM zz_vf2_m ORDER BY id;
-- begin-expected
-- columns: id:int4 | row_number:int8
-- row: 1 | 1
-- row: 2 | 2
-- row: 3 | 3
-- row: 4 | 4
-- rowcount: 4
-- end-expected
SELECT id, pg_catalog.row_number() OVER (ORDER BY id) FROM zz_vf2_m ORDER BY id;
-- begin-expected
-- columns: id:int4 | rank:int8
-- row: 1 | 1
-- row: 2 | 2
-- row: 3 | 3
-- row: 4 | 4
-- rowcount: 4
-- end-expected
SELECT id, pg_catalog.rank() OVER (ORDER BY id) FROM zz_vf2_m ORDER BY id;
