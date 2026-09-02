-- source: investigation-2026-08.md
-- finding: 378
-- title: collectWindowFunctions and containsWindowFunction enumerate node types by hand and omit InExpr, BetweenExpr, IsBooleanExpr and ArrayExpr, so the query never tak
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_w (id int, v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_w VALUES (1,10),(2,20),(3,30);
-- begin-expected
-- columns: id:int4 | ?column?:bool
-- row: 1 | t
-- row: 2 | t
-- row: 3 | f
-- rowcount: 3
-- end-expected
SELECT id, row_number() OVER (ORDER BY id) IN (1,2) FROM zz_vf2_w ORDER BY id;
-- begin-expected
-- columns: id:int4 | array:_int8
-- row: 1 | {1}
-- row: 2 | {2}
-- row: 3 | {3}
-- rowcount: 3
-- end-expected
SELECT id, ARRAY[row_number() OVER (ORDER BY id)] FROM zz_vf2_w ORDER BY id;
