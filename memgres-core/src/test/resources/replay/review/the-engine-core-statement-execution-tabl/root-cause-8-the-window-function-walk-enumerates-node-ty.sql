-- source: review-2026-08.md
-- finding: Root cause 8: the window-function walk enumerates node types by hand and has holes
-- area: The engine core: statement execution, tables, windows, joins and deparsing
-- title: Root cause 8: the window-function walk enumerates node types by hand and has holes
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_w (id int, v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_w VALUES (1,10),(2,20),(3,30);
-- begin-expected
-- columns: id:int4 | inlist:bool
-- row: 1 | t
-- row: 2 | t
-- row: 3 | f
-- rowcount: 3
-- end-expected
SELECT id, row_number() OVER (ORDER BY id) IN (1,2) AS inlist FROM zz_vf2_w ORDER BY id;
-- begin-expected
-- columns: id:int4 | btw:bool
-- row: 1 | t
-- row: 2 | t
-- row: 3 | f
-- rowcount: 3
-- end-expected
SELECT id, row_number() OVER (ORDER BY id) BETWEEN 1 AND 2 AS btw FROM zz_vf2_w ORDER BY id;
-- begin-expected
-- columns: id:int4 | ist:bool
-- row: 1 | t
-- row: 2 | f
-- row: 3 | f
-- rowcount: 3
-- end-expected
SELECT id, (row_number() OVER (ORDER BY id) = 1) IS TRUE AS ist FROM zz_vf2_w ORDER BY id;
-- begin-expected
-- columns: id:int4 | arr:_int8
-- row: 1 | {1}
-- row: 2 | {2}
-- row: 3 | {3}
-- rowcount: 3
-- end-expected
SELECT id, ARRAY[row_number() OVER (ORDER BY id)] AS arr FROM zz_vf2_w ORDER BY id;
