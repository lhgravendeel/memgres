-- source: review-2026-08.md
-- finding: Root cause 7: an assignment subscript is one token, not an expression
-- area: The parsers
-- title: Root cause 7: an assignment subscript is one token, not an expression
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_arr (id int, a int[]);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_arr VALUES (1, ARRAY[10,20,30]);
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_vf2_arr SET a[1+1] = 99 WHERE id = 1;
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_vf2_arr SET a[id] = 77 WHERE id = 1;
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_vf2_arr SET a[1:1+1] = ARRAY[7,8] WHERE id = 1;
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_vf2_arr SET a[(SELECT 3)] = 55 WHERE id = 1;
-- begin-expected
-- columns: a:_int4
-- row: {7,8,55}
-- rowcount: 1
-- end-expected
SELECT a FROM zz_vf2_arr;
