-- source: investigation-2026-08.md
-- finding: 16
-- title: TypeCoercion.compare() has no array branch and its compareBound() narrows numeric range bounds with Long.compare(longValue()). The element-wise comparator array
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_c1 (a int[]);
-- begin-expected
-- ok: 5
-- end-expected
INSERT INTO zz_vf_c1 VALUES ('{1,2}'),('{1}'),('{}'),('{1,2,3}'),('{2}');
-- begin-expected
-- columns: min:_int4 | max:_int4
-- row: {} | {2}
-- rowcount: 1
-- end-expected
SELECT min(a), max(a) FROM zz_vf_c1;
-- begin-expected
-- columns: a:_int4
-- row: {}
-- row: {1}
-- row: {1,2}
-- row: {1,2,3}
-- row: {2}
-- rowcount: 5
-- end-expected
SELECT a FROM zz_vf_c1 GROUP BY a ORDER BY a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_c3 (id int, a text[]);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_c3 VALUES (1, ARRAY['a,b']), (2, ARRAY['a','a']), (3, ARRAY['b']);
-- begin-expected
-- columns: id:int4 | a:_text
-- row: 2 | {a,a}
-- row: 1 | {"a,b"}
-- row: 3 | {b}
-- rowcount: 3
-- end-expected
SELECT id, a FROM zz_vf_c3 ORDER BY a, id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_nr (id int, r numrange);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_nr VALUES (1, numrange(1.5,3.0)), (2, numrange(1.2,3.0)), (3, numrange(1.9,3.0));
-- begin-expected
-- columns: id:int4
-- row: 2
-- row: 1
-- row: 3
-- rowcount: 3
-- end-expected
SELECT id FROM zz_vf_nr ORDER BY r, id;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT numrange(1.5,3.0) < numrange(1.9,3.0);
