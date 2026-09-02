-- source: investigation-2026-08.md
-- finding: 12
-- title: Subscripted UPDATE is rewritten by the parser into jsonb_set(col,'{i}',to_jsonb(v)). A jsonb path has no lower bound, no rectangular shape and no element type, 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_a1 (id int, a int[]);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_a1 VALUES (1,'{1,2,3}'),(2,NULL);
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_vf_a1 SET a[0] = 99 WHERE id = 1;
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_vf_a1 SET a[2] = 99 WHERE id = 2;
-- begin-expected
-- columns: id:int4 | a:_int4
-- row: 1 | [0:3]={99,1,2,3}
-- row: 2 | [2:2]={99}
-- rowcount: 2
-- end-expected
SELECT id, a FROM zz_vf_a1 ORDER BY id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_a3 (id int, m int[]);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_a3 VALUES (1,'{{1,2},{3,4}}');
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_vf_a3 SET m[1][1] = 99 WHERE id = 1;
-- begin-expected
-- columns: m:_int4
-- row: {{99,2},{3,4}}
-- rowcount: 1
-- end-expected
SELECT m FROM zz_vf_a3;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_a2 (id int, a int[]);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_a2 VALUES (3,'{1,2,3}');
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: source array too small
-- end-expected-error
UPDATE zz_vf_a2 SET a[2:3] = '{7}' WHERE id = 3;
-- begin-expected
-- columns: a:_int4
-- row: {1,2,3}
-- rowcount: 1
-- end-expected
SELECT a FROM zz_vf_a2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_js (b jsonb);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_js VALUES ('{}');
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_vf_js SET b['c'] = '5';
-- begin-expected
-- columns: b:jsonb | jsonb_typeof:text
-- row: {"c": 5} | number
-- rowcount: 1
-- end-expected
SELECT b, jsonb_typeof(b -> 'c') FROM zz_vf_js;
