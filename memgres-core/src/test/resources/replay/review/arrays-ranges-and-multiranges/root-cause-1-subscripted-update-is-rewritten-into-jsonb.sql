-- source: review-2026-08.md
-- finding: Root cause 1: subscripted UPDATE is rewritten into `jsonb_set`
-- area: Arrays, ranges and multiranges
-- title: Root cause 1: subscripted UPDATE is rewritten into `jsonb_set`
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
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_a2" does not exist
-- end-expected-error
UPDATE zz_vf_a2 SET a[2:3] = '{7}' WHERE id = 3;
-- a was {1,2,3}
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_js" does not exist
-- end-expected-error
UPDATE zz_vf_js SET b['c'] = '5';
-- b was '{}'::jsonb
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_js" does not exist
-- end-expected-error
SELECT b, jsonb_typeof(b -> 'c') FROM zz_vf_js;
