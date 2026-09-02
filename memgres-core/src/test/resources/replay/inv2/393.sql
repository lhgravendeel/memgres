-- source: investigation-2026-08.md
-- finding: 393
-- title: Unrelated singletons in this area
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_rd (id int);
-- begin-expected
-- columns: a:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT a FROM (VALUES (1)) v(a);
-- begin-expected
-- columns: generate_series:int4
-- row: 1
-- row: 2
-- row: 3
-- rowcount: 3
-- end-expected
SELECT * FROM generate_series(1,3);
-- begin-expected
-- columns: id:int4
-- rowcount: 0
-- end-expected
SELECT id FROM zz_vf_rd UNION ALL SELECT id FROM zz_vf_rd;
-- begin-expected
-- columns: id:int4
-- row: 99
-- rowcount: 1
-- end-expected
INSERT INTO zz_vf_rd VALUES (99) RETURNING id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pc (id int);
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf_pc1 AS SELECT id FROM zz_vf_pc WHERE id = $1;
-- begin-expected
-- columns: statement:text | parameter_types:text
-- row: PREPARE zz_vf_pc1 AS SELECT id FROM zz_vf_pc WHERE id = $1 | {integer}
-- rowcount: 1
-- end-expected
SELECT statement, parameter_types::text FROM pg_prepared_statements WHERE name = 'zz_vf_pc1';
