-- source: investigation-2026-08.md
-- finding: 133
-- title: MERGE is implemented on a code path of its own that skips what every other DML path does: it reads targetTable.getRows() rather than collectAllPartitionTables, 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ff (k int, v text) PARTITION BY RANGE (k);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ff_1 PARTITION OF zz_vf_ff FOR VALUES FROM (1) TO (10);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ffs (k int, v text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_ff VALUES (5, 'old');
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_ffs VALUES (5, 'new');
-- begin-expected
-- ok: 1
-- end-expected
MERGE INTO zz_vf_ff t USING zz_vf_ffs u ON t.k = u.k WHEN MATCHED THEN UPDATE SET v = u.v;
-- begin-expected
-- columns: k:int4 | v:text
-- row: 5 | new
-- rowcount: 1
-- end-expected
SELECT k, v FROM zz_vf_ff ORDER BY k;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_st (id int primary key, owner text, n int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_st VALUES (1,'zz_vf_role',10),(2,'other',20);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf_role LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT, INSERT, UPDATE, DELETE ON zz_vf_st TO zz_vf_role;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_st ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_vf_sel ON zz_vf_st FOR SELECT USING (true);
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_vf_upd ON zz_vf_st FOR UPDATE USING (owner = current_user);
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_vf_role;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: target row violates row-level security policy (USING expression) for table "zz_vf_st"
-- end-expected-error
MERGE INTO zz_vf_st t USING (VALUES (1),(2)) AS v(id) ON t.id = v.id WHEN MATCHED THEN UPDATE SET n = 99;
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- columns: id:int4 | n:int4
-- row: 1 | 10
-- row: 2 | 20
-- rowcount: 2
-- end-expected
SELECT id, n FROM zz_vf_st ORDER BY id;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_mt (id int PRIMARY KEY, v int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ms (id int, v int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_ms VALUES (1,111);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: INSERT has more target columns than expressions
-- end-expected-error
MERGE INTO zz_vf_mt t USING zz_vf_ms s ON t.id = s.id + 5000
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id + 700);
