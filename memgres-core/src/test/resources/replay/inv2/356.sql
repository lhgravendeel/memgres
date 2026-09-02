-- source: investigation-2026-08.md
-- finding: 356
-- title: SKIP LOCKED decides a row is locked when it is not in the named relation's own row list: `if (!b.table().getRows().contains(b.row())) { lockable = false; break;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_sp (id int) PARTITION BY RANGE (id);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_sp1 PARTITION OF zz_vf2_sp FOR VALUES FROM (0) TO (10);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_sp VALUES (1),(2),(3);
-- begin-expected
-- columns: id:int4
-- row: 1
-- row: 2
-- row: 3
-- rowcount: 3
-- end-expected
SELECT id FROM zz_vf2_sp ORDER BY id FOR UPDATE SKIP LOCKED;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ip (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ic1 () INHERITS (zz_vf2_ip);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_ip VALUES (1);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_ic1 VALUES (2);
-- begin-expected
-- columns: id:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT id FROM zz_vf2_ip ORDER BY id FOR UPDATE SKIP LOCKED;
