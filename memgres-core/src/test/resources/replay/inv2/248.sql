-- source: investigation-2026-08.md
-- finding: 248
-- title: The write paths walk a table's partitions but not its inheritance children: collectAllPartitionTables recurses through getPartitions() only, while the read path
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_par (id int, v int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_chi () INHERITS (zz_par);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_par VALUES (1,1);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_chi VALUES (2,2);
-- begin-expected
-- ok: 2
-- end-expected
UPDATE zz_par SET v = v + 100;
-- begin-expected
-- columns: id:int4 | v:int4
-- row: 1 | 101
-- row: 2 | 102
-- rowcount: 2
-- end-expected
SELECT id, v FROM zz_par ORDER BY id;
-- begin-expected
-- ok: 1
-- end-expected
DELETE FROM zz_par WHERE id = 2;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_chi;
