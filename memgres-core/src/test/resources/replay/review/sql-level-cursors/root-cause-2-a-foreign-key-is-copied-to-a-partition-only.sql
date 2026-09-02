-- source: review-2026-08.md
-- finding: Root cause 2: a foreign key is copied to a partition only if it is a PRIMARY KEY or a UNIQUE constraint
-- area: SQL-level cursors
-- title: Root cause 2: a foreign key is copied to a partition only if it is a PRIMARY KEY or a UNIQUE constraint
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ref (id int PRIMARY KEY);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p (k int NOT NULL, refid int REFERENCES zz_ref(id)) PARTITION BY RANGE (k);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pa PARTITION OF zz_p FOR VALUES FROM (0) TO (100);
-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "zz_pa" violates foreign key constraint "zz_p_refid_fkey"
-- end-expected-error
INSERT INTO zz_pa VALUES (1, 999);
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_p;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_constraint WHERE conrelid='zz_pa'::regclass AND contype='f';
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_ref VALUES (1);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_p VALUES (3, 1);
-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "zz_ref" violates foreign key constraint "zz_p_refid_fkey" on table "zz_p"
-- end-expected-error
DELETE FROM zz_ref WHERE id = 1;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_ref;
-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "zz_ref" violates foreign key constraint "zz_p_refid_fkey" on table "zz_p"
-- end-expected-error
UPDATE zz_ref SET id = 7 WHERE id = 1;
