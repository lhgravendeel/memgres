-- ============================================================================
-- A snapshot of an inheritance parent holds what its children hold for it
-- ============================================================================

DROP TABLE IF EXISTS snap_ic CASCADE;
DROP TABLE IF EXISTS snap_ip CASCADE;
CREATE TABLE snap_ip (id int, t text);
CREATE TABLE snap_ic (extra int) INHERITS (snap_ip);
INSERT INTO snap_ip VALUES (1,'p');
INSERT INTO snap_ic VALUES (2,'c',9);

BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- stmt: reading the parent reads the child, at the instant the snapshot was taken
-- begin-expected
-- columns: id|t
-- row: 1|p
-- row: 2|c
-- end-expected
SELECT id::text AS id, t FROM snap_ip ORDER BY id;

-- stmt: ONLY reads what the parent stores itself
-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM ONLY snap_ip;

INSERT INTO snap_ic VALUES (3,'c2',8);

-- stmt: a row written straight into the child is a row of the parent as well
-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::text AS cnt FROM snap_ip;

UPDATE snap_ip SET t = 'x' WHERE id = 2;

-- stmt: the write went to the row the child stores, and both names show it
-- begin-expected
-- columns: id|t
-- row: 2|x
-- end-expected
SELECT id::text AS id, t FROM snap_ip WHERE id = 2;

-- begin-expected
-- columns: id|t|extra
-- row: 2|x|9
-- end-expected
SELECT id::text AS id, t, extra::text AS extra FROM snap_ic WHERE id = 2;

DELETE FROM snap_ip WHERE id = 2;

-- stmt: and a delete through the parent takes it from both
-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::text AS cnt FROM snap_ip;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM snap_ic;

COMMIT;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::text AS cnt FROM snap_ip;

DROP TABLE snap_ic;
DROP TABLE snap_ip;

-- ============================================================================
-- A partitioned table stores no rows of its own, and its snapshot says so
-- ============================================================================

DROP TABLE IF EXISTS snap_pp CASCADE;
CREATE TABLE snap_pp (id int) PARTITION BY RANGE (id);
CREATE TABLE snap_pc PARTITION OF snap_pp FOR VALUES FROM (1) TO (100);
INSERT INTO snap_pp VALUES (1),(2),(3);

BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::text AS cnt FROM snap_pp;

DELETE FROM snap_pp WHERE id = 2;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::text AS cnt FROM snap_pp;

UPDATE snap_pp SET id = 50 WHERE id = 3;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM snap_pp WHERE id = 50;

INSERT INTO snap_pp VALUES (7);

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::text AS cnt FROM snap_pp;

-- stmt: every one of those rows lives in the partition, so ONLY finds none
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::text AS cnt FROM ONLY snap_pp;

COMMIT;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::text AS cnt FROM snap_pp;

DROP TABLE snap_pp;
