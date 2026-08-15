-- ============================================================================
-- A write reading from its own snapshot
-- ============================================================================
-- A statement in a REPEATABLE READ or SERIALIZABLE transaction may act only on
-- the version of a row its snapshot holds, and when another session takes that
-- row away and commits, PostgreSQL ends the transaction rather than let the
-- statement report that it wrote nothing. That refusal is owed to another
-- session's delete and to nothing else, which is the half of the rule one
-- session can show: everything below is a row the transaction was shown or
-- wrote itself, a row it took away itself, or no row at all, and none of it is
-- refused. The concurrent cases live in the Java test that goes with this
-- file.
-- ============================================================================

CREATE TABLE snd_t (i int PRIMARY KEY, v int);
INSERT INTO snd_t VALUES (1,10),(2,20),(3,30);

-- ----------------------------------------------------------------------------
-- 1. Every row the snapshot holds is the transaction's to write, and so is
--    every row it wrote itself
-- ----------------------------------------------------------------------------
BEGIN ISOLATION LEVEL REPEATABLE READ;

-- the first statement fixes the snapshot
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) AS count FROM snd_t;

UPDATE snd_t SET v = v + 1 WHERE i = 1;
DELETE FROM snd_t WHERE i = 3;
INSERT INTO snd_t VALUES (4,40);
UPDATE snd_t SET v = v + 1 WHERE i = 4;
DELETE FROM snd_t WHERE i = 4;

-- begin-expected
-- columns: i, v
-- row: 1, 11
-- row: 2, 20
-- end-expected
SELECT i, v FROM snd_t ORDER BY i;

COMMIT;

-- begin-expected
-- columns: i, v
-- row: 1, 11
-- row: 2, 20
-- end-expected
SELECT i, v FROM snd_t ORDER BY i;

-- ----------------------------------------------------------------------------
-- 2. A row the transaction took away itself is not a row taken from it: the
--    write that no longer reaches it simply reaches nothing
-- ----------------------------------------------------------------------------
BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM snd_t;

DELETE FROM snd_t WHERE i = 2;
UPDATE snd_t SET v = 99 WHERE i = 2;
DELETE FROM snd_t WHERE i = 2;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM snd_t WHERE v = 99;

-- and locking it finds nothing there to lock
-- begin-expected
-- columns: i, v
-- end-expected
SELECT i, v FROM snd_t WHERE i = 2 FOR UPDATE;

ROLLBACK;

-- begin-expected
-- columns: i, v
-- row: 1, 11
-- row: 2, 20
-- end-expected
SELECT i, v FROM snd_t ORDER BY i;

-- ----------------------------------------------------------------------------
-- 3. A write that reaches no row is not a serialization failure either
-- ----------------------------------------------------------------------------
BEGIN ISOLATION LEVEL REPEATABLE READ;

UPDATE snd_t SET v = 0 WHERE i = 9;
DELETE FROM snd_t WHERE i = 9;
UPDATE snd_t SET v = 0 WHERE false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM snd_t WHERE v = 0;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM snd_t;

COMMIT;

-- ----------------------------------------------------------------------------
-- 4. A SERIALIZABLE transaction empties what its snapshot holds, and a
--    rollback hands it all back
-- ----------------------------------------------------------------------------
BEGIN ISOLATION LEVEL SERIALIZABLE;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM snd_t;

DELETE FROM snd_t;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM snd_t;

ROLLBACK;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM snd_t;

-- ----------------------------------------------------------------------------
-- 5. The same holds of a partitioned relation, and of a row moved between its
--    partitions by the transaction itself
-- ----------------------------------------------------------------------------
CREATE TABLE snd_p (i int PRIMARY KEY, v int) PARTITION BY RANGE (i);
CREATE TABLE snd_p0 PARTITION OF snd_p FOR VALUES FROM (0) TO (10);
CREATE TABLE snd_p1 PARTITION OF snd_p FOR VALUES FROM (10) TO (20);
INSERT INTO snd_p VALUES (1,10),(2,20),(12,120);

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) AS count FROM snd_p;

UPDATE snd_p SET i = 15 WHERE i = 2;
UPDATE snd_p SET v = 151 WHERE i = 15;
DELETE FROM snd_p WHERE i = 12;
UPDATE snd_p SET v = 999 WHERE i = 12;

-- begin-expected
-- columns: tableoid, i, v
-- row: snd_p0 | 1 | 10
-- row: snd_p1 | 15 | 151
-- end-expected
SELECT tableoid::regclass::text AS tableoid, i, v FROM snd_p ORDER BY i;

COMMIT;

-- begin-expected
-- columns: tableoid, i, v
-- row: snd_p0 | 1 | 10
-- row: snd_p1 | 15 | 151
-- end-expected
SELECT tableoid::regclass::text AS tableoid, i, v FROM snd_p ORDER BY i;

DROP TABLE snd_p;
DROP TABLE snd_t;
