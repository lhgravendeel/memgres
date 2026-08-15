-- ============================================================================
-- The version a write chooses its rows on
-- ============================================================================
-- A statement in a REPEATABLE READ or SERIALIZABLE transaction chooses the rows
-- it writes on the version its snapshot holds, and it is refused the moment one
-- of them turns out to have been replaced by a transaction that committed since.
-- One session can show only the half of that rule which is never refused: a row
-- this transaction wrote itself is its own latest version, so a qualification
-- reads it as the transaction left it, however many times it is written and
-- whatever the snapshot was shown before. The refusals belong to the concurrent
-- cases, which live in the Java test that goes with this file.
-- ============================================================================

CREATE TABLE vwq_t (i int PRIMARY KEY, v int);
INSERT INTO vwq_t VALUES (1,10),(2,20),(3,30);

-- ----------------------------------------------------------------------------
-- 1. A row this transaction wrote out of its own qualification is passed over,
--    and nothing is refused over it
-- ----------------------------------------------------------------------------
BEGIN ISOLATION LEVEL REPEATABLE READ;

-- the first statement fixes the snapshot
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) AS count FROM vwq_t;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
UPDATE vwq_t SET v = -1 WHERE i = 1 RETURNING i;

-- begin-expected
-- columns: i
-- end-expected
UPDATE vwq_t SET v = 9 WHERE i = 1 AND v > 0 RETURNING i;

-- begin-expected
-- columns: i
-- end-expected
DELETE FROM vwq_t WHERE i = 1 AND v > 0 RETURNING i;

-- begin-expected
-- columns: i | v
-- row: 1, -1
-- row: 2, 20
-- row: 3, 30
-- end-expected
SELECT i, v FROM vwq_t ORDER BY i;

COMMIT;

-- ----------------------------------------------------------------------------
-- 2. A row this transaction wrote into its own qualification is written again
-- ----------------------------------------------------------------------------
BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) AS count FROM vwq_t;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
UPDATE vwq_t SET v = 5 WHERE i = 1 RETURNING i;

-- begin-expected
-- columns: i | v
-- row: 1, 9
-- end-expected
UPDATE vwq_t SET v = 9 WHERE i = 1 AND v > 0 RETURNING i, v;

COMMIT;

-- ----------------------------------------------------------------------------
-- 3. The same at SERIALIZABLE, and with the row taken away at the end
-- ----------------------------------------------------------------------------
BEGIN ISOLATION LEVEL SERIALIZABLE;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) AS count FROM vwq_t;

-- begin-expected
-- columns: i
-- row: 3
-- end-expected
UPDATE vwq_t SET v = -3 WHERE i = 3 RETURNING i;

-- begin-expected
-- columns: i
-- end-expected
DELETE FROM vwq_t WHERE i = 3 AND v > 0 RETURNING i;

-- begin-expected
-- columns: i | v
-- row: 3, -3
-- end-expected
DELETE FROM vwq_t WHERE i = 3 AND v < 0 RETURNING i, v;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM vwq_t;

COMMIT;

-- ----------------------------------------------------------------------------
-- 4. A row the transaction inserted itself is its own to write and to take away
-- ----------------------------------------------------------------------------
BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM vwq_t;

INSERT INTO vwq_t VALUES (4,-4);

-- begin-expected
-- columns: i
-- end-expected
UPDATE vwq_t SET v = 9 WHERE i = 4 AND v > 0 RETURNING i;

-- begin-expected
-- columns: i | v
-- row: 4, 40
-- end-expected
UPDATE vwq_t SET v = 40 WHERE i = 4 AND v < 0 RETURNING i, v;

-- begin-expected
-- columns: i
-- row: 4
-- end-expected
DELETE FROM vwq_t WHERE i = 4 AND v > 0 RETURNING i;

COMMIT;

-- begin-expected
-- columns: i | v
-- row: 1, 9
-- row: 2, 20
-- end-expected
SELECT i, v FROM vwq_t ORDER BY i;

-- ----------------------------------------------------------------------------
-- 5. A savepoint takes the write back, and the row is written as it stood
--    before it
-- ----------------------------------------------------------------------------
BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM vwq_t;

SAVEPOINT s1;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
UPDATE vwq_t SET v = -9 WHERE i = 1 RETURNING i;

ROLLBACK TO SAVEPOINT s1;

-- begin-expected
-- columns: i | v
-- row: 1, 11
-- end-expected
UPDATE vwq_t SET v = 11 WHERE i = 1 AND v > 0 RETURNING i, v;

COMMIT;

-- ----------------------------------------------------------------------------
-- 6. The same through a partitioned table, where the row lives in a partition
--    and the statement names the relation above it
-- ----------------------------------------------------------------------------
CREATE TABLE vwq_p (i int, v int) PARTITION BY RANGE (i);
CREATE TABLE vwq_p0 PARTITION OF vwq_p FOR VALUES FROM (0) TO (10);
CREATE TABLE vwq_p1 PARTITION OF vwq_p FOR VALUES FROM (10) TO (20);
INSERT INTO vwq_p VALUES (2,20),(12,120);

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM vwq_p;

-- begin-expected
-- columns: i
-- row: 2
-- end-expected
UPDATE vwq_p SET v = -2 WHERE i = 2 RETURNING i;

-- begin-expected
-- columns: i
-- end-expected
UPDATE vwq_p SET v = 9 WHERE i = 2 AND v > 0 RETURNING i;

-- begin-expected
-- columns: i | v
-- row: 2, 22
-- end-expected
UPDATE vwq_p0 SET v = 22 WHERE i = 2 AND v < 0 RETURNING i, v;

-- begin-expected
-- columns: i | v
-- row: 2, 22
-- row: 12, 120
-- end-expected
SELECT i, v FROM vwq_p ORDER BY i;

COMMIT;

-- ----------------------------------------------------------------------------
-- 7. And through an inheritance parent, where the child carries a column of its
--    own that the parent never declared
-- ----------------------------------------------------------------------------
CREATE TABLE vwq_h (i int, v int);
CREATE TABLE vwq_hc (extra text) INHERITS (vwq_h);
INSERT INTO vwq_hc VALUES (3,30,'x');

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) AS count FROM vwq_h;

-- begin-expected
-- columns: i
-- row: 3
-- end-expected
UPDATE vwq_h SET v = -3 WHERE i = 3 RETURNING i;

-- begin-expected
-- columns: i
-- end-expected
UPDATE vwq_h SET v = 9 WHERE i = 3 AND v > 0 RETURNING i;

-- begin-expected
-- columns: i | v
-- row: 3, 33
-- end-expected
UPDATE vwq_h SET v = 33 WHERE i = 3 AND v < 0 RETURNING i, v;

-- begin-expected
-- columns: i | v | extra
-- row: 3, 33, x
-- end-expected
SELECT i, v, extra FROM vwq_hc;

COMMIT;

-- ----------------------------------------------------------------------------
-- 8. READ COMMITTED reads each statement afresh and has no snapshot to judge a
--    write by, so the same shapes answer the same way
-- ----------------------------------------------------------------------------
BEGIN;

-- begin-expected
-- columns: i
-- row: 2
-- end-expected
UPDATE vwq_t SET v = -1 WHERE i = 2 RETURNING i;

-- begin-expected
-- columns: i
-- end-expected
UPDATE vwq_t SET v = 9 WHERE i = 2 AND v > 0 RETURNING i;

-- begin-expected
-- columns: i | v
-- row: 2, 21
-- end-expected
UPDATE vwq_t SET v = 21 WHERE i = 2 AND v < 0 RETURNING i, v;

COMMIT;

-- begin-expected
-- columns: i | v
-- row: 1, 11
-- row: 2, 21
-- end-expected
SELECT i, v FROM vwq_t ORDER BY i;

-- ----------------------------------------------------------------------------
-- 9. A write joined to another relation chooses its rows the same way
-- ----------------------------------------------------------------------------
CREATE TABLE vwq_f (i int, w int);
INSERT INTO vwq_f VALUES (1,100),(2,200);

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM vwq_t;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
UPDATE vwq_t SET v = -1 WHERE i = 1 RETURNING i;

-- begin-expected
-- columns: i | v
-- row: 2, 200
-- end-expected
UPDATE vwq_t SET v = vwq_f.w FROM vwq_f WHERE vwq_f.i = vwq_t.i AND vwq_t.v > 0 RETURNING vwq_t.i, vwq_t.v;

-- begin-expected
-- columns: i | v
-- row: 1, -1
-- end-expected
DELETE FROM vwq_t USING vwq_f WHERE vwq_f.i = vwq_t.i AND vwq_t.v < 0 RETURNING vwq_t.i, vwq_t.v;

-- begin-expected
-- columns: i | v
-- row: 2, 200
-- end-expected
SELECT i, v FROM vwq_t ORDER BY i;

COMMIT;

DROP TABLE vwq_f;
DROP TABLE vwq_hc;
DROP TABLE vwq_h;
DROP TABLE vwq_p;
DROP TABLE vwq_t;
