-- ============================================================================
-- A row keeps its identity through a snapshot, and a write that names a second
-- relation matches against it once
--
-- The rows a REPEATABLE READ or SERIALIZABLE transaction reads are the ones
-- the snapshot froze, but they are still the same rows: ctid, xmin, cmin, xmax
-- and tableoid read off them exactly as they do outside a transaction, and a
-- qualification may select by ctid. And UPDATE ... FROM / DELETE ... USING act
-- on a target row once however many rows of the second relation it joins.
--
-- The concurrent half of this behaviour -- what a statement that had to wait
-- for another session still sees -- needs two sessions and lives in
-- WriteVisibilityAndDefaultsTest.
--
-- Every value was measured against PostgreSQL 18.
-- ============================================================================

CREATE TABLE wri_e (i int, s text);
INSERT INTO wri_e VALUES (1,'a'),(2,'b'),(3,'c');

-- outside a transaction, each row answers with its own place in the relation
-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 3
-- end-expected
SELECT ctid, i FROM wri_e ORDER BY i;

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 3
-- end-expected
SELECT ctid, i FROM wri_e ORDER BY i;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(DISTINCT ctid) FROM wri_e;

-- begin-expected
-- columns: i
-- row: 2
-- end-expected
SELECT i FROM wri_e WHERE ctid = '(0,2)';

-- begin-expected
-- columns: bool_and
-- row: t
-- end-expected
SELECT bool_and(xmin::text <> '0') FROM wri_e;

-- begin-expected
-- columns: bool_and
-- row: t
-- end-expected
SELECT bool_and(cmin::text = '0') FROM wri_e;

-- begin-expected
-- columns: bool_and
-- row: t
-- end-expected
SELECT bool_and(xmax::text = '0') FROM wri_e;

-- begin-expected
-- columns: bool_and
-- row: t
-- end-expected
SELECT bool_and(tableoid = 'wri_e'::regclass) FROM wri_e;

COMMIT;

BEGIN ISOLATION LEVEL SERIALIZABLE;

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 3
-- end-expected
SELECT ctid, i FROM wri_e ORDER BY i;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(DISTINCT ctid) FROM wri_e;

-- begin-expected
-- columns: i
-- row: 2
-- end-expected
SELECT i FROM wri_e WHERE ctid = '(0,2)';

COMMIT;

-- the isolation level set after BEGIN reads the same way
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 3
-- end-expected
SELECT ctid, i FROM wri_e ORDER BY i;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(DISTINCT ctid) FROM wri_e;

-- begin-expected
-- columns: i
-- row: 2
-- end-expected
SELECT i FROM wri_e WHERE ctid = '(0,2)';

COMMIT;

DROP TABLE wri_e;

-- ============================================================================
-- UPDATE ... FROM and DELETE ... USING with nobody else in the way
-- ============================================================================

CREATE TABLE wri_t (i int PRIMARY KEY, v int, s text);
CREATE TABLE wri_u (j int, w int, t text);
INSERT INTO wri_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c'),(4,40,'d');
INSERT INTO wri_u VALUES (1,100,'x'),(2,200,'y'),(2,201,'y2'),(5,500,'z');

-- a target row that joins more than one row of the second relation is acted on
-- once, and only rows the join keeps are written
UPDATE wri_t SET v = wri_u.w FROM wri_u WHERE wri_u.j = wri_t.i;

-- begin-expected
-- columns: i | v | s
-- row: 1 | 100 | a
-- row: 2 | 200 | b
-- row: 3 | 30 | c
-- row: 4 | 40 | d
-- end-expected
SELECT i, v, s FROM wri_t ORDER BY i;

-- begin-expected
-- columns: i | v | s
-- row: 2 | 200 | y
-- end-expected
UPDATE wri_t SET s = u.t FROM wri_u u WHERE u.j = wri_t.i AND u.w > 150 RETURNING i, v, s;

-- begin-expected
-- columns: i | v | s
-- row: 1 | 100 | a
-- row: 2 | 200 | y
-- row: 3 | 30 | c
-- row: 4 | 40 | d
-- end-expected
SELECT i, v, s FROM wri_t ORDER BY i;

-- a FROM clause that joins nothing writes nothing
UPDATE wri_t SET v = v + 1 FROM wri_u WHERE wri_u.j = 999;

-- begin-expected
-- columns: i | v | s
-- row: 1 | 100 | a
-- row: 2 | 200 | y
-- row: 3 | 30 | c
-- row: 4 | 40 | d
-- end-expected
SELECT i, v, s FROM wri_t ORDER BY i;

-- a subquery on the right of SET is read over the whole relation, not the join
-- begin-expected
-- columns: i | v
-- row: 1 | 500
-- row: 2 | 500
-- end-expected
UPDATE wri_t SET v = (SELECT max(w) FROM wri_u) FROM wri_u z WHERE z.j = wri_t.i RETURNING i, v;

-- begin-expected
-- columns: i | v | s
-- row: 1 | 500 | a
-- row: 2 | 500 | y
-- row: 3 | 30 | c
-- row: 4 | 40 | d
-- end-expected
SELECT i, v, s FROM wri_t ORDER BY i;

-- begin-expected
-- columns: i | v
-- row: 1 | 500
-- end-expected
DELETE FROM wri_t USING wri_u WHERE wri_u.j = wri_t.i AND wri_u.w = 100 RETURNING i, v;

-- begin-expected
-- columns: i | v | s
-- row: 2 | 500 | y
-- row: 3 | 30 | c
-- row: 4 | 40 | d
-- end-expected
SELECT i, v, s FROM wri_t ORDER BY i;

DELETE FROM wri_t USING wri_u WHERE wri_u.j = 999;

-- begin-expected
-- columns: i | v | s
-- row: 2 | 500 | y
-- row: 3 | 30 | c
-- row: 4 | 40 | d
-- end-expected
SELECT i, v, s FROM wri_t ORDER BY i;

-- the row that joins two USING rows is deleted once
DELETE FROM wri_t USING wri_u WHERE wri_u.j = wri_t.i;

-- begin-expected
-- columns: i | v | s
-- row: 3 | 30 | c
-- row: 4 | 40 | d
-- end-expected
SELECT i, v, s FROM wri_t ORDER BY i;

DROP TABLE wri_t;
DROP TABLE wri_u;
