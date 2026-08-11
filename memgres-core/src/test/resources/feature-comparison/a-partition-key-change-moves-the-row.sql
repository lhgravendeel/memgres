-- An UPDATE that changes the partition key moves the row, and a move is a delete and an insert:
-- the version in the partition that held it is made dead and a new version is written into the
-- partition the new values route to. So the row's place is the destination's next place, an abort
-- leaves it where it came from, and the place the aborted move took is not handed out again.
-- Every expectation was measured on PostgreSQL 18.

-- ============================================================================
-- The row goes where its new values belong
-- ============================================================================
CREATE TABLE pkm_a (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE pkm_a0 PARTITION OF pkm_a FOR VALUES FROM (0) TO (10);
CREATE TABLE pkm_a1 PARTITION OF pkm_a FOR VALUES FROM (10) TO (20);
INSERT INTO pkm_a VALUES (1,'a'),(2,'b'),(11,'c');

-- RETURNING reports the row the statement wrote, not the one it replaced.
-- begin-expected
-- columns: i | s
-- row: 12 | b
-- end-expected
UPDATE pkm_a SET i = 12 WHERE i = 2 RETURNING i, s;

-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,1) | pkm_a0 | 1 | a
-- row: (0,1) | pkm_a1 | 11 | c
-- row: (0,2) | pkm_a1 | 12 | b
-- end-expected
SELECT ctid::text AS ctid, tableoid::regclass::text AS reln, i, s FROM pkm_a
  ORDER BY tableoid::regclass::text, i;

DROP TABLE pkm_a;

-- ============================================================================
-- An aborted move leaves the row in the partition it came from
-- ============================================================================
CREATE TABLE pkm_b (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE pkm_b0 PARTITION OF pkm_b FOR VALUES FROM (0) TO (10);
CREATE TABLE pkm_b1 PARTITION OF pkm_b FOR VALUES FROM (10) TO (20);
INSERT INTO pkm_b VALUES (1,'a'),(2,'b'),(11,'c');

BEGIN;
UPDATE pkm_b SET i = 12 WHERE i = 2;

-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,1) | pkm_b0 | 1 | a
-- row: (0,1) | pkm_b1 | 11 | c
-- row: (0,2) | pkm_b1 | 12 | b
-- end-expected
SELECT ctid::text AS ctid, tableoid::regclass::text AS reln, i, s FROM pkm_b
  ORDER BY tableoid::regclass::text, i;

ROLLBACK;

-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,1) | pkm_b0 | 1 | a
-- row: (0,2) | pkm_b0 | 2 | b
-- row: (0,1) | pkm_b1 | 11 | c
-- end-expected
SELECT ctid::text AS ctid, tableoid::regclass::text AS reln, i, s FROM pkm_b
  ORDER BY tableoid::regclass::text, i;

-- The place the aborted move took in the destination is not handed out again.
UPDATE pkm_b SET i = 12 WHERE i = 2;

-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,1) | pkm_b0 | 1 | a
-- row: (0,1) | pkm_b1 | 11 | c
-- row: (0,3) | pkm_b1 | 12 | b
-- end-expected
SELECT ctid::text AS ctid, tableoid::regclass::text AS reln, i, s FROM pkm_b
  ORDER BY tableoid::regclass::text, i;

DROP TABLE pkm_b;

-- ============================================================================
-- A savepoint undoes a move the same way
-- ============================================================================
CREATE TABLE pkm_c (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE pkm_c0 PARTITION OF pkm_c FOR VALUES FROM (0) TO (10);
CREATE TABLE pkm_c1 PARTITION OF pkm_c FOR VALUES FROM (10) TO (20);
INSERT INTO pkm_c VALUES (2,'b');

BEGIN;
SAVEPOINT sp;
UPDATE pkm_c SET i = 12 WHERE i = 2;

-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,1) | pkm_c1 | 12 | b
-- end-expected
SELECT ctid::text AS ctid, tableoid::regclass::text AS reln, i, s FROM pkm_c
  ORDER BY tableoid::regclass::text, i;

ROLLBACK TO SAVEPOINT sp;

-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,1) | pkm_c0 | 2 | b
-- end-expected
SELECT ctid::text AS ctid, tableoid::regclass::text AS reln, i, s FROM pkm_c
  ORDER BY tableoid::regclass::text, i;

COMMIT;

-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,1) | pkm_c0 | 2 | b
-- end-expected
SELECT ctid::text AS ctid, tableoid::regclass::text AS reln, i, s FROM pkm_c
  ORDER BY tableoid::regclass::text, i;

DROP TABLE pkm_c;

-- ============================================================================
-- UPDATE ... FROM and a MERGE update arm move a row the same way
-- ============================================================================
CREATE TABLE pkm_d (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE pkm_d0 PARTITION OF pkm_d FOR VALUES FROM (0) TO (10);
CREATE TABLE pkm_d1 PARTITION OF pkm_d FOR VALUES FROM (10) TO (20);
CREATE TABLE pkm_ds (k int, nv int);
INSERT INTO pkm_d VALUES (1,'a'),(2,'b');
INSERT INTO pkm_ds VALUES (2,15);

-- begin-expected
-- columns: i | s
-- row: 15 | b
-- end-expected
UPDATE pkm_d SET i = u.nv FROM pkm_ds u WHERE u.k = pkm_d.i RETURNING pkm_d.i, pkm_d.s;

-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,1) | pkm_d0 | 1 | a
-- row: (0,1) | pkm_d1 | 15 | b
-- end-expected
SELECT ctid::text AS ctid, tableoid::regclass::text AS reln, i, s FROM pkm_d
  ORDER BY tableoid::regclass::text, i;

-- The MERGE arm reports the place and the relation the row it wrote now lives at.
-- begin-expected
-- columns: i | s | ctid | reln
-- row: 17 | a | (0,2) | pkm_d1
-- end-expected
MERGE INTO pkm_d t USING (SELECT 1 AS k) q ON t.i = q.k
  WHEN MATCHED THEN UPDATE SET i = 17
  RETURNING t.i, t.s, t.ctid::text AS ctid, t.tableoid::regclass::text AS reln;

-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,1) | pkm_d1 | 15 | b
-- row: (0,2) | pkm_d1 | 17 | a
-- end-expected
SELECT ctid::text AS ctid, tableoid::regclass::text AS reln, i, s FROM pkm_d
  ORDER BY tableoid::regclass::text, i;

DROP TABLE pkm_d;
DROP TABLE pkm_ds;

-- ============================================================================
-- A move between sub-partitions goes to the leaf the values route to
-- ============================================================================
CREATE TABLE pkm_e (i int, j int) PARTITION BY RANGE (i);
CREATE TABLE pkm_e0 PARTITION OF pkm_e FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (j);
CREATE TABLE pkm_e00 PARTITION OF pkm_e0 FOR VALUES FROM (0) TO (10);
CREATE TABLE pkm_e01 PARTITION OF pkm_e0 FOR VALUES FROM (10) TO (20);
CREATE TABLE pkm_e1 PARTITION OF pkm_e FOR VALUES FROM (10) TO (20);
INSERT INTO pkm_e VALUES (1,1),(11,5);

UPDATE pkm_e SET j = 15 WHERE i = 1;

-- begin-expected
-- columns: ctid | reln | i | j
-- row: (0,1) | pkm_e01 | 1 | 15
-- row: (0,1) | pkm_e1 | 11 | 5
-- end-expected
SELECT ctid::text AS ctid, tableoid::regclass::text AS reln, i, j FROM pkm_e
  ORDER BY tableoid::regclass::text, i;

UPDATE pkm_e SET i = 15 WHERE i = 1;

-- begin-expected
-- columns: ctid | reln | i | j
-- row: (0,1) | pkm_e1 | 11 | 5
-- row: (0,2) | pkm_e1 | 15 | 15
-- end-expected
SELECT ctid::text AS ctid, tableoid::regclass::text AS reln, i, j FROM pkm_e
  ORDER BY tableoid::regclass::text, i;

DROP TABLE pkm_e;

-- ============================================================================
-- A row read through a parent is a row that can be locked
-- ============================================================================
CREATE TABLE pkm_l (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE pkm_l0 PARTITION OF pkm_l FOR VALUES FROM (0) TO (10);
CREATE TABLE pkm_l1 PARTITION OF pkm_l FOR VALUES FROM (10) TO (20);
INSERT INTO pkm_l VALUES (1,'a'),(2,'b'),(11,'c');

BEGIN;

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- row: 2 | b
-- row: 11 | c
-- end-expected
SELECT i, s FROM pkm_l ORDER BY i FOR UPDATE;

-- begin-expected
-- columns: i | s
-- row: 2 | b
-- end-expected
SELECT i, s FROM pkm_l WHERE i = 2 FOR UPDATE;

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- row: 2 | b
-- row: 11 | c
-- end-expected
SELECT i, s FROM pkm_l ORDER BY i FOR SHARE;

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- row: 2 | b
-- row: 11 | c
-- end-expected
SELECT i, s FROM pkm_l ORDER BY i FOR UPDATE NOWAIT;

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- row: 2 | b
-- row: 11 | c
-- end-expected
SELECT i, s FROM pkm_l ORDER BY i FOR UPDATE SKIP LOCKED;

-- A partitioned table stores nothing of its own, so ONLY locks nothing.
-- begin-expected
-- columns: i | s
-- end-expected
SELECT i, s FROM ONLY pkm_l ORDER BY i FOR UPDATE;

COMMIT;
DROP TABLE pkm_l;

CREATE TABLE pkm_m (i int, s text);
CREATE TABLE pkm_m0 (extra int) INHERITS (pkm_m);
INSERT INTO pkm_m VALUES (1,'a');
INSERT INTO pkm_m0 VALUES (2,'b',7);

BEGIN;

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- row: 2 | b
-- end-expected
SELECT i, s FROM pkm_m ORDER BY i FOR UPDATE;

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- end-expected
SELECT i, s FROM ONLY pkm_m ORDER BY i FOR UPDATE;

-- begin-expected
-- columns: i | s
-- row: 2 | b
-- end-expected
SELECT i, s FROM pkm_m WHERE i = 2 FOR SHARE;

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- row: 2 | b
-- end-expected
SELECT i, s FROM pkm_m ORDER BY i FOR UPDATE SKIP LOCKED;

COMMIT;
DROP TABLE pkm_m CASCADE;

-- ============================================================================
-- What a routed insert can report of the tuple it wrote
-- ============================================================================
CREATE TABLE pkm_r (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE pkm_r0 PARTITION OF pkm_r FOR VALUES FROM (0) TO (100);

-- Where the row went and which relation took it are answerable.
-- begin-expected
-- columns: i | ctid | reln
-- row: 1 | (0,1) | pkm_r0
-- end-expected
INSERT INTO pkm_r VALUES (1,'a') RETURNING i, ctid::text AS ctid,
  tableoid::regclass::text AS reln;

-- The tuple's own transaction is not, because the row was routed rather than written here.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot retrieve a system column in this context
-- end-expected-error
INSERT INTO pkm_r VALUES (9,'z') RETURNING i, cmin;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot retrieve a system column in this context
-- end-expected-error
INSERT INTO pkm_r VALUES (9,'z') RETURNING i, xmin;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot retrieve a system column in this context
-- end-expected-error
INSERT INTO pkm_r VALUES (9,'z') RETURNING i, xmax::text;

-- A MERGE that can insert is the same statement in another spelling.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot retrieve a system column in this context
-- end-expected-error
MERGE INTO pkm_r t USING (SELECT 6 AS k) q ON t.i = q.k
  WHEN NOT MATCHED THEN INSERT VALUES (q.k,'f') RETURNING t.i, t.cmin;

-- Naming the partition asks nothing of the routing, and neither does a later write.
-- begin-expected
-- columns: i | cmin
-- row: 7 | 0
-- end-expected
INSERT INTO pkm_r0 VALUES (7,'g') RETURNING i, cmin;

-- begin-expected
-- columns: i | cmin
-- row: 7 | 0
-- end-expected
UPDATE pkm_r SET s = 'z' WHERE i = 7 RETURNING i, cmin;

-- begin-expected
-- columns: i | cmin
-- row: 7 | 0
-- end-expected
DELETE FROM pkm_r WHERE i = 7 RETURNING i, cmin;

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- end-expected
SELECT i, s FROM pkm_r ORDER BY i;

-- cleanup
DROP TABLE pkm_r;
