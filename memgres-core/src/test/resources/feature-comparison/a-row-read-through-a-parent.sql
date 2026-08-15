-- A row read through a partitioned parent or an inheritance parent still knows which row it is.
-- ctid, tableoid, xmin, xmax and cmin are answered from the relation that stores the row, not from
-- the one it was reached through, and a write qualified on ctid or tableoid reaches every relation
-- the parent stands for. Every expectation was measured on PostgreSQL 18.

-- ============================================================================
-- A row read through a partitioned parent keeps its own place and its own name
-- ============================================================================
CREATE TABLE rrp_p (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE rrp_p0 PARTITION OF rrp_p FOR VALUES FROM (0) TO (10);
CREATE TABLE rrp_p1 PARTITION OF rrp_p FOR VALUES FROM (10) TO (20);
INSERT INTO rrp_p VALUES (1,'a'),(2,'b'),(11,'c'),(12,'d');

-- Each partition numbers its own tuples from one.
-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_p0 | 1
-- row: (0,2) | rrp_p0 | 2
-- row: (0,1) | rrp_p1 | 11
-- row: (0,2) | rrp_p1 | 12
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_p ORDER BY i;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(DISTINCT ctid) FROM rrp_p;

-- begin-expected
-- columns: i
-- row: 2
-- row: 12
-- end-expected
SELECT i FROM rrp_p WHERE ctid = '(0,2)' ORDER BY i;

-- begin-expected
-- columns: xmin | xmax | cmin
-- row: t | t | t
-- end-expected
SELECT bool_and(xmin::text <> '0') AS xmin, bool_and(xmax::text = '0') AS xmax,
       bool_and(cmin::text = '0') AS cmin FROM rrp_p;

-- ONLY reads the partitioned table's own storage, which holds nothing.
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM ONLY rrp_p;

-- A qualification on ctid reaches every partition: one tuple per partition answers to (0,2).
DELETE FROM rrp_p WHERE ctid = '(0,2)';

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- row: 11 | c
-- end-expected
SELECT i, s FROM rrp_p ORDER BY i;

-- A qualification on tableoid names one partition.
UPDATE rrp_p SET s = 'Q' WHERE tableoid = 'rrp_p1'::regclass;

-- An update writes the new version of the row in the relation that stores it, so the row's place
-- moves within that relation and not within the one it was written through.
-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,1) | rrp_p0 | 1 | a
-- row: (0,3) | rrp_p1 | 11 | Q
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i, s FROM rrp_p ORDER BY i;

-- begin-expected
-- columns: ctid | i | s
-- row: (0,3) | 11 | Q
-- end-expected
SELECT ctid, i, s FROM rrp_p1 ORDER BY i;

DROP TABLE rrp_p;

-- ============================================================================
-- A row read through an inheritance parent keeps its own place and its own name
-- ============================================================================
CREATE TABLE rrp_h (i int, s text);
CREATE TABLE rrp_h0 (x int) INHERITS (rrp_h);
INSERT INTO rrp_h VALUES (1,'a'),(2,'b');
INSERT INTO rrp_h0 VALUES (3,'c',30),(4,'d',40);

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_h | 1
-- row: (0,2) | rrp_h | 2
-- row: (0,1) | rrp_h0 | 3
-- row: (0,2) | rrp_h0 | 4
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_h ORDER BY i;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(DISTINCT ctid) FROM rrp_h;

-- begin-expected
-- columns: i
-- row: 1
-- row: 3
-- end-expected
SELECT i FROM rrp_h WHERE ctid = '(0,1)' ORDER BY i;

-- ONLY reads what the parent stores itself.
-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_h | 1
-- row: (0,2) | rrp_h | 2
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM ONLY rrp_h ORDER BY i;

UPDATE rrp_h SET s = 'z' WHERE i = 3;

-- begin-expected
-- columns: ctid | i | s
-- row: (0,3) | 3 | z
-- row: (0,2) | 4 | d
-- end-expected
SELECT ctid, i, s FROM rrp_h0 ORDER BY i;

-- The parent's own numbering is untouched by a write to a row it does not store.
-- begin-expected
-- columns: ctid | i | s
-- row: (0,1) | 1 | a
-- row: (0,2) | 2 | b
-- end-expected
SELECT ctid, i, s FROM ONLY rrp_h ORDER BY i;

DELETE FROM rrp_h WHERE tableoid = 'rrp_h0'::regclass;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM rrp_h;

DROP TABLE rrp_h CASCADE;

-- ============================================================================
-- Sub-partitions, inheritance chains and a partition with its own column order
-- ============================================================================
CREATE TABLE rrp_a (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE rrp_a0 PARTITION OF rrp_a FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (i);
CREATE TABLE rrp_a00 PARTITION OF rrp_a0 FOR VALUES FROM (0) TO (10);
CREATE TABLE rrp_a01 PARTITION OF rrp_a0 FOR VALUES FROM (10) TO (100);
INSERT INTO rrp_a VALUES (1,'a'),(2,'b'),(11,'c');

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_a00 | 1
-- row: (0,2) | rrp_a00 | 2
-- row: (0,1) | rrp_a01 | 11
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_a ORDER BY i;

-- The same rows read at the level between them.
-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_a00 | 1
-- row: (0,2) | rrp_a00 | 2
-- row: (0,1) | rrp_a01 | 11
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_a0 ORDER BY i;

DROP TABLE rrp_a;

CREATE TABLE rrp_b (i int, s text);
CREATE TABLE rrp_b0 (x int) INHERITS (rrp_b);
CREATE TABLE rrp_b00 (y int) INHERITS (rrp_b0);
INSERT INTO rrp_b VALUES (1,'a');
INSERT INTO rrp_b0 VALUES (2,'b',20);
INSERT INTO rrp_b00 VALUES (3,'c',30,300);

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_b | 1
-- row: (0,1) | rrp_b0 | 2
-- row: (0,1) | rrp_b00 | 3
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_b ORDER BY i;

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_b0 | 2
-- row: (0,1) | rrp_b00 | 3
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_b0 ORDER BY i;

DROP TABLE rrp_b CASCADE;

CREATE TABLE rrp_c (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE rrp_c0 (s text, i int);
INSERT INTO rrp_c0 VALUES ('x',1),('y',2);
ALTER TABLE rrp_c ATTACH PARTITION rrp_c0 FOR VALUES FROM (0) TO (10);

-- A partition may hold its columns in another order; the row's place is still its own.
-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,1) | rrp_c0 | 1 | x
-- row: (0,2) | rrp_c0 | 2 | y
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i, s FROM rrp_c ORDER BY i;

DROP TABLE rrp_c;

-- ============================================================================
-- A write qualified on ctid or tableoid reaches the relation that stores the row
-- ============================================================================
CREATE TABLE rrp_g (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE rrp_g0 PARTITION OF rrp_g FOR VALUES FROM (0) TO (10);
CREATE TABLE rrp_g1 PARTITION OF rrp_g FOR VALUES FROM (10) TO (20);
INSERT INTO rrp_g VALUES (1,'a'),(2,'b'),(11,'c'),(12,'d');

DELETE FROM rrp_g WHERE ctid = '(0,2)';

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- row: 11 | c
-- end-expected
SELECT i, s FROM rrp_g ORDER BY i;

UPDATE rrp_g SET s = 'Y' WHERE ctid = '(0,1)';

-- begin-expected
-- columns: i | s
-- row: 1 | Y
-- row: 11 | Y
-- end-expected
SELECT i, s FROM rrp_g ORDER BY i;

DROP TABLE rrp_g;

CREATE TABLE rrp_r (i int, s text);
CREATE TABLE rrp_r0 () INHERITS (rrp_r);
INSERT INTO rrp_r VALUES (1,'a');
INSERT INTO rrp_r0 VALUES (2,'b'),(3,'c');

UPDATE rrp_r SET s = 'q' WHERE tableoid = 'rrp_r0'::regclass;

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- row: 2 | q
-- row: 3 | q
-- end-expected
SELECT i, s FROM rrp_r ORDER BY i;

DELETE FROM rrp_r WHERE tableoid = 'rrp_r0'::regclass;

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- end-expected
SELECT i, s FROM rrp_r ORDER BY i;

DROP TABLE rrp_r CASCADE;

-- An update through an inheritance parent moves the row within the child that stores it.
CREATE TABLE rrp_k (i int, s text);
CREATE TABLE rrp_k0 () INHERITS (rrp_k);
INSERT INTO rrp_k VALUES (1,'a');
INSERT INTO rrp_k0 VALUES (2,'b'),(3,'c');
UPDATE rrp_k SET s = 'z' WHERE i = 2;

-- begin-expected
-- columns: ctid | i | s
-- row: (0,3) | 2 | z
-- row: (0,2) | 3 | c
-- end-expected
SELECT ctid, i, s FROM rrp_k0 ORDER BY i;

-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,1) | rrp_k | 1 | a
-- row: (0,3) | rrp_k0 | 2 | z
-- row: (0,2) | rrp_k0 | 3 | c
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i, s FROM rrp_k ORDER BY i;

-- begin-expected
-- columns: ctid | i | s
-- row: (0,1) | 1 | a
-- end-expected
SELECT ctid, i, s FROM ONLY rrp_k ORDER BY i;

DROP TABLE rrp_k CASCADE;

-- ============================================================================
-- A repeatable-read transaction reads the same places through the same relations
-- ============================================================================
CREATE TABLE rrp_v (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE rrp_v0 PARTITION OF rrp_v FOR VALUES FROM (0) TO (10);
CREATE TABLE rrp_v1 PARTITION OF rrp_v FOR VALUES FROM (10) TO (20);
INSERT INTO rrp_v VALUES (1,'a'),(2,'b'),(11,'c');
CREATE TABLE rrp_w (i int, s text);
CREATE TABLE rrp_w0 (x int) INHERITS (rrp_w);
INSERT INTO rrp_w VALUES (1,'a');
INSERT INTO rrp_w0 VALUES (2,'b',20),(3,'c',30);

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_v0 | 1
-- row: (0,2) | rrp_v0 | 2
-- row: (0,1) | rrp_v1 | 11
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_v ORDER BY i;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(DISTINCT ctid) FROM rrp_v;

-- begin-expected
-- columns: i
-- row: 2
-- end-expected
SELECT i FROM rrp_v WHERE ctid = '(0,2)' ORDER BY i;

-- begin-expected
-- columns: xmin | xmax | cmin
-- row: t | t | t
-- end-expected
SELECT bool_and(xmin::text <> '0') AS xmin, bool_and(xmax::text = '0') AS xmax,
       bool_and(cmin::text = '0') AS cmin FROM rrp_v;

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_w | 1
-- row: (0,1) | rrp_w0 | 2
-- row: (0,2) | rrp_w0 | 3
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_w ORDER BY i;

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_w | 1
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM ONLY rrp_w ORDER BY i;

-- What this transaction writes itself it reads at the place its own relation gave it.
INSERT INTO rrp_v VALUES (5,'e');
INSERT INTO rrp_w0 VALUES (7,'g',70);

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_v0 | 1
-- row: (0,2) | rrp_v0 | 2
-- row: (0,3) | rrp_v0 | 5
-- row: (0,1) | rrp_v1 | 11
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_v ORDER BY i;

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_w | 1
-- row: (0,1) | rrp_w0 | 2
-- row: (0,2) | rrp_w0 | 3
-- row: (0,3) | rrp_w0 | 7
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_w ORDER BY i;

UPDATE rrp_v SET s = 'Z' WHERE i = 1;

-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,4) | rrp_v0 | 1 | Z
-- row: (0,2) | rrp_v0 | 2 | b
-- row: (0,3) | rrp_v0 | 5 | e
-- row: (0,1) | rrp_v1 | 11 | c
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i, s FROM rrp_v ORDER BY i;

DELETE FROM rrp_w WHERE i = 3;

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_w | 1
-- row: (0,1) | rrp_w0 | 2
-- row: (0,3) | rrp_w0 | 7
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_w ORDER BY i;

COMMIT;

-- begin-expected
-- columns: ctid | reln | i | s
-- row: (0,4) | rrp_v0 | 1 | Z
-- row: (0,2) | rrp_v0 | 2 | b
-- row: (0,3) | rrp_v0 | 5 | e
-- row: (0,1) | rrp_v1 | 11 | c
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i, s FROM rrp_v ORDER BY i;

DROP TABLE rrp_v;
DROP TABLE rrp_w CASCADE;

-- ============================================================================
-- A relation made inside a repeatable-read transaction is read the same way
-- ============================================================================
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT 1;
CREATE TABLE rrp_t (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE rrp_t0 PARTITION OF rrp_t FOR VALUES FROM (0) TO (10);
CREATE TABLE rrp_t1 PARTITION OF rrp_t FOR VALUES FROM (10) TO (20);
INSERT INTO rrp_t VALUES (1,'a'),(2,'b'),(11,'c');

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_t0 | 1
-- row: (0,2) | rrp_t0 | 2
-- row: (0,1) | rrp_t1 | 11
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_t ORDER BY i;

CREATE TABLE rrp_u (i int, s text);
CREATE TABLE rrp_u0 (x int) INHERITS (rrp_u);
INSERT INTO rrp_u VALUES (1,'a');
INSERT INTO rrp_u0 VALUES (2,'b',20);

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_u | 1
-- row: (0,1) | rrp_u0 | 2
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_u ORDER BY i;

-- begin-expected
-- columns: xmin | xmax
-- row: t | t
-- end-expected
SELECT bool_and(xmin::text <> '0') AS xmin, bool_and(xmax::text = '0') AS xmax FROM rrp_t;

COMMIT;

-- begin-expected
-- columns: ctid | reln | i
-- row: (0,1) | rrp_t0 | 1
-- row: (0,2) | rrp_t0 | 2
-- row: (0,1) | rrp_t1 | 11
-- end-expected
SELECT ctid, tableoid::regclass::text AS reln, i FROM rrp_t ORDER BY i;

-- cleanup
DROP TABLE rrp_t;
DROP TABLE rrp_u CASCADE;
