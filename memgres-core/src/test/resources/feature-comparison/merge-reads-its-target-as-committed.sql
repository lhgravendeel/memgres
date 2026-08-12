-- ============================================================================
-- MERGE reads and writes its target the way UPDATE and DELETE do
-- ============================================================================
-- The waiting a MERGE does for another session's uncommitted row cannot be
-- written in one session; what is written here is the answer the target scan,
-- the arms and the INSERT arm have to keep giving when nobody else is writing:
-- the rows a partitioned or inherited target really stores, the row a key
-- change moves, the key the relation already holds, and the writes an aborted
-- transaction takes back.
-- ============================================================================

CREATE TABLE zzy8ctm_c1 (i int PRIMARY KEY, v int, s text);
CREATE TABLE zzy8ctm_c1s (i int, v int, s text);
INSERT INTO zzy8ctm_c1 VALUES (1,10,'a'),(2,20,'b'),(3,30,'c');
INSERT INTO zzy8ctm_c1s VALUES (2,200,'x'),(4,400,'y');

MERGE INTO zzy8ctm_c1 t USING zzy8ctm_c1s u ON t.i = u.i
  WHEN MATCHED THEN UPDATE SET s = u.s
  WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.v, u.s);

-- begin-expected
-- columns: i|v|s
-- row: 1|10|a
-- row: 2|20|x
-- row: 3|30|c
-- row: 4|400|y
-- end-expected
SELECT i, v, s FROM zzy8ctm_c1 ORDER BY i;

-- An arm whose condition fails leaves the row alone and does not count.
-- begin-expected
-- columns: merge_action|i|v|s|v
-- end-expected
MERGE INTO zzy8ctm_c1 t USING zzy8ctm_c1s u ON t.i = u.i
  WHEN MATCHED AND t.v = 77 THEN UPDATE SET s = 'no'
  RETURNING merge_action(), t.i, t.v, t.s, u.v;

-- begin-expected
-- columns: merge_action|i
-- end-expected
MERGE INTO zzy8ctm_c1 t USING zzy8ctm_c1s u ON t.i = u.i
  WHEN MATCHED THEN DO NOTHING
  RETURNING merge_action(), t.i;

-- All three arms over one statement.
MERGE INTO zzy8ctm_c1 t USING (VALUES (2,201,'p'),(9,900,'q')) u(i,v,s) ON t.i = u.i
  WHEN MATCHED THEN UPDATE SET v = t.v + 1
  WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.v, u.s)
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET s = 'ns';

-- begin-expected
-- columns: i|v|s
-- row: 1|10|ns
-- row: 2|21|x
-- row: 3|30|ns
-- row: 4|400|ns
-- row: 9|900|q
-- end-expected
SELECT i, v, s FROM zzy8ctm_c1 ORDER BY i;

-- The INSERT arm is refused a key the relation already holds.
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zzy8ctm_c1_pkey"
-- end-expected-error
MERGE INTO zzy8ctm_c1 t USING zzy8ctm_c1s u ON t.i = u.i + 100
  WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.v, u.s);

-- begin-expected
-- columns: cnt
-- row: 5
-- end-expected
SELECT count(*) AS cnt FROM zzy8ctm_c1;

-- The WHEN NOT MATCHED BY SOURCE arm reaches the rows the join left over, and
-- no others.
-- begin-expected
-- columns: merge_action|i
-- row: DELETE|1
-- row: DELETE|3
-- row: DELETE|9
-- end-expected
MERGE INTO zzy8ctm_c1 t USING zzy8ctm_c1s u ON t.i = u.i
  WHEN NOT MATCHED BY SOURCE THEN DELETE
  RETURNING merge_action(), t.i;

-- begin-expected
-- columns: i|v|s
-- row: 2|21|x
-- row: 4|400|ns
-- end-expected
SELECT i, v, s FROM zzy8ctm_c1 ORDER BY i;

-- ============================================================================
-- What a MERGE writes is taken back with the transaction that wrote it
-- ============================================================================
BEGIN;
MERGE INTO zzy8ctm_c1 t USING zzy8ctm_c1s u ON t.i = u.i
  WHEN MATCHED THEN UPDATE SET s = 'in-flight'
  WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.v, 'in-flight');

-- begin-expected
-- columns: i|v|s
-- row: 2|21|in-flight
-- row: 4|400|in-flight
-- end-expected
SELECT i, v, s FROM zzy8ctm_c1 ORDER BY i;
ROLLBACK;

-- begin-expected
-- columns: i|v|s
-- row: 2|21|x
-- row: 4|400|ns
-- end-expected
SELECT i, v, s FROM zzy8ctm_c1 ORDER BY i;

BEGIN;
SAVEPOINT zzy8ctm_sp;
MERGE INTO zzy8ctm_c1 t USING zzy8ctm_c1s u ON t.i = u.i
  WHEN MATCHED THEN DELETE;
ROLLBACK TO SAVEPOINT zzy8ctm_sp;

-- begin-expected
-- columns: i|v|s
-- row: 2|21|x
-- row: 4|400|ns
-- end-expected
SELECT i, v, s FROM zzy8ctm_c1 ORDER BY i;
COMMIT;

-- ============================================================================
-- A partitioned target, whose rows live in the leaves
-- ============================================================================
CREATE TABLE zzy8ctm_c2 (i int PRIMARY KEY, v int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzy8ctm_c2a PARTITION OF zzy8ctm_c2 FOR VALUES FROM (0) TO (10);
CREATE TABLE zzy8ctm_c2b PARTITION OF zzy8ctm_c2 FOR VALUES FROM (10) TO (20);
CREATE TABLE zzy8ctm_c2s (i int, v int, s text);
INSERT INTO zzy8ctm_c2 VALUES (1,10,'a'),(2,20,'b'),(12,120,'l');
INSERT INTO zzy8ctm_c2s VALUES (2,200,'x'),(12,220,'z'),(15,150,'n');

-- begin-expected
-- columns: merge_action|i|s
-- row: UPDATE|2|x
-- row: UPDATE|12|z
-- row: INSERT|15|n
-- end-expected
MERGE INTO zzy8ctm_c2 t USING zzy8ctm_c2s u ON t.i = u.i
  WHEN MATCHED THEN UPDATE SET s = u.s
  WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.v, u.s)
  RETURNING merge_action(), t.i, t.s;

-- begin-expected
-- columns: i|v|s
-- row: 1|10|a
-- row: 2|20|x
-- row: 12|120|z
-- row: 15|150|n
-- end-expected
SELECT i, v, s FROM zzy8ctm_c2 ORDER BY i;

-- begin-expected
-- columns: c1|c2
-- row: 2|2
-- end-expected
SELECT (SELECT count(*) FROM zzy8ctm_c2a) AS c1, (SELECT count(*) FROM zzy8ctm_c2b) AS c2;

-- An assignment to the partition key moves the row into the partition it now
-- belongs to.
MERGE INTO zzy8ctm_c2 t USING zzy8ctm_c2s u ON t.i = u.i AND u.i = 2
  WHEN MATCHED THEN UPDATE SET i = 14;

-- begin-expected
-- columns: c1|c2
-- row: 1|3
-- end-expected
SELECT (SELECT count(*) FROM zzy8ctm_c2a) AS c1, (SELECT count(*) FROM zzy8ctm_c2b) AS c2;

-- begin-expected
-- columns: i|v|s
-- row: 1|10|a
-- row: 12|120|z
-- row: 14|20|x
-- row: 15|150|n
-- end-expected
SELECT i, v, s FROM zzy8ctm_c2 ORDER BY i;

-- begin-expected
-- columns: rel|i
-- row: zzy8ctm_c2a|1
-- row: zzy8ctm_c2b|12
-- row: zzy8ctm_c2b|14
-- row: zzy8ctm_c2b|15
-- end-expected
SELECT tableoid::regclass::text AS rel, i FROM zzy8ctm_c2 ORDER BY i;

-- ============================================================================
-- An inheritance parent, whose rows live in its children
-- ============================================================================
CREATE TABLE zzy8ctm_c3 (i int, v int, s text);
CREATE TABLE zzy8ctm_c3c (i int, v int, s text) INHERITS (zzy8ctm_c3);
CREATE TABLE zzy8ctm_c3s (i int, v int, s text);
INSERT INTO zzy8ctm_c3 VALUES (1,10,'a');
INSERT INTO zzy8ctm_c3c VALUES (2,20,'b');
INSERT INTO zzy8ctm_c3s VALUES (2,200,'x'),(4,400,'y');

-- begin-expected
-- columns: merge_action|i|s
-- row: UPDATE|2|x
-- row: INSERT|4|y
-- end-expected
MERGE INTO zzy8ctm_c3 t USING zzy8ctm_c3s u ON t.i = u.i
  WHEN MATCHED THEN UPDATE SET s = u.s
  WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.v, u.s)
  RETURNING merge_action(), t.i, t.s;

-- The matched row was rewritten where it lives; the new one belongs to the
-- parent the statement named.
-- begin-expected
-- columns: rel|i|v|s
-- row: zzy8ctm_c3|1|10|a
-- row: zzy8ctm_c3c|2|20|x
-- row: zzy8ctm_c3|4|400|y
-- end-expected
SELECT tableoid::regclass::text AS rel, i, v, s FROM zzy8ctm_c3 ORDER BY i;

-- ONLY reads what the parent stores itself.
-- begin-expected
-- columns: i|v|s
-- row: 1|10|a
-- row: 4|400|y
-- end-expected
SELECT i, v, s FROM ONLY zzy8ctm_c3 ORDER BY i;

-- begin-expected
-- columns: merge_action|i|s
-- row: UPDATE|1|ns
-- end-expected
MERGE INTO zzy8ctm_c3 t USING zzy8ctm_c3s u ON t.i = u.i
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET s = 'ns'
  RETURNING merge_action(), t.i, t.s;

-- ============================================================================
-- A source that reads the target relation
-- ============================================================================
CREATE TABLE zzy8ctm_c4 (i int PRIMARY KEY, v int, s text);
INSERT INTO zzy8ctm_c4 VALUES (1,10,'a'),(2,20,'b'),(3,30,'c');

-- begin-expected
-- columns: merge_action|i|v
-- row: UPDATE|2|10
-- end-expected
MERGE INTO zzy8ctm_c4 t USING (SELECT i+1 AS i, v FROM zzy8ctm_c4 WHERE i = 1) u ON t.i = u.i
  WHEN MATCHED THEN UPDATE SET v = u.v
  RETURNING merge_action(), t.i, t.v;

-- begin-expected
-- columns: i|v|s
-- row: 1|10|a
-- row: 2|10|b
-- row: 3|30|c
-- end-expected
SELECT i, v, s FROM zzy8ctm_c4 ORDER BY i;

DROP TABLE zzy8ctm_c4;
DROP TABLE zzy8ctm_c3c;
DROP TABLE zzy8ctm_c3;
DROP TABLE zzy8ctm_c3s;
DROP TABLE zzy8ctm_c2 CASCADE;
DROP TABLE zzy8ctm_c2s;
DROP TABLE zzy8ctm_c1;
DROP TABLE zzy8ctm_c1s;
