-- A row's tuple identity across an aborted and a committed write, and a qualification that reads
-- the relation it writes to. Every expectation below is what PostgreSQL 18 answered.

CREATE TABLE zzw5f_tid (i int PRIMARY KEY, v int, s text);
INSERT INTO zzw5f_tid VALUES (1,10,'a'),(2,20,'b'),(3,30,'c');

-- begin-expected
-- columns: place
-- row: 1=(0,1)
-- row: 2=(0,2)
-- row: 3=(0,3)
-- end-expected
SELECT i || '=' || ctid::text AS place FROM zzw5f_tid ORDER BY i;

BEGIN;

-- an UPDATE writes a new version of the row, in a place of its own
UPDATE zzw5f_tid SET v = 99 WHERE i = 1;

-- begin-expected
-- columns: place
-- row: (0,4)
-- end-expected
SELECT ctid::text AS place FROM zzw5f_tid WHERE i = 1;

ROLLBACK;

-- an abort renumbers nothing: the version that was there is the version that is there
-- begin-expected
-- columns: place,v
-- row: (0,1)|10
-- end-expected
SELECT ctid::text AS place, v::text AS v FROM zzw5f_tid WHERE i = 1;

UPDATE zzw5f_tid SET v = 99 WHERE i = 1;

-- the place the aborted version took is not handed out again
-- begin-expected
-- columns: place
-- row: (0,5)
-- end-expected
SELECT ctid::text AS place FROM zzw5f_tid WHERE i = 1;

UPDATE zzw5f_tid SET s = 'q' WHERE i IN (SELECT i FROM zzw5f_tid WHERE v = 20);

-- begin-expected
-- columns: r
-- row: 1:99:a
-- row: 2:20:q
-- row: 3:30:c
-- end-expected
SELECT i||':'||v||':'||s AS r FROM zzw5f_tid ORDER BY i;

UPDATE zzw5f_tid SET s = 'e' WHERE EXISTS (SELECT 1 FROM zzw5f_tid t2 WHERE t2.i = zzw5f_tid.i AND t2.v = 99);

-- begin-expected
-- columns: r
-- row: 1:99:e
-- row: 2:20:q
-- row: 3:30:c
-- end-expected
SELECT i||':'||v||':'||s AS r FROM zzw5f_tid ORDER BY i;

DROP TABLE zzw5f_tid;