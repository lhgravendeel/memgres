-- source: review-2026-08.md
-- finding: Root cause 2: partition routing is on some write paths and not others
-- area: Dates, times, intervals — and the DML/MERGE/COPY findings filed with them
-- title: Root cause 2: partition routing is on some write paths and not others
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p1 (i int, s text) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p1a PARTITION OF zz_p1 FOR VALUES FROM (1) TO (10);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_p1 VALUES (5, 'a');
-- begin-expected
-- ok: 0
-- end-expected
DELETE FROM ONLY zz_p1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_h1 (id int, v text) PARTITION BY LIST (v);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_h1a PARTITION OF zz_h1 FOR VALUES IN ('a');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_h1d PARTITION OF zz_h1 DEFAULT;
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zz_h1a" violates partition constraint
-- end-expected-error
INSERT INTO zz_h1a VALUES (1, 'b');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_j6 (a int, b text) PARTITION BY RANGE (a);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_j6a PARTITION OF zz_j6 FOR VALUES FROM (0) TO (10);
-- over the wire: COPY zz_j6 FROM STDIN WITH (FORMAT csv)   with   1,x   then   25,q
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_src (k int, nv int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_src VALUES (1, 500);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p6 (id int, k int) PARTITION BY RANGE (id);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p6a PARTITION OF zz_p6 FOR VALUES FROM (0) TO (100);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p6b PARTITION OF zz_p6 FOR VALUES FROM (100) TO (1000);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_p6 VALUES (1, 1);
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_p6 p SET id = s.nv FROM zz_src s WHERE p.k = s.k;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p2 (i int, s text) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p2a PARTITION OF zz_p2 FOR VALUES FROM (1) TO (10);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p2b PARTITION OF zz_p2 FOR VALUES FROM (10) TO (20);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_p2 VALUES (5, 'a');
-- begin-expected
-- columns: i:int4 | s:text
-- row: 15 | a
-- rowcount: 1
-- end-expected
UPDATE zz_p2 SET i = 15 WHERE s = 'a' RETURNING i, s;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p3 (i int PRIMARY KEY, s text) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p3a PARTITION OF zz_p3 FOR VALUES FROM (1) TO (10);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p3b PARTITION OF zz_p3 FOR VALUES FROM (10) TO (20);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_p3 VALUES (5, 'a');
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid ON UPDATE specification
-- end-expected-error
INSERT INTO zz_p3 VALUES (5, 'b') ON CONFLICT (i) DO UPDATE SET i = 17;
