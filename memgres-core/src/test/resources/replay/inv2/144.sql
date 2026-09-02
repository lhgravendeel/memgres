-- source: investigation-2026-08.md
-- finding: 144
-- title: ALTER TABLE ... RENAME TO constructs a brand-new Table and hand-copies a subset of its state: it re-points its own partitions and its own parent pointer, but no
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_f6 (i int) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_f6_1 PARTITION OF zz_f6 FOR VALUES FROM (1) TO (10);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_f6_1 RENAME TO zz_f6_x;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_f6 VALUES (5);
-- begin-expected
-- columns: n:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) AS n FROM zz_f6_x;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_f7p (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_f7c () INHERITS (zz_f7p);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_f7c VALUES (2);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_f7p RENAME TO zz_f7p2;
-- begin-expected
-- columns: n:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) AS n FROM zz_f7p2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE UNLOGGED TABLE zz_u1 (a int PRIMARY KEY);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_u1 REPLICA IDENTITY FULL;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_u1 FORCE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_u1 RENAME TO zz_u2;
-- begin-expected
-- columns: relpersistence:char | relreplident:char | relrowsecurity:bool | relforcerowsecurity:bool
-- row: u | f | f | t
-- rowcount: 1
-- end-expected
SELECT relpersistence, relreplident, relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname='zz_u2';
