-- source: investigation-2026-08.md
-- finding: 156
-- title: DDL destroys before it resolves: a multi-object DROP drops as it walks the name list, CREATE ... IF NOT EXISTS / OR REPLACE on a materialized view re-runs the q
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_z5 (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf_zi1 ON zz_vf_z5 (a);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: index "zz_vf_nosuchidx" does not exist
-- end-expected-error
DROP INDEX zz_vf_zi1, zz_vf_nosuchidx;
-- begin-expected
-- columns: n:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_indexes WHERE indexname = 'zz_vf_zi1';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_v8t (i int, v text);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_v8t VALUES (1,'a'),(2,'b');
-- begin-expected
-- ok: 2
-- end-expected
CREATE MATERIALIZED VIEW zz_vf_v8m AS SELECT i, v FROM zz_vf_v8t;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_v8t VALUES (3,'c');
-- begin-expected
-- ok: 0
-- end-expected
CREATE MATERIALIZED VIEW IF NOT EXISTS zz_vf_v8m AS SELECT i, v FROM zz_vf_v8t;
-- begin-expected
-- columns: mv_rows:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*)::int AS mv_rows FROM zz_vf_v8m;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_hp2 (i int PRIMARY KEY) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_hp2_1 PARTITION OF zz_vf_hp2 FOR VALUES FROM (1) TO (10);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_hp2 VALUES (5);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_hd (j int REFERENCES zz_vf_hp2(i));
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_hd VALUES (5);
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table zz_vf_hp2_1 because other objects depend on it
-- end-expected-error
DROP TABLE zz_vf_hp2_1;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf_v8t" already exists
-- end-expected-error
CREATE TABLE zz_vf_v8t (i int, v text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_v8t VALUES (1,'a');
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf_v8m" already exists
-- end-expected-error
CREATE MATERIALIZED VIEW zz_vf_v8m AS SELECT i, v FROM zz_vf_v8t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "MATERIALIZED"
-- end-expected-error
CREATE OR REPLACE MATERIALIZED VIEW zz_vf_v8m AS SELECT i, v FROM zz_vf_v8t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "MATERIALIZED"
-- end-expected-error
CREATE TEMP MATERIALIZED VIEW zz_vf_v8m2 AS SELECT 1;
