-- source: review-2026-08.md
-- finding: Root cause 2: the six system column names are answered before any FROM item is consulted
-- area: System columns, row locking and TABLESAMPLE
-- title: Root cause 2: the six system column names are answered before any FROM item is consulted
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_o1 (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_o2 (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_o1 VALUES (1),(2),(3);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf2_o2 VALUES (1),(2);
-- begin-expected
-- columns: id:int4 | ?column?:bool
-- row: 1 | f
-- row: 2 | f
-- row: 3 | t
-- rowcount: 3
-- end-expected
SELECT a.id, b.ctid IS NULL FROM zz_vf2_o1 a LEFT JOIN zz_vf2_o2 b ON a.id=b.id ORDER BY a.id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_j1 (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_j2 (id int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_j1 VALUES (1);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_j2 VALUES (1);
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "ctid" is ambiguous
-- end-expected-error
SELECT ctid::text FROM zz_vf2_j1, zz_vf2_j2;
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "tableoid" is ambiguous
-- end-expected-error
SELECT tableoid FROM zz_vf2_j1, zz_vf2_j2;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "xmin" does not exist
-- end-expected-error
SELECT xmin::text FROM zz_vf2_j1 a JOIN zz_vf2_j2 b ON a.id=b.id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_sv (id int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_sv VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_svv AS SELECT id FROM zz_vf2_sv;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "ctid" does not exist
-- end-expected-error
SELECT ctid::text FROM zz_vf2_svv;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "ctid" does not exist
-- end-expected-error
SELECT ctid::text FROM (SELECT id FROM zz_vf2_sv) s;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "ctid" does not exist
-- end-expected-error
WITH c AS (SELECT id FROM zz_vf2_sv) SELECT ctid::text FROM c;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "ctid" does not exist
-- end-expected-error
SELECT ctid::text FROM (VALUES (1)) v(x);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "tableoid" does not exist
-- end-expected-error
SELECT tableoid FROM generate_series(1,1) g;
