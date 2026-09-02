-- source: review-2026-08.md
-- finding: Root cause 3: the per-row MVCC metadata is a stub — xmax and cmax are literal zeros and the ctid is an unreset counter
-- area: System columns, row locking and TABLESAMPLE
-- title: Root cause 3: the per-row MVCC metadata is a stub — xmax and cmax are literal zeros and the ctid is an unreset counter
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_lk (id int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_lk VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- columns: id:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT id FROM zz_vf2_lk WHERE id=1 FOR UPDATE;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT xmax::text <> '0' FROM zz_vf2_lk WHERE id=1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_cm (id int);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_cm VALUES (10);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_cm VALUES (11);
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(DISTINCT cmin::text) FROM zz_vf2_cm WHERE id IN (10,11);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ct (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_ct VALUES (1),(2),(3);
-- begin-expected
-- ok: 0
-- end-expected
TRUNCATE zz_vf2_ct;
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf2_ct VALUES (7),(8);
-- begin-expected
-- columns: id:int4 | ctid:text
-- row: 7 | (0,1)
-- row: 8 | (0,2)
-- rowcount: 2
-- end-expected
SELECT id, ctid::text FROM zz_vf2_ct ORDER BY id;
