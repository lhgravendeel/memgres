-- source: review-2026-08.md
-- finding: Root cause 2: MERGE commits with `System.arraycopy` onto the live row
-- area: Arrays, ranges and multiranges
-- title: Root cause 2: MERGE commits with `System.arraycopy` onto the live row
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_mt2 (id int PRIMARY KEY, v int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ms2 (id int, v int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_mt2 VALUES (1,10),(2,20);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_ms2 VALUES (1,111),(2,222);
-- begin-expected
-- ok: 2
-- end-expected
MERGE INTO zz_vf_mt2 t USING zz_vf_ms2 s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET id = t.id + 100, v = s.v;
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zz_vf_mt2_pkey"
-- end-expected-error
INSERT INTO zz_vf_mt2 VALUES (101, 999);
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_mt2;
-- begin-expected
-- columns: id:int4 | v:int4
-- row: 101 | 111
-- rowcount: 1
-- end-expected
SELECT id, v FROM zz_vf_mt2 WHERE id = 101 ORDER BY v;
