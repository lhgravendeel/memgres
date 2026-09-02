-- source: investigation-2026-08.md
-- finding: 188
-- title: Table-level locks are a write-path courtesy with a two-mode compatibility stub: reads never take ACCESS SHARE, and tableLockModesCompatible returns true for eve
-- session A                                   -- session B
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_lk (i int PRIMARY KEY, v int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_lk VALUES (1,1),(2,2);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
LOCK TABLE zz_vf_lk IN ACCESS EXCLUSIVE MODE;
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_lk;
-- session A                                   -- session B
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_lm (i int);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
LOCK TABLE zz_vf_lm IN SHARE MODE;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
LOCK TABLE zz_vf_lm IN ROW EXCLUSIVE MODE NOWAIT;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_lm VALUES (1);
