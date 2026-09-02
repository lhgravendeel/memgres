-- source: investigation-2026-08.md
-- finding: 303
-- title: The Bind parameter-count guard is keyed off the first word of the statement: maxParamPlaceholder returns 0 unless the trimmed, uppercased text starts with SELEC
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_bt (a int, b int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_bt VALUES (1,1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_bs (a int, b int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_bs VALUES (1,9);
-- Parse 'MERGE INTO zz_bt t USING zz_bs s ON t.a=s.a WHEN MATCHED THEN UPDATE SET b=$1'
-- then Bind with zero parameter values, Execute, Sync
-- begin-expected
-- columns: b:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT b FROM zz_bt;
-- begin-expected
-- ok: 0
-- end-expected
SET SEED TO 0.5;
-- begin-expected
-- ok: 0
-- end-expected
SET seed = 0.5;
-- begin-expected
-- columns: seed:text
-- row: unavailable
-- rowcount: 1
-- end-expected
SHOW seed;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: 2 is outside the valid range for parameter "seed" (-1 .. 1)
-- end-expected-error
SET SEED TO 2;
