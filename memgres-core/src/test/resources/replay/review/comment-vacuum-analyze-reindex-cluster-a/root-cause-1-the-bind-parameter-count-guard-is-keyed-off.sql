-- source: review-2026-08.md
-- finding: Root cause 1: the Bind parameter-count guard is keyed off the first word of the statement, and MERGE is not one of the words
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Root cause 1: the Bind parameter-count guard is keyed off the first word of the statement, and MERGE is not one of the words
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_bt (a int, b int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_bt VALUES (1, 1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_bs (a int, b int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_bs VALUES (1, 9);
-- Parse 'MERGE INTO zz_bt t USING zz_bs s ON t.a = s.a WHEN MATCHED THEN UPDATE SET b = $1'
-- then Bind with zero parameter values, Execute, Sync
-- begin-expected
-- columns: b:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT b FROM zz_bt;
