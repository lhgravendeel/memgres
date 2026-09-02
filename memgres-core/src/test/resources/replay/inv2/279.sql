-- source: investigation-2026-08.md
-- finding: 279
-- title: Binary COPY FROM is a second implementation of COPY that lacks the first one's per-row rollback: the text/CSV path wraps executeCopyFromRow in a try that calls 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_cp (a int, b int NOT NULL);
-- CopyManager: COPY zz_vf2_cp FROM STDIN (FORMAT binary) with rows (1,1) (2,2) (3,NULL)
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf2_cp;
