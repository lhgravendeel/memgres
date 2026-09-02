-- source: review-2026-08.md
-- finding: Root cause 20: the wire layer scrapes the FETCH/EXECUTE object name out of the uppercased SQL text
-- area: PREPARE, EXECUTE, DO and CALL
-- title: Root cause 20: the wire layer scrapes the FETCH/EXECUTE object name out of the uppercased SQL text
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_r (id int);
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf2_pr AS INSERT INTO zz_vf2_r VALUES (9) RETURNING id AS a;
-- begin-expected
-- columns: a:int4
-- row: 9
-- rowcount: 1
-- end-expected
EXECUTE "zz_vf2_pr";
