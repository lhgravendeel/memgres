-- source: review-2026-08.md
-- finding: Root cause 3: TRUNCATE's schema search omits the temp namespace
-- area: Wire protocol and error-report fidelity
-- title: Root cause 3: TRUNCATE's schema search omits the temp namespace
-- begin-expected
-- ok: 0
-- end-expected
CREATE TEMP TABLE zz_vf_tr (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_tr VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
TRUNCATE zz_vf_tr;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_tr;
