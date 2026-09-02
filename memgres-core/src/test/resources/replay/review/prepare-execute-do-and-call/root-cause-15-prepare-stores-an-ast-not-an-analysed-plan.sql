-- source: review-2026-08.md
-- finding: Root cause 15: PREPARE stores an AST, not an analysed plan
-- area: PREPARE, EXECUTE, DO and CALL
-- title: Root cause 15: PREPARE stores an AST, not an analysed plan
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf2_q7 AS SELECT 1 AS v INTO zz_vf2_newtab;
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf2_q6 (int) AS SELECT $1 + $2 AS v;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ct (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_ct VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf2_pl AS SELECT * FROM zz_vf2_ct;
-- begin-expected
-- columns: a:int4
-- row: 1
-- rowcount: 1
-- end-expected
EXECUTE zz_vf2_pl;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf2_ct ADD COLUMN b int;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cached plan must not change result type
-- end-expected-error
EXECUTE zz_vf2_pl;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf2_ct ALTER COLUMN a TYPE text;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cached plan must not change result type
-- end-expected-error
EXECUTE zz_vf2_pl;
