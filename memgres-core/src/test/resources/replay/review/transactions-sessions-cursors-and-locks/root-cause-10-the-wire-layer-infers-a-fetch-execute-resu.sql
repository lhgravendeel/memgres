-- source: review-2026-08.md
-- finding: Root cause 10: the wire layer infers a FETCH/EXECUTE result shape by string-matching the SQL text
-- area: Transactions, sessions, cursors and locks
-- title: Root cause 10: the wire layer infers a FETCH/EXECUTE result shape by string-matching the SQL text
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_c (i int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_c VALUES (1),(2),(3);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
DECLARE zz_vf_x7 CURSOR FOR SELECT i FROM zz_vf_c ORDER BY i;
-- begin-expected
-- columns: i:int4
-- row: 1
-- rowcount: 1
-- end-expected
FETCH zz_vf_x7;
-- bare form, no FROM/IN
-- begin-expected
-- ok: 0
-- end-expected
DECLARE "ZzVfCase2" CURSOR FOR SELECT 7;
-- begin-expected
-- columns: ?column?:int4
-- row: 7
-- rowcount: 1
-- end-expected
FETCH ALL FROM "ZzVfCase2";
-- quoted cursor name
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- ok: 0
-- end-expected
PREPARE "ZzVfPrep" AS SELECT 42;
-- begin-expected
-- columns: ?column?:int4
-- row: 42
-- rowcount: 1
-- end-expected
EXECUTE "ZzVfPrep";
-- quoted plan name;
