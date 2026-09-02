-- source: investigation-2026-08.md
-- finding: 352
-- title: SET's three forms disagree about what a value is: the =/TO path guards against an empty value, the bare `SET name value` path guards against nothing and swallow
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_sp (id int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_sp VALUES (1);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SET search_path;
-- begin-expected
-- columns: search_path:text
-- row: "$user", public
-- rowcount: 1
-- end-expected
SHOW search_path;
-- begin-expected
-- columns: id:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT id FROM zz_vf2_sp;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SET CONSTRAINTS ALL;
