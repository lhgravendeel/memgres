-- source: review-2026-08.md
-- finding: Root cause 5: a DEFAULT that raises is replaced by the expression's own source text
-- area: The engine core: statement execution, tables, windows, joins and deparsing
-- title: Root cause 5: a DEFAULT that raises is replaced by the expression's own source text
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_dt (a text DEFAULT (1/0));
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
INSERT INTO zz_vf2_dt DEFAULT VALUES;
-- begin-expected
-- columns: a:text
-- rowcount: 0
-- end-expected
SELECT a FROM zz_vf2_dt;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_dz (a int DEFAULT (1/0));
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
INSERT INTO zz_vf2_dz DEFAULT VALUES;
