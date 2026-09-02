-- source: investigation-2026-08.md
-- finding: 375
-- title: evaluateDefault evaluates the default inside catch(Exception) and, on failure, returns the default's own source text as the column value.
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
