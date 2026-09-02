-- source: investigation-2026-08.md
-- finding: 186
-- title: A multi-statement simple Query is executed as N independent autocommit statements, so PostgreSQL's implicit transaction block does not exist and a failure halfw
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_imp (a int);
-- sent as ONE simple Query message:
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_imp VALUES (1);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch_zz_vf" does not exist
-- end-expected-error
SELECT nosuch_zz_vf;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_imp;
