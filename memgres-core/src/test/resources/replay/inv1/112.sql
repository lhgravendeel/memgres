-- source: investigation.md
-- finding: 112
-- title: `DEFAULT` expression validation is absent
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER COLUMN c1 SET DEFAULT 'wrong_datatype';
-- c1 is int
--   PG: 22P02 invalid input syntax for type integer | mg: OK, fails later at INSERT
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER COLUMN c1 SET DEFAULT (SELECT 1);
--   PG: 0A000 cannot use subquery in DEFAULT expression | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER COLUMN c1 SET DEFAULT c2;
--   PG: 0A000 cannot use column reference in DEFAULT expression | mg: OK;
