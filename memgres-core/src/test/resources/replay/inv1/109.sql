-- source: investigation.md
-- finding: 109
-- title: `ALTER TABLE ALTER CONSTRAINT` option validation (3 cases)
-- begin-expected-error
-- sqlstate: 42601
-- message-like: constraint declared INITIALLY DEFERRED must be DEFERRABLE
-- end-expected-error
ALTER TABLE t ALTER CONSTRAINT c NOT DEFERRABLE INITIALLY DEFERRED;
--   PG: 42601 constraint declared INITIALLY DEFERRED must be DEFERRABLE | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER CONSTRAINT c NO INHERIT;
-- c is a foreign key
--   PG: 42809 constraint is not a not-null constraint | mg: OK
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: constraints cannot be altered to be NOT VALID
-- end-expected-error
ALTER TABLE t ALTER CONSTRAINT c NOT VALID;
--   PG: 0A000 constraints cannot be altered to be NOT VALID | mg: OK;
