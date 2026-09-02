-- source: investigation.md
-- finding: 114
-- title: Inheritance `ALTER` validation
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "child" does not exist
-- end-expected-error
ALTER TABLE child DROP COLUMN a;
-- inherited; PG: 42P16 cannot drop inherited column | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "parent" does not exist
-- end-expected-error
ALTER TABLE ONLY parent ADD COLUMN d int;
-- PG: 42P16 column must be added to child tables too | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "c" does not exist
-- end-expected-error
ALTER TABLE c INHERIT p;
-- c is missing a column of p
--   PG: 42804 child table is missing column "b" | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "c" does not exist
-- end-expected-error
ALTER TABLE c INHERIT p;
-- c has a different type for a shared column
--   PG: 42804 child table has different type for column "a" | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "c" does not exist
-- end-expected-error
ALTER TABLE c NO INHERIT p;
-- p is not a parent of c
--   PG: 42P01 relation is not a parent | mg: OK;
