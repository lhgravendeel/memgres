-- source: investigation.md
-- finding: 71
-- title: `ALTER TYPE` and `ALTER DOMAIN` do not check attributes or data (6 cases)
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ct" does not exist
-- end-expected-error
ALTER TYPE ct ADD ATTRIBUTE a int;
-- a exists; PG: 42701 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ct" does not exist
-- end-expected-error
ALTER TYPE ct DROP ATTRIBUTE nosuch;
-- PG: 42703 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ct" does not exist
-- end-expected-error
ALTER TYPE ct RENAME ATTRIBUTE nosuch TO z;
-- PG: 42703 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ct" does not exist
-- end-expected-error
ALTER TYPE ct ALTER ATTRIBUTE nosuch TYPE int;
-- PG: 42703 | mg: OK

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "d" does not exist
-- end-expected-error
ALTER DOMAIN d SET NOT NULL;
-- a column of that domain holds NULL
--   PG: 23502 column "a" of table "t" contains null values | mg: OK
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "d" does not exist
-- end-expected-error
ALTER DOMAIN d DROP CONSTRAINT nosuch;
-- PG: 42704 | mg: OK
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "d" does not exist
-- end-expected-error
ALTER DOMAIN d VALIDATE CONSTRAINT c1;
-- existing rows violate it
--   PG: 23514 contains values that violate the new constraint | mg: OK;
