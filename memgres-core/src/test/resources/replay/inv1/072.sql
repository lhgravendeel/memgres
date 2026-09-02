-- source: investigation.md
-- finding: 72
-- title: `ALTER TABLE ALTER COLUMN` accepts contradictory changes
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER COLUMN id DROP NOT NULL;
-- id is the primary key
--   PG: 42P16 column "id" is in a primary key | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER COLUMN v SET DEFAULT 5;
-- v is a generated column
--   PG: 42601 column "v" is a generated column | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER COLUMN w DROP IDENTITY;
-- w is not an identity column
--   PG: 55000 is not an identity column | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t SET SCHEMA nosuch_schema;
-- PG: 3F000 | mg: OK;
