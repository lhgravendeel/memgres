-- source: investigation.md
-- finding: 116
-- title: `ALTER COLUMN … DROP EXPRESSION` is unsupported
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER COLUMN b DROP EXPRESSION;
-- PG: works | mg: 42601 syntax error
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER COLUMN b DROP EXPRESSION IF EXISTS;
-- PG: works | mg: 42601;
