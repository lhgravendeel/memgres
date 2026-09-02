-- source: investigation.md
-- finding: 111
-- title: System column names can be reused ⚠️
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column name "xmax" conflicts with a system column name
-- end-expected-error
CREATE TABLE t (xmax int);
-- PG: 42701 conflicts with a system column name | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ADD COLUMN xmin integer;
-- PG: 42701 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ADD COLUMN ctid integer;
-- PG: 42701 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t RENAME COLUMN i TO tableoid;
-- PG: 42701 | mg: OK;
