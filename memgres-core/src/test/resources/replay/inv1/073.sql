-- source: investigation.md
-- finding: 73
-- title: `ALTER SEQUENCE` bound validation (3 cases)
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "s" does not exist
-- end-expected-error
ALTER SEQUENCE s MINVALUE 100;
-- current START is 1; PG: 22023 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "s" does not exist
-- end-expected-error
ALTER SEQUENCE s INCREMENT BY 0;
-- PG: 22023 INCREMENT must not be zero | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "s" does not exist
-- end-expected-error
ALTER SEQUENCE s MAXVALUE 10 MINVALUE 20;
-- PG: 22023 MINVALUE must be less than MAXVALUE | mg: OK;
