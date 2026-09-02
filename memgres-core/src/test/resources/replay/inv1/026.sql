-- source: investigation.md
-- finding: 26
-- title: Sequence option validation is absent
-- begin-expected-error
-- sqlstate: 22023
-- message-like: START value (100) cannot be greater than MAXVALUE (10)
-- end-expected-error
CREATE SEQUENCE s START 100 MINVALUE 1 MAXVALUE 10;
--   PG: 22023 START value (100) cannot be greater than MAXVALUE (10) | mg: OK
-- begin-expected-error
-- sqlstate: 22023
-- message-like: CACHE (0) must be greater than zero
-- end-expected-error
CREATE SEQUENCE s CACHE 0;
--   PG: 22023 CACHE (0) must be greater than zero                    | mg: OK;
