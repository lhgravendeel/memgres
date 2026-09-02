-- source: investigation.md
-- finding: 33
-- title: Deferred foreign keys are not deferred ⚠️ high — rejects valid SQL
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "parent" does not exist
-- end-expected-error
CREATE TABLE c (p int REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "c" does not exist
-- end-expected-error
INSERT INTO c VALUES (77);
-- no such parent yet; the whole point of deferring
--   PG: OK — checked at commit | mg: 23503 violates foreign key constraint, immediately
-- begin-expected
-- ok: 0
-- end-expected
SET CONSTRAINTS ALL IMMEDIATE;
-- with a pending violation; PG: 23503 | mg: OK, no check
-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "no_such_name" does not exist
-- end-expected-error
SET CONSTRAINTS no_such_name DEFERRED;
-- PG: 42704 does not exist        | mg: OK
-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "non_deferrable" does not exist
-- end-expected-error
SET CONSTRAINTS non_deferrable DEFERRED;
-- PG: 42809 is not deferrable     | mg: OK;
