-- source: review-2026-08.md
-- finding: Root cause 11: ParameterStatus is emitted by a substring match over statements that begin "SET "
-- area: Catalog builders and the wire layer, second pass
-- title: Root cause 11: ParameterStatus is emitted by a substring match over statements that begin "SET "
-- begin-expected-error
-- sqlstate: 08006
-- message-like: The server's client_encoding parameter was changed to LATIN1. The JDBC driver requires client_encoding to be UTF8 for correct operation.
-- end-expected-error
SET client_encoding TO 'LATIN1';
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SET search_path TO pg_catalog, public;
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
RESET application_name;
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SELECT set_config('application_name','zz_vf2_two',false);
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
BEGIN;
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SET LOCAL application_name TO 'zz_vf2_local';
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
COMMIT;
