-- source: investigation.md
-- finding: 40
-- title: Transaction-control statements outside a transaction block
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: SAVEPOINT can only be used in transaction blocks
-- end-expected-error
SAVEPOINT s;
-- PG: 25P01 can only be used in transaction blocks | mg: OK
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: RELEASE SAVEPOINT can only be used in transaction blocks
-- end-expected-error
RELEASE SAVEPOINT s;
-- PG: 25P01 | mg: OK
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: ROLLBACK AND CHAIN can only be used in transaction blocks
-- end-expected-error
ROLLBACK AND CHAIN;
-- PG: 25P01 | mg: OK
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: COMMIT AND CHAIN can only be used in transaction blocks
-- end-expected-error
COMMIT AND CHAIN;
-- PG: 25P01 | mg: OK  (also recorded as #29)
-- begin-expected
-- ok: 0
-- end-expected
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
-- after a query has run in the transaction
--   PG: 25001 must be called before any query | mg: OK;
