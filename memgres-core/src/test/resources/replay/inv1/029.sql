-- source: investigation.md
-- finding: 29
-- title: `COMMIT AND CHAIN` outside a transaction block
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: COMMIT AND CHAIN can only be used in transaction blocks
-- end-expected-error
COMMIT AND CHAIN;
-- PG: 25P01 can only be used in transaction blocks | mg: OK;
