-- source: investigation-2026-08.md
-- finding: 326
-- title: Deferred option errors are built by string concatenation — the token position and a "near '…'" fragment are glued into the primary message, the option name is u
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized EXPLAIN option "bogusopt"
-- end-expected-error
EXPLAIN (BOGUSOPT) SELECT 1;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized value for EXPLAIN option "format": "bogus"
-- end-expected-error
EXPLAIN (COSTS OFF, FORMAT BOGUS) SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: format requires a parameter
-- end-expected-error
EXPLAIN (COSTS OFF, FORMAT) SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: verbose requires a Boolean value
-- end-expected-error
EXPLAIN (VERBOSE bogus) SELECT 1;
