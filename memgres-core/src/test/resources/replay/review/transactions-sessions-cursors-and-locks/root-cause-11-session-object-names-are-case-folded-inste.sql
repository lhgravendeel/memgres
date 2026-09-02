-- source: review-2026-08.md
-- finding: Root cause 11: session object names are case-folded instead of being kept as parsed identifiers
-- area: Transactions, sessions, cursors and locks
-- title: Root cause 11: session object names are case-folded instead of being kept as parsed identifiers
-- begin-expected
-- ok: 0
-- end-expected
PREPARE "ZzVfPrep" AS SELECT 42;
-- begin-expected-error
-- sqlstate: 26000
-- message-like: prepared statement "zzvfprep" does not exist
-- end-expected-error
EXECUTE zzvfprep;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
DECLARE "ZzVfCase" CURSOR FOR SELECT 7;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zzvfcase" does not exist
-- end-expected-error
FETCH ALL FROM zzvfcase;
