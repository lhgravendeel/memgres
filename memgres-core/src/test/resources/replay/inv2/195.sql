-- source: investigation-2026-08.md
-- finding: 195
-- title: Session object names (prepared statements, cursors) are stored and looked up through name.toLowerCase() rather than as parsed identifiers, so a double-quoted mi
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
