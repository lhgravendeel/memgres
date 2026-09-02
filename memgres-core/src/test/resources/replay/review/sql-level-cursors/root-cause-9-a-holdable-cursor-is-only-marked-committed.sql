-- source: review-2026-08.md
-- finding: Root cause 9: a holdable cursor is only marked committed by an explicit COMMIT
-- area: SQL-level cursors
-- title: Root cause 9: a holdable cursor is only marked committed by an explicit COMMIT
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_w" does not exist
-- end-expected-error
DECLARE zz_wh CURSOR WITH HOLD FOR SELECT i FROM zz_w ORDER BY i;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zz_wh" does not exist
-- end-expected-error
FETCH ALL FROM zz_wh;
