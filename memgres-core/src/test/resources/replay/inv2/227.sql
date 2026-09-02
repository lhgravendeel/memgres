-- source: investigation-2026-08.md
-- finding: 227
-- title: a holdable cursor is only marked committed by an explicit COMMIT, and the rollback path deletes every holdable cursor that is not marked
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
