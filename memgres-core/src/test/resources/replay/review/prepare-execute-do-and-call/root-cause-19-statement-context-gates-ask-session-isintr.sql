-- source: review-2026-08.md
-- finding: Root cause 19: statement-context gates ask session.isInTransaction(), which a DO block's implicit transaction leaves false
-- area: PREPARE, EXECUTE, DO and CALL
-- title: Root cause 19: statement-context gates ask session.isInTransaction(), which a DO block's implicit transaction leaves false
-- begin-expected-error
-- sqlstate: 25001
-- message-like: DISCARD ALL cannot be executed from a function
-- end-expected-error
DO $$ BEGIN EXECUTE 'DISCARD ALL'; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_vt2 (id int);
-- begin-expected-error
-- sqlstate: 25001
-- message-like: VACUUM cannot be executed from a function
-- end-expected-error
DO $$ BEGIN VACUUM zz_vf2_vt2; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf2_vfn() RETURNS void LANGUAGE plpgsql AS $$ BEGIN EXECUTE 'VACUUM zz_vf2_vt2'; END $$;
-- begin-expected-error
-- sqlstate: 25001
-- message-like: VACUUM cannot be executed from a function
-- end-expected-error
SELECT zz_vf2_vfn();
-- begin-expected
-- ok: 0
-- end-expected
BEGIN READ ONLY;
-- begin-expected
-- columns: ?column?:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT 1;
-- begin-expected-error
-- sqlstate: 25001
-- message-like: transaction read-write mode must be set before any query
-- end-expected-error
SET TRANSACTION READ WRITE;
