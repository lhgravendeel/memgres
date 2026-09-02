-- source: review-2026-08.md
-- finding: Root cause 2: CALL and DO have no transaction frame of their own
-- area: PREPARE, EXECUTE, DO and CALL
-- title: Root cause 2: CALL and DO have no transaction frame of their own
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_tt (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_vf2_tp(n int) LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zz_vf2_tt VALUES (n); END $$;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_tt VALUES (3);
-- begin-expected
-- ok: -1
-- end-expected
CALL zz_vf2_tp(4);
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: id:int4
-- rowcount: 0
-- end-expected
SELECT id FROM zz_vf2_tt ORDER BY id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_dt (id int);
-- begin-expected
-- ok: 0
-- end-expected
DO $$ BEGIN ROLLBACK; END $$;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_dt VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf2_dt;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unsupported transaction command in PL/pgSQL
-- end-expected-error
DO $$ BEGIN START TRANSACTION; END $$;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 1/0;
-- begin-expected
-- columns: ?column?:text
-- row: after
-- rowcount: 1
-- end-expected
SELECT 'after';
-- begin-expected
-- columns: ?column?:text
-- row: again
-- rowcount: 1
-- end-expected
SELECT 'again';
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_vf2_cc() LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zz_vf2_cl VALUES ('a'); COMMIT; INSERT INTO zz_vf2_cl VALUES ('b'); END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_vf2_ce() LANGUAGE plpgsql AS $$ BEGIN CALL zz_vf2_cc(); EXCEPTION WHEN OTHERS THEN INSERT INTO zz_vf2_cl VALUES ('caught ' || SQLSTATE); END $$;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_cl" does not exist
-- end-expected-error
CALL zz_vf2_ce();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_cl" does not exist
-- end-expected-error
SELECT msg FROM zz_vf2_cl ORDER BY msg;
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_vf2_sd() LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN COMMIT; END $$;
-- begin-expected-error
-- sqlstate: 2D000
-- message-like: invalid transaction termination
-- end-expected-error
CALL zz_vf2_sd();
