-- source: investigation-2026-08.md
-- finding: 214
-- title: Relation locks exist only on the DML write path: checkTableLockForDml has one caller, the DDL executors take no relation lock, DECLARE CURSOR takes none either,
-- session B: BEGIN; SELECT count(*) FROM zz_vf2_dl;
-- session A, SET lock_timeout='2000ms':
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: table "zz_vf2_dl" does not exist
-- end-expected-error
DROP TABLE zz_vf2_dl;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_c_d" does not exist
-- end-expected-error
DECLARE zz_c_dc CURSOR FOR SELECT id FROM zz_c_d ORDER BY id;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zz_c_dc" does not exist
-- end-expected-error
FETCH 1 FROM zz_c_dc;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_c_d" does not exist
-- end-expected-error
TRUNCATE zz_c_d;
-- session B: BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM zz_vf2_mv;
-- session A: UPDATE zz_vf2_mv SET v=99 WHERE i=1;
-- session B:
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_mv" does not exist
-- end-expected-error
SELECT v FROM zz_vf2_mv WHERE i=1 FOR UPDATE;
