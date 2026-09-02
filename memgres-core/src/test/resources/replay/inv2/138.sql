-- source: investigation-2026-08.md
-- finding: 138
-- title: Constraint and DDL-visibility checks read the live structures directly, without the MVCC rules the rest of the engine honours: FK checks see other sessions' unc
-- session A
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_ta AS (x int);
-- session B
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_tb AS (y int);
-- left open
-- session A
-- begin-expected
-- ok: 0
-- end-expected
DROP TYPE zz_vf_ta;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_type WHERE typname='zz_vf_ta';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_vf_ta" does not exist
-- end-expected-error
CREATE TABLE zz_vf_tt2 (c zz_vf_ta);
-- session A
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pp2 (id int PRIMARY KEY, note text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_cc (id int PRIMARY KEY, p int REFERENCES zz_vf_pp2(id));
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_pp2 VALUES (1,'a');
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_cc VALUES (1,1);
-- not committed
-- session B
-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "zz_vf_pp2" violates foreign key constraint "zz_vf_cc_p_fkey" on table "zz_vf_cc"
-- end-expected-error
DELETE FROM zz_vf_pp2 WHERE id=1;
