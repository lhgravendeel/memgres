-- source: investigation-2026-08.md
-- finding: 92
-- title: StatementCancel is polled in only a handful of loops; four long-running paths and every lock wait never check it, so statement_timeout, lock_timeout and client 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_to (i int, t text);
-- begin-expected
-- ok: 200000
-- end-expected
INSERT INTO zz_to SELECT g, md5(g::text) FROM generate_series(1, 200000) g;
-- begin-expected
-- ok: 0
-- end-expected
SET statement_timeout = '20ms';
-- begin-expected-error
-- sqlstate: 57014
-- message-like: canceling statement due to statement timeout
-- end-expected-error
DELETE FROM zz_to;
-- begin-expected-error
-- sqlstate: 57014
-- message-like: canceling statement due to statement timeout
-- end-expected-error
SELECT count(*) FROM (SELECT * FROM zz_to EXCEPT SELECT * FROM zz_to) x;
-- begin-expected-error
-- sqlstate: 57014
-- message-like: canceling statement due to statement timeout
-- end-expected-error
CREATE INDEX zz_toi ON zz_to (t);
-- session A: BEGIN; LOCK TABLE zz_lk IN ACCESS EXCLUSIVE MODE;  (left open)
-- session B: SET lock_timeout = '300ms'; INSERT INTO zz_lk VALUES (2,2);
-- begin-expected
-- ok: 0
-- end-expected
SET transaction_timeout = '500ms';
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- columns: ?column?:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT 1;
-- wait 1.5 s
-- begin-expected
-- columns: ?column?:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT 2;
