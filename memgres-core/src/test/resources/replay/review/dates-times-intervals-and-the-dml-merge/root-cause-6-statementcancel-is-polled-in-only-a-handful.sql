-- source: review-2026-08.md
-- finding: Root cause 6: StatementCancel is polled in only a handful of loops
-- area: Dates, times, intervals — and the DML/MERGE/COPY findings filed with them
-- title: Root cause 6: StatementCancel is polled in only a handful of loops
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
-- session A: BEGIN; LOCK TABLE zz_lk IN ACCESS EXCLUSIVE MODE;   (left open)
-- session B: SET lock_timeout = '300ms'; INSERT INTO zz_lk VALUES (2,2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_lk (i int PRIMARY KEY, v int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_lk VALUES (1,1);
-- session A: BEGIN; SELECT i FROM zz_lk WHERE i=1 FOR KEY SHARE;   (left open)
-- session B: UPDATE zz_lk SET v=99 WHERE i=1;
