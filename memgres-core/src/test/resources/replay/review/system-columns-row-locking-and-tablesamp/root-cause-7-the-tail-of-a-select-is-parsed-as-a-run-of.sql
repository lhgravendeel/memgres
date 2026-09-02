-- source: review-2026-08.md
-- finding: Root cause 7: the tail of a SELECT is parsed as a run of individually-optional keywords
-- area: System columns, row locking and TABLESAMPLE
-- title: Root cause 7: the tail of a SELECT is parsed as a run of individually-optional keywords
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_fl (id int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_fl VALUES (1);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SELECT id FROM zz_vf2_fl FOR NO;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SELECT id FROM zz_vf2_fl FOR NO KEY;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SELECT id FROM zz_vf2_fl FOR KEY;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SELECT id FROM zz_vf2_fl FOR UPDATE SKIP;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ol (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_ol VALUES (1),(2),(3);
-- begin-expected
-- columns: id:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT id FROM zz_vf2_ol ORDER BY id OFFSET 1 LIMIT 1 FOR UPDATE;
-- begin-expected
-- columns: id:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT id FROM zz_vf2_ol ORDER BY id FOR UPDATE OFFSET 1 LIMIT 1;
