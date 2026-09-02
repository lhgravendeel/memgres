-- source: review-2026-08.md
-- finding: Root cause 10: SET's three forms disagree about what a value is
-- area: The parsers
-- title: Root cause 10: SET's three forms disagree about what a value is
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_sp (id int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_sp VALUES (1);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SET search_path;
-- begin-expected
-- columns: search_path:text
-- row: "$user", public
-- rowcount: 1
-- end-expected
SHOW search_path;
-- begin-expected
-- columns: id:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT id FROM zz_vf2_sp;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SET CONSTRAINTS ALL;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'4MB'"
-- end-expected-error
SET work_mem '4MB' rubbish;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'ISO'"
-- end-expected-error
SET datestyle 'ISO' junk here;
