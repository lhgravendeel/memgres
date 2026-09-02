-- source: investigation-2026-08.md
-- finding: 80
-- title: A Java library exception is not translated at the call site, so it reaches the client as XX000 'Internal error' — new sites beyond the ones the August report li
-- begin-expected
-- columns: right:text
-- row: abc
-- rowcount: 1
-- end-expected
SELECT right('abc', -2147483648);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_str" does not exist
-- end-expected-error
SELECT right(s, i) FROM zz_vf2_str;
-- i = -2147483648, s = 'abc'
-- begin-expected
-- columns: format:text
-- row: y
-- rowcount: 1
-- end-expected
SELECT format('%*s', NULL, 'y');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT format('%*s', 'abc', 'y');
-- begin-expected
-- columns: text:text
-- row: 'a':16383
-- rowcount: 1
-- end-expected
SELECT 'a:99999999999999999999'::tsvector::text;
-- begin-expected
-- columns: ?column?:tsvector
-- row: 'a':1,16383
-- rowcount: 1
-- end-expected
SELECT 'a:1'::tsvector || 'a:99999999999999999999'::tsvector;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_tv (v tsvector);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: wrong position info in tsvector: "a:2147483648"
-- end-expected-error
INSERT INTO zz_vf2_tv VALUES ('a:2147483648');
