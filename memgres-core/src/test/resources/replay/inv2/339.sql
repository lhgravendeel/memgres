-- source: investigation-2026-08.md
-- finding: 339
-- title: The parser has no ColId/ColLabel/type_func_name keyword categories. readIdentifier accepts any KEYWORD token as a name, so every reserved word can be a column, 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_cn (exists int, position int, coalesce int, nullif int, greatest int, least int, trim int, substring int, overlay int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_cn VALUES (1,2,3,4,5,6,7,8,9);
-- begin-expected
-- columns: exists:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT exists FROM zz_cn;
-- begin-expected
-- columns: trim:int4
-- row: 7
-- rowcount: 1
-- end-expected
SELECT trim FROM zz_cn;
-- begin-expected
-- columns: greatest:int4
-- row: 5
-- rowcount: 1
-- end-expected
SELECT greatest FROM zz_cn;
-- begin-expected
-- columns: a:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT user IS NOT NULL AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "order"
-- end-expected-error
CREATE TABLE zz_q6 (order int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "select"
-- end-expected-error
CREATE TABLE zz_q6b (select int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "user"
-- end-expected-error
CREATE TABLE zz_q6c (user int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "from"
-- end-expected-error
CREATE TABLE zz_q6d (from int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "left"
-- end-expected-error
CREATE TABLE zz_tfn (left int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t VALUES (1);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "left"
-- end-expected-error
SELECT * FROM zz_t AS left;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "inner"
-- end-expected-error
SELECT * FROM zz_t AS inner;
