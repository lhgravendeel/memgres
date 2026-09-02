-- source: investigation-2026-08.md
-- finding: 128
-- title: The lexer's escape and identifier handling: an unrecognised escape keeps its backslash, surrogates and NUL are not checked, identifiers are clipped by UTF-16 le
-- begin-expected
-- columns: a:text
-- row: z
-- rowcount: 1
-- end-expected
SELECT E'\z' AS a;
-- begin-expected
-- columns: a:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT length(E'\z') AS a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_q5 ("ÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜ" int);
-- begin-expected
-- columns: bytes:int4
-- row: 62
-- rowcount: 1
-- end-expected
SELECT octet_length(column_name) AS bytes FROM information_schema.columns WHERE table_name = 'zz_q5';
-- begin-expected
-- columns: ?column?:int4
-- row: 31
-- rowcount: 1
-- end-expected
SELECT 0x1F;
-- begin-expected
-- columns: ?column?:int4
-- row: 15
-- rowcount: 1
-- end-expected
SELECT 0o17;
-- begin-expected
-- columns: ?column?:int4
-- row: 10
-- rowcount: 1
-- end-expected
SELECT 0b1010;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: invalid Unicode surrogate pair
-- end-expected-error
SELECT U&'\d801' AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: invalid Unicode surrogate pair at or near "'"
-- end-expected-error
SELECT E'\uD801' AS a;
-- begin-expected-error
-- sqlstate: 22021
-- message-like: invalid byte sequence for encoding "UTF8": 0x00
-- end-expected-error
SELECT length(E'\0') AS a;
