-- source: investigation-2026-08.md
-- finding: 340
-- title: The lexer's literal syntax is missing three PostgreSQL rules: the doubled escape character in U&'...' (and U&"...") that is the only way to write a literal back
-- begin-expected
-- columns: a:text
-- row: a\b
-- rowcount: 1
-- end-expected
SELECT U&'a\\b' AS a;
-- begin-expected
-- columns: a:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT length(U&'\\') AS a;
-- begin-expected
-- columns: a:int4
-- row: 16
-- rowcount: 1
-- end-expected
SELECT 0x10 AS a;
-- begin-expected
-- columns: a:int4
-- row: 15
-- rowcount: 1
-- end-expected
SELECT 0o17 AS a;
-- begin-expected
-- columns: a:int4
-- row: 10
-- rowcount: 1
-- end-expected
SELECT 0b1010 AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "$"
-- end-expected-error
SELECT $ 'hello' $;
