-- source: review-2026-08.md
-- finding: Three literal forms the lexer does not implement
-- area: Parser and identifier resolution
-- title: Three literal forms the lexer does not implement
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
-- columns: ?column?:int4
-- row: 16
-- rowcount: 1
-- end-expected
SELECT 0x10;
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
-- message-like: syntax error at or near "$"
-- end-expected-error
SELECT $ 'hello' $;
