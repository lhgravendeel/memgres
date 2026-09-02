-- source: investigation-2026-08.md
-- finding: 69
-- title: The lexer applies Java case folding to unquoted identifiers and is missing three SQL literal forms (UESCAPE, adjacent-literal continuation across a newline, N'.
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ident1 (MÜLLER int);
-- begin-expected
-- columns: column_name:name
-- row: mÜller
-- rowcount: 1
-- end-expected
SELECT column_name FROM information_schema.columns WHERE table_name = 'zz_vf_ident1';
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "müller" does not exist
-- end-expected-error
SELECT müller FROM zz_vf_ident1;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf_ident1;
-- begin-expected
-- columns: a:text
-- row: data
-- rowcount: 1
-- end-expected
SELECT U&'d!0061t!+000061' UESCAPE '!' AS a;
-- begin-expected
-- columns: ?column?:text
-- row: a\b
-- rowcount: 1
-- end-expected
SELECT U&'a\\b';
-- begin-expected
-- columns: a:text
-- row: foobar
-- rowcount: 1
-- end-expected
SELECT 'foo'
'bar' AS a;
-- begin-expected
-- columns: a:bpchar
-- row: abc
-- rowcount: 1
-- end-expected
SELECT N'abc' AS a;
