-- source: review-2026-08.md
-- finding: Root cause 16: the lexer folds with Java case rules and is missing three SQL literal forms
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 16: the lexer folds with Java case rules and is missing three SQL literal forms
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
-- columns: ?column?:text
-- row: data
-- rowcount: 1
-- end-expected
SELECT U&'d!0061t!+000061' UESCAPE '!';
-- begin-expected
-- columns: ?column?:text
-- row: a\b
-- rowcount: 1
-- end-expected
SELECT U&'a\\b';
-- begin-expected
-- columns: ?column?:text
-- row: foobar
-- rowcount: 1
-- end-expected
SELECT 'foo'
'bar';
-- begin-expected
-- columns: bpchar:bpchar
-- row: abc
-- rowcount: 1
-- end-expected
SELECT N'abc';
