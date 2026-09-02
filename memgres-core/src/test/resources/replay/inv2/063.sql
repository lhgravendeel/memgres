-- source: investigation-2026-08.md
-- finding: 63
-- title: The encoding-name argument of the conversion functions is validated against a name list and then discarded (or passed straight to Charset.forName, or swallowed 
-- begin-expected
-- columns: convert_from:text
-- row: é
-- rowcount: 1
-- end-expected
SELECT convert_from('\xe9'::bytea, 'LATIN1');
-- begin-expected
-- columns: convert_to:bytea
-- row: \x616263
-- rowcount: 1
-- end-expected
SELECT convert_to('abc','SQL_ASCII');
-- begin-expected
-- columns: convert:bytea
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT convert('abc'::bytea, NULL, 'UTF8');
-- begin-expected
-- columns: to_ascii:text
-- row: KarACl
-- rowcount: 1
-- end-expected
SELECT to_ascii('Karél', 'LATIN1');
-- begin-expected
-- columns: length:int4
-- row: 4
-- rowcount: 1
-- end-expected
SELECT length('jose', 'UTF8');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid source encoding name "BOGUSENC"
-- end-expected-error
SELECT convert('abc'::bytea, 'BOGUSENC', 'UTF8');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid destination encoding name "BOGUSENC"
-- end-expected-error
SELECT convert('abc'::bytea, 'UTF8', 'BOGUSENC');
