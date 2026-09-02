-- source: review-2026-08.md
-- finding: Root cause 7: encode/decode delegate to Java codecs instead of implementing PostgreSQL's
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 7: encode/decode delegate to Java codecs instead of implementing PostgreSQL's
-- begin-expected
-- columns: encode:text
-- row: c3a9
-- rowcount: 1
-- end-expected
SELECT encode(decode('é', 'escape'), 'hex');
-- begin-expected
-- columns: length:int4
-- row: 5
-- rowcount: 1
-- end-expected
SELECT length(encode('\x0102030405'::bytea, 'escape'));
-- begin-expected
-- columns: encode:text
-- row: 	
-- rowcount: 1
-- end-expected
SELECT encode('\x09'::bytea,'escape');
-- begin-expected
-- columns: length:int4
-- row: 77
-- rowcount: 1
-- end-expected
SELECT length(encode(decode(repeat('61', 57), 'hex'), 'base64'));
-- begin-expected
-- columns: encode:text
-- row: 61626364
-- rowcount: 1
-- end-expected
SELECT encode(decode(E'YWJj\nZA==', 'base64'), 'hex');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid base64 end sequence
-- end-expected-error
SELECT encode(decode('YWJjZA','base64'),'hex');
-- begin-expected
-- columns: encode:text
-- row: 1234
-- rowcount: 1
-- end-expected
SELECT encode(decode('12 34', 'hex'), 'hex');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid hexadecimal digit: "x"
-- end-expected-error
SELECT decode('xy','hex');
