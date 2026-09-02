-- source: review-2026-08.md
-- finding: Root cause 1: tsvector and tsquery text I/O is not a faithful reader/writer
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 1: tsvector and tsquery text I/O is not a faithful reader/writer
-- begin-expected
-- columns: text:text
-- row: 'a':1,2
-- rowcount: 1
-- end-expected
SELECT 'a:1 a:2'::tsvector::text;
-- begin-expected
-- columns: text:text
-- row: 'it''s'
-- rowcount: 1
-- end-expected
SELECT '''it''''s'''::tsvector::text;
-- begin-expected
-- columns: setweight:text
-- row: 'a' 'b'
-- rowcount: 1
-- end-expected
SELECT setweight('a b'::tsvector, 'A')::text;
-- begin-expected
-- columns: text:text
-- row: 'a':16383
-- rowcount: 1
-- end-expected
SELECT ('a:16383'::tsvector || 'a:1'::tsvector)::text;
-- begin-expected
-- columns: text:text
-- row: 'a':1,2,3
-- rowcount: 1
-- end-expected
SELECT 'a:3,1,2'::tsvector::text;
-- begin-expected
-- columns: text:text
-- row: 'a':1
-- rowcount: 1
-- end-expected
SELECT 'a:1,1'::tsvector::text;
-- begin-expected
-- columns: text:text
-- row: 'a b'
-- rowcount: 1
-- end-expected
SELECT 'a\ b'::tsvector::text;
-- begin-expected
-- columns: text:text
-- row: 'ab'
-- rowcount: 1
-- end-expected
SELECT E'a\\b'::tsvector::text;
-- begin-expected
-- columns: text:text
-- row: ':1'
-- rowcount: 1
-- end-expected
SELECT ':1'::tsvector::text;
-- begin-expected
-- columns: text:text
-- row: 'a:b'
-- rowcount: 1
-- end-expected
SELECT '''a:b'''::tsquery::text;
-- begin-expected
-- columns: text:text
-- row: 'it''s'
-- rowcount: 1
-- end-expected
SELECT '''it''''s'''::tsquery::text;
-- begin-expected
-- columns: text:text
-- row: 'a':*AB
-- rowcount: 1
-- end-expected
SELECT 'a:*AB'::tsquery::text;
-- begin-expected
-- columns: text:text
-- row: 'a':*A
-- rowcount: 1
-- end-expected
SELECT 'a:A*'::tsquery::text;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsvector: "a:"
-- end-expected-error
SELECT 'a:'::tsvector;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsvector: "a:1,"
-- end-expected-error
SELECT 'a:1,'::tsvector;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsvector: "a:,1"
-- end-expected-error
SELECT 'a:,1'::tsvector;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsvector: "'unterminated"
-- end-expected-error
SELECT '''unterminated'::tsvector;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsvector: "''  "
-- end-expected-error
SELECT '''''  '::tsvector;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "a:1"
-- end-expected-error
SELECT 'a:1'::tsquery;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "a:Z"
-- end-expected-error
SELECT 'a:Z'::tsquery;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "cat <-1> dog"
-- end-expected-error
SELECT 'cat <-1> dog'::tsquery;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: distance in phrase operator must be an integer value between zero and 16384 inclusive
-- end-expected-error
SELECT tsquery_phrase('a'::tsquery,'b'::tsquery,-1)::text;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "& cat"
-- end-expected-error
SELECT '& cat'::tsquery;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "cat & & dog"
-- end-expected-error
SELECT 'cat & & dog'::tsquery;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "()"
-- end-expected-error
SELECT '()'::tsquery;
