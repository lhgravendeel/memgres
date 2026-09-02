-- source: review-2026-08.md
-- finding: Root cause 18: TextSearchFunctions dispatches by name without checking arity or argument shape
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 18: TextSearchFunctions dispatches by name without checking arity or argument shape
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ts_rw(t tsquery, s tsquery);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_ts_rw VALUES ('a'::tsquery, 'x | y'::tsquery);
-- begin-expected
-- columns: ts_rewrite:text
-- row: 'b' & ( 'y' | 'x' )
-- rowcount: 1
-- end-expected
SELECT ts_rewrite('a & b'::tsquery, 'SELECT t, s FROM zz_vf_ts_rw')::text;
-- begin-expected-error
-- sqlstate: XX000
-- message-like: unrecognized weight: 0
-- end-expected-error
SELECT setweight('a:1'::tsvector, '');
-- begin-expected-error
-- sqlstate: XX000
-- message-like: unrecognized weight: 88
-- end-expected-error
SELECT setweight('a:1'::tsvector, 'X');
-- begin-expected
-- columns: setweight:tsvector
-- row: 'a':1A
-- rowcount: 1
-- end-expected
SELECT setweight('a:1'::tsvector, 'a');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized weight: "X"
-- end-expected-error
SELECT ts_filter('a:1A'::tsvector, '{X}');
-- begin-expected-error
-- sqlstate: 2200F
-- message-like: lexeme array may not contain empty strings
-- end-expected-error
SELECT array_to_tsvector(ARRAY['']::text[]);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: ts_stat query must return one tsvector column
-- end-expected-error
SELECT count(*) FROM ts_stat('SELECT 1');
