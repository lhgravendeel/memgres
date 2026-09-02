-- source: investigation-2026-08.md
-- finding: 72
-- title: TextSearchFunctions dispatches by name without checking arity or argument shape: the two-argument ts_rewrite overload is routed to the three-argument code, setw
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
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf_ts_rw;
-- begin-expected-error
-- sqlstate: XX000
-- message-like: unrecognized weight: 0
-- end-expected-error
SELECT setweight('a:1'::tsvector, '')::text;
-- begin-expected-error
-- sqlstate: XX000
-- message-like: unrecognized weight: 88
-- end-expected-error
SELECT setweight('a:1'::tsvector, 'X')::text;
-- begin-expected
-- columns: setweight:text
-- row: 'a':1A
-- rowcount: 1
-- end-expected
SELECT setweight('a:1'::tsvector, 'a')::text;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized weight: "X"
-- end-expected-error
SELECT ts_filter('a:1A'::tsvector, '{X}')::text;
-- begin-expected-error
-- sqlstate: 2200F
-- message-like: lexeme array may not contain empty strings
-- end-expected-error
SELECT array_to_tsvector(ARRAY['']::text[])::text;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: ts_stat query must return one tsvector column
-- end-expected-error
SELECT count(*) FROM ts_stat('SELECT 1');
