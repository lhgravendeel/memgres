-- source: investigation-2026-08.md
-- finding: 97
-- title: 'infinity' is carried as a magic string that most datetime functions never test for, so it falls through into ordinary epoch arithmetic
-- begin-expected
-- columns: date_bin:text
-- row: infinity
-- rowcount: 1
-- end-expected
SELECT date_bin(interval '1 hour', timestamp 'infinity', timestamp '2001-01-01')::text;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: origin out of range
-- end-expected-error
SELECT date_bin(interval '1 hour', timestamp '2020-01-01', timestamp 'infinity')::text;
-- begin-expected
-- columns: to_timestamp:text
-- row: infinity
-- rowcount: 1
-- end-expected
SELECT to_timestamp('inf'::float8)::text;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range: "1e+308"
-- end-expected-error
SELECT to_timestamp(1e308)::text;
-- begin-expected
-- columns: age:text
-- row: infinity
-- rowcount: 1
-- end-expected
SELECT age(timestamp 'infinity', timestamp '2020-01-01')::text;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT age(timestamp 'infinity', timestamp 'infinity')::text;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: cannot subtract infinite dates
-- end-expected-error
SELECT date 'infinity' - date '-infinity';
-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamps cannot be binned into infinite intervals
-- end-expected-error
SELECT date_bin(interval 'infinity', timestamp '2020-01-01', timestamp '2001-01-01')::text;
