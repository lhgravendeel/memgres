-- source: review-2026-08.md
-- finding: Root cause 11: 'infinity' is a string most datetime functions never test for
-- area: Dates, times, intervals — and the DML/MERGE/COPY findings filed with them
-- title: Root cause 11: 'infinity' is a string most datetime functions never test for
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
