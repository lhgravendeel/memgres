-- source: review-2026-08.md
-- finding: Aggregate name lookup folds case with the default locale
-- area: Aggregates, window functions and grouping
-- title: Aggregate name lookup folds case with the default locale
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT MIN(n) FROM t;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT STRING_AGG(n::text, ',') FROM t;
