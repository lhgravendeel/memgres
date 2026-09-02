-- source: investigation.md
-- finding: 75
-- title: `real` arithmetic and aggregates do not check overflow (5 cases)
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT 3.4e38::real * 2::real;
-- PG: 22003 value out of range: overflow | mg: 6.79e+38
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT 3.0e38::real + 3.0e38::real;
-- PG: 22003 | mg: 6.0e+38
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT 3.4e38::real / 0.5::real;
-- PG: 22003 | mg: 6.79e+38
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT 1.0e38::real * 1.0e38::real;
-- PG: 22003 | mg: 9.99e+75
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT sum(r) FROM t;
-- r is real, sum overflows; PG: 22003 | mg: 6.0e+38;
