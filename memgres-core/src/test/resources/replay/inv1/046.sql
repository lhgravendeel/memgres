-- source: investigation.md
-- finding: 46
-- title: Mathematical domain errors are not raised (7 cases)
-- begin-expected-error
-- sqlstate: 2201F
-- message-like: zero raised to a negative power is undefined
-- end-expected-error
SELECT power(0::numeric, -1);
-- PG: 2201F zero raised to a negative power | mg: Infinity
-- begin-expected-error
-- sqlstate: 2201F
-- message-like: a negative number raised to a non-integer power yields a complex result
-- end-expected-error
SELECT power(-1::numeric, 0.5);
-- PG: 2201F negative to a non-integer power | mg: NaN
-- begin-expected-error
-- sqlstate: 2201F
-- message-like: zero raised to a negative power is undefined
-- end-expected-error
SELECT power(0::float8, -1);
-- PG: 2201F | mg: Infinity
-- begin-expected-error
-- sqlstate: 2201F
-- message-like: a negative number raised to a non-integer power yields a complex result
-- end-expected-error
SELECT power(-1::float8, 0.5);
-- PG: 2201F | mg: NaN
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT log(1.0, 10.0);
-- PG: 22012 division by zero | mg: Infinity;
