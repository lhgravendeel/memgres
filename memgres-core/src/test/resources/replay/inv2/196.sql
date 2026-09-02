-- source: investigation-2026-08.md
-- finding: 196
-- title: FunctionEvaluator checks argument types but not argument ranges: a zero step, an out-of-range seed, a zero start offset, equal bucket bounds and an empty enum a
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_empty AS ENUM ();
-- begin-expected-error
-- sqlstate: 55000
-- message-like: enum zz_vf_empty contains no values
-- end-expected-error
SELECT enum_first(NULL::zz_vf_empty);
-- begin-expected-error
-- sqlstate: 55000
-- message-like: enum zz_vf_empty contains no values
-- end-expected-error
SELECT enum_last(NULL::zz_vf_empty);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: step size cannot equal zero
-- end-expected-error
SELECT generate_series(1, 10, 0);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: step size cannot equal zero
-- end-expected-error
SELECT generate_series(1::numeric, 5::numeric, 0::numeric);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: step size cannot equal zero
-- end-expected-error
SELECT generate_series('2000-01-01'::timestamp, '2000-01-05'::timestamp, '0 days'::interval);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: setseed parameter 2 is out of allowed range [-1,1]
-- end-expected-error
SELECT setseed(2);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "start": 0
-- end-expected-error
SELECT regexp_count('abc','a',0);
-- begin-expected-error
-- sqlstate: 2201G
-- message-like: lower bound cannot equal upper bound
-- end-expected-error
SELECT width_bucket(1,2,2,1);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function gen_random_bytes(integer) does not exist
-- end-expected-error
SELECT length(gen_random_bytes(1025));
