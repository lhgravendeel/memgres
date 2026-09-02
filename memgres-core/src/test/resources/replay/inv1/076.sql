-- source: investigation.md
-- finding: 76
-- title: `numeric` typmod bounds and `abs`/`gcd` at the limit
-- begin-expected-error
-- sqlstate: 22023
-- message-like: NUMERIC precision 1001 must be between 1 and 1000
-- end-expected-error
SELECT 1::numeric(1001,0);
-- PG: 22023 precision must be between 1 and 1000 | mg: OK
-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
SELECT 1::numeric(5,10);
-- PG: 22003 numeric field overflow | mg: 1.0000000000
-- begin-expected-error
-- sqlstate: 22003
-- message-like: smallint out of range
-- end-expected-error
SELECT abs('-32768'::int2);
-- PG: 22003 smallint out of range | mg: 32768 (outside int2)
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT gcd('-9223372036854775808'::int8, 0::int8);
-- PG: 22003 | mg: -9223372036854775808;
