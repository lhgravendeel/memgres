-- source: investigation.md
-- finding: 58
-- title: Integer negation and `abs` do not overflow-check ⚠️
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT abs('-9223372036854775808'::int8);
-- PG: 22003 bigint out of range | mg: -9223372036854775808
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT -('-9223372036854775808'::int8);
-- PG: 22003                     | mg: -9223372036854775808
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT abs('-2147483648'::int4);
-- PG: 22003 integer out of range| mg: -2147483648;
