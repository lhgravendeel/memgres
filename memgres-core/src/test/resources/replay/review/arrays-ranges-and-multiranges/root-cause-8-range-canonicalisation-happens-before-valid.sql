-- source: review-2026-08.md
-- finding: Root cause 8: range canonicalisation happens before validation, and overflows
-- area: Arrays, ranges and multiranges
-- title: Root cause 8: range canonicalisation happens before validation, and overflows
-- begin-expected
-- columns: int4range:int4range
-- row: empty
-- rowcount: 1
-- end-expected
SELECT '(5,5)'::int4range;
-- begin-expected-error
-- sqlstate: 22000
-- message-like: range lower bound must be less than or equal to range upper bound
-- end-expected-error
SELECT '[5,4]'::int4range;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT '[-2147483648,2147483647]'::int4range;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT '[1,9223372036854775807]'::int8range;
-- begin-expected
-- columns: ?column?:numrange
-- row: (1,10]
-- rowcount: 1
-- end-expected
SELECT '(1,5]'::numrange + '(5,10]'::numrange;
-- begin-expected
-- columns: ?column?:numrange
-- row: (3,5]
-- rowcount: 1
-- end-expected
SELECT '(1,5]'::numrange * '(3,10]'::numrange;
-- begin-expected
-- columns: range_merge:numrange
-- row: (1,10]
-- rowcount: 1
-- end-expected
SELECT range_merge('(1,5]'::numrange,'(7,10]'::numrange);
-- begin-expected
-- columns: nummultirange:nummultirange
-- row: {[1.5,4.5)}
-- rowcount: 1
-- end-expected
SELECT nummultirange(numrange(1.5,2.5), numrange(2.5,4.5));
