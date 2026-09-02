-- source: investigation.md
-- finding: 7
-- title: Range and multirange operator gaps
-- begin-expected
-- columns: ?column?:numrange
-- row: [2.0,2.5)
-- rowcount: 1
-- end-expected
SELECT '[1.5,2.5)'::numrange * '[2.0,3.0)'::numrange;
--   PG: [2.0,2.5)  | mg: 42883 operator does not exist: inet * inet   ← misparsed as inet
-- begin-expected
-- columns: ?column?:numrange
-- row: (1.5,3.5]
-- rowcount: 1
-- end-expected
SELECT '(1.5,2.5]'::numrange + '(2.0,3.5]'::numrange;
--   PG: (1.5,3.5]  | mg: 22P02 invalid input syntax for type point    ← misparsed as point
-- begin-expected-error
-- sqlstate: 22000
-- message-like: result of range union would not be contiguous
-- end-expected-error
SELECT '[1.5,2.0)'::numrange + '[2.4,3.5)'::numrange;
--   PG: 22000 result of range union would not be contiguous | mg: [1.5,3.5) (silently wrong)
-- begin-expected-error
-- sqlstate: 22000
-- message-like: result of range difference would not be contiguous
-- end-expected-error
SELECT '[1.5,2.0)'::numrange - '[1.8,1.9)'::numrange;
--   PG: 22000 would not be contiguous | mg: returns the original range
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: numrange @> integer
-- end-expected-error
SELECT '[1,5]'::numrange @> 5;
-- PG: 42883 numrange @> integer | mg: true
-- begin-expected
-- columns: int4multirange:int4multirange
-- row: {[1,3),[5,7)}
-- rowcount: 1
-- end-expected
SELECT '{[1,3),empty,[5,7)}'::int4multirange;
-- PG: {[1,3),[5,7)} | mg: 22P02 malformed
-- begin-expected
-- columns: int4multirange:int4multirange
-- row: {[1,3),[5,7)}
-- rowcount: 1
-- end-expected
SELECT '{[1,3), [5,7)}'::int4multirange;
-- a space breaks it | mg: 22P02;
