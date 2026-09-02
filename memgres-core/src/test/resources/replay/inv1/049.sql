-- source: investigation.md
-- finding: 49
-- title: Arrays, ranges and aggregates (5 cases)
-- begin-expected
-- columns: ?column?:_int4
-- row: {1,2,3,4}
-- rowcount: 1
-- end-expected
SELECT ARRAY[1,2] || '{3,4}';
-- PG: {1,2,3,4} | mg: 42883 operator does not exist: integer[] || point
-- begin-expected
-- columns: ?column?:_int4
-- row: {{1,2,3},{4,5,6},{7,8,9}}
-- rowcount: 1
-- end-expected
SELECT ARRAY[1,2,3] || ARRAY[[4,5,6],[7,8,9]];
-- PG: {{1,2,3},{4,5,6},{7,8,9}} | mg: 42883
-- begin-expected-error
-- sqlstate: 22004
-- message-like: dimension array or low bound array cannot be null
-- end-expected-error
SELECT array_fill(1, NULL::int[]);
-- PG: 22004 dimension array cannot be null | mg: NULL
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT ARRAY[NULL]::int[] @> ARRAY[NULL]::int[];
-- PG: 22004 array must not contain nulls | mg: false
-- begin-expected
-- columns: upper_inf:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT upper_inf('[2020-01-01,infinity)'::tsrange);
--   PG: false | mg: 22P02 invalid input syntax for type integer: "infinity"
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT rank(1,2) WITHIN GROUP (ORDER BY v) FROM t;
--   PG: 42883 wrong number of arguments | mg: returns 1;
