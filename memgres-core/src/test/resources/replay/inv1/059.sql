-- source: investigation.md
-- finding: 59
-- title: `array_fill` dimension validation
-- begin-expected-error
-- sqlstate: 22004
-- message-like: dimension array or low bound array cannot be null
-- end-expected-error
SELECT array_fill(1, ARRAY[2,2], NULL::int[]);
--   PG: 22004 dimension array or low bound array cannot be null | mg: {{1,1},{1,1}}
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: wrong number of array subscripts
-- end-expected-error
SELECT array_fill(1, ARRAY[2,2], '{}'::int[]);
--   PG: 2202E wrong number of array subscripts | mg: accepted
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: wrong number of array subscripts
-- end-expected-error
SELECT array_fill(1, ARRAY[3,3], ARRAY[1,1,1]);
--   PG: 2202E wrong number of array subscripts | mg: accepted
-- begin-expected-error
-- sqlstate: 54000
-- message-like: array size exceeds the maximum allowed (134217727)
-- end-expected-error
SELECT array_fill(1, ARRAY[-1]);
--   PG: array size exceeds the maximum allowed | mg: {}
-- begin-expected
-- columns: array_fill:_int4
-- row: {}
-- rowcount: 1
-- end-expected
SELECT array_fill(1, ARRAY[]::int[]);
-- PG: {} | mg: 1  (not even an array);
