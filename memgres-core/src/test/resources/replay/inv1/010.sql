-- source: investigation.md
-- finding: 10
-- title: Array literal forms
-- begin-expected
-- columns: int4:_int4
-- row: [0:2]={10,20,30}
-- rowcount: 1
-- end-expected
SELECT '[0:2]={10,20,30}'::int[];
-- PG: works | mg: parsed as text, then
-- begin-expected
-- columns: array_length:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT array_length('[0:2]={10,20,30}'::int[], 1);
--   mg: 42804 function array_length(text, integer) does not exist
-- begin-expected
-- columns: int4:_int4
-- row: [-2:0]={10,20,30}
-- rowcount: 1
-- end-expected
SELECT '[-2:0]={10,20,30}'::int[];
-- mg: 22P02 invalid input syntax for type integer
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{{1,2},{3}}"
-- end-expected-error
SELECT '{{1,2},{3}}'::int[];
-- PG: 22P02 malformed array literal | mg: accepted
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "{{1,2},3}"
-- end-expected-error
SELECT '{{1,2},3}'::int[];
-- PG: 22P02                         | mg: accepted
-- begin-expected-error
-- sqlstate: 42804
-- message-like: ARRAY types integer[] and integer cannot be matched
-- end-expected-error
SELECT ARRAY[ARRAY[1,2], 3];
-- PG: 42804 types cannot be matched | mg: accepted
-- begin-expected-error
-- sqlstate: 2202E
-- message-like: cannot accumulate arrays of different dimensionality
-- end-expected-error
SELECT array_agg(a) FROM (VALUES ('{1,2}'::int[]),('{3}'::int[])) v(a);
--   PG: 2202E cannot accumulate arrays of different dimensionality | mg: accepted;
