-- source: review-2026-08.md
-- finding: Root cause 7: every subscript is lowered to the jsonb `->` operator
-- area: Arrays, ranges and multiranges
-- title: Root cause 7: every subscript is lowered to the jsonb `->` operator
-- begin-expected
-- columns: array:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT (ARRAY[1,2,3])[1.5];
-- begin-expected-error
-- sqlstate: 42804
-- message-like: cannot subscript type text because it does not support subscripting
-- end-expected-error
SELECT ('abcdef'::text)[2];
-- begin-expected-error
-- sqlstate: 42804
-- message-like: array subscript must have type integer
-- end-expected-error
SELECT (ARRAY[1,2,3])['x'::text];
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT (ARRAY[1,2,3])[4294967297];
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: jsonb -> bigint
-- end-expected-error
SELECT '[1,2]'::jsonb -> 1::bigint;
-- begin-expected
-- columns: array:int4
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT (ARRAY[[1,2],[3,4]])[1];
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT (ARRAY[1,2,3,4,5])[NULL:2] IS NULL;
-- begin-expected
-- columns: point:float8 | pg_typeof:regtype
-- row: 1 | double precision
-- rowcount: 1
-- end-expected
SELECT ('(1,2)'::point)[0], pg_typeof(('(1,2)'::point)[0]);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: cannot subscript type json because it does not support subscripting
-- end-expected-error
SELECT ('{"a": 1}'::json)['a'];
