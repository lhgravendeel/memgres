-- source: investigation-2026-08.md
-- finding: 15
-- title: Values carry no SQL type at runtime, so operators and functions are chosen by looking at the shape of the value's text. Text beginning '{' is an array, text mat
-- begin-expected
-- columns: ?column?:text
-- row: {a,b}c
-- rowcount: 1
-- end-expected
SELECT '{a,b}' || 'c';
-- begin-expected
-- columns: ?column?:text
-- row: {a,b}c
-- rowcount: 1
-- end-expected
SELECT u || 'c' FROM unnest(ARRAY['{a,b}']) AS u;
-- begin-expected
-- columns: ?column?:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT NULL || u FROM unnest(ARRAY['{a,b}']) AS u;
-- begin-expected
-- columns: ?column?:_int4
-- row: {1,2,NULL}
-- rowcount: 1
-- end-expected
SELECT ARRAY[1,2] || NULL::int;
-- begin-expected
-- columns: cardinality:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT cardinality(ARRAY[1,2] || NULL::int);
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'a' IN ('{a,b}');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "{1,2}"
-- end-expected-error
SELECT 1 IN ('{1,2}');
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'a' = ANY (ARRAY['{a,b}']);
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '[1,5]'::jsonb @> '[2,3]'::jsonb;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '[2,3]'::jsonb <@ '[1,5]'::jsonb;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '[1,10]'::jsonb @> '[5]'::jsonb;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '[1,5)'::int4range &< '[1,3)'::int4range;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'empty'::int4range &< '[1,2)'::int4range;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '{[1,5)}'::int4multirange &< '{[3,10)}'::int4multirange;
-- begin-expected
-- columns: array:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT (ARRAY[1,2,3])[2];
-- begin-expected
-- columns: text:text
-- row: b
-- rowcount: 1
-- end-expected
SELECT ('{a,b,c}'::text[])[2];
-- begin-expected
-- columns: int4:_int4
-- row: {1,2}
-- rowcount: 1
-- end-expected
SELECT '{1,2}'::text::int[];
-- begin-expected
-- columns: varchar:varchar
-- row: [1,5)
-- rowcount: 1
-- end-expected
SELECT ('[1,5)'::int4range)::varchar;
