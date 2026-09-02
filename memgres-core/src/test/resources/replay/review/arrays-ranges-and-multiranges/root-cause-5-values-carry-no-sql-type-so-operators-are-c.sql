-- source: review-2026-08.md
-- finding: Root cause 5: values carry no SQL type, so operators are chosen by sniffing the text
-- area: Arrays, ranges and multiranges
-- title: Root cause 5: values carry no SQL type, so operators are chosen by sniffing the text
-- begin-expected
-- columns: ?column?:text
-- row: {a,b}c
-- rowcount: 1
-- end-expected
SELECT '{a,b}' || 'c';
-- begin-expected
-- columns: ?column?:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT NULL || u FROM unnest(ARRAY['{a,b}']) AS u;
-- begin-expected
-- columns: ?column?:_int4 | cardinality:int4
-- row: {1,2,NULL} | 3
-- rowcount: 1
-- end-expected
SELECT ARRAY[1,2] || NULL::int, cardinality(ARRAY[1,2] || NULL::int);
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
SELECT '[1,5]'::jsonb @> '[2,3]'::jsonb;
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
-- row: t
-- rowcount: 1
-- end-expected
SELECT '{[1,5)}'::int4multirange &< '{[3,10)}'::int4multirange;
-- begin-expected
-- columns: ?column?:text
-- row: (1,2)x
-- rowcount: 1
-- end-expected
SELECT ROW(1,2) || 'x';
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function array_length(text, integer) does not exist
-- end-expected-error
SELECT array_length('{1,2,3}'::text, 1), cardinality('{1,2,3}'::text);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function unnest(text) does not exist
-- end-expected-error
SELECT unnest('{1,2,3}'::text);
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type int4range to numrange
-- end-expected-error
SELECT '[1,5)'::int4range::numrange;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: int4range && numrange
-- end-expected-error
SELECT '[1,5)'::int4range && '[2,3)'::numrange;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: int4range + int4multirange
-- end-expected-error
SELECT '[1,5)'::int4range + '{[6,8)}'::int4multirange;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_r5" does not exist
-- end-expected-error
SELECT min(r) FROM zz_vf_r5;
-- r int4range
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = integer[]
-- end-expected-error
SELECT 1 = ANY (SELECT ARRAY[1,2]);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer[] = integer
-- end-expected-error
SELECT ARRAY[1] = ANY (ARRAY[ARRAY[1],ARRAY[2]]);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type integer[]
-- end-expected-error
SELECT ('{1,2}'::int[] COLLATE "C");
-- begin-expected
-- columns: array:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT (ARRAY[1,2,3])[2];
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
