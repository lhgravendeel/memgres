-- source: investigation-2026-08.md
-- finding: 14
-- title: An array is carried as its PostgreSQL literal text, and every site that has to produce or read one rolls its own parser/renderer. There are at least four render
-- begin-expected
-- columns: array_append:_int4
-- row: [0:2]={1,2,3}
-- rowcount: 1
-- end-expected
SELECT array_append('[0:1]={1,2}'::int[], 3);
-- begin-expected
-- columns: array_remove:_int4
-- row: [0:1]={1,3}
-- rowcount: 1
-- end-expected
SELECT array_remove('[0:2]={1,2,3}'::int[], 2);
-- begin-expected
-- columns: array_to_string:text
-- row: 1,2,3
-- rowcount: 1
-- end-expected
SELECT array_to_string('[0:2]={1,2,3}'::int[], ',');
-- begin-expected
-- columns: array_position:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT array_position('[0:2]={1,2,3}'::int[], 3);
-- begin-expected
-- columns: ?column?:_int4
-- row: [0:2]={1,2,3}
-- rowcount: 1
-- end-expected
SELECT '[0:1]={1,2}'::int[] || ARRAY[3];
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 1 = ANY('[0:1]={1,2}'::int[]);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '[0:1]={1,2}'::int[] @> '{1}'::int[];
-- begin-expected
-- columns: u:int4
-- row: 1
-- row: 2
-- row: 3
-- rowcount: 3
-- end-expected
SELECT * FROM unnest('[0:2]={1,2,3}'::int[]) AS u;
-- begin-expected
-- columns: array:text
-- row: {"NULL"}
-- rowcount: 1
-- end-expected
SELECT ARRAY['NULL']::text;
-- begin-expected
-- columns: timestamp:_timestamp
-- row: {"2020-01-01 10:00:00"}
-- rowcount: 1
-- end-expected
SELECT '{"2020-01-01 10:00:00"}'::timestamp[];
-- begin-expected
-- columns: array_agg:_text
-- row: {"a	b",c}
-- rowcount: 1
-- end-expected
SELECT array_agg(x) FROM (VALUES (E'a\tb'), ('c')) v(x);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_e2 AS ENUM ('a b', 'c''d', 'E', 'e');
-- begin-expected
-- columns: enum_range:_zz_vf_e2
-- row: {"a b",c'd,E,e}
-- rowcount: 1
-- end-expected
SELECT enum_range(NULL::zz_vf_e2);
-- begin-expected
-- columns: regexp_match:text
-- row: 01
-- rowcount: 1
-- end-expected
SELECT (regexp_match('x 01 y', '(\d\d)'))[1];
-- begin-expected
-- columns: array_to_string:text
-- row: 007
-- rowcount: 1
-- end-expected
SELECT array_to_string(regexp_match('x 007 y','(\d+)'),'|');
-- begin-expected
-- columns: regexp_split_to_array:text
-- row: 01
-- rowcount: 1
-- end-expected
SELECT (regexp_split_to_array('01,02',','))[1];
