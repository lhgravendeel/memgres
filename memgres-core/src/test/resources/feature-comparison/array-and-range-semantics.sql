-- Array literal input and range/multirange operator semantics.
--
-- Covers: the [lb:ub]= dimension prefix an array may be written with (negative bounds, several
-- dimensions, the bare [n] form, whitespace) and the functions that report those bounds; the
-- malformed array literals PostgreSQL refuses, including the ones that used to load as a different
-- array from the text; rectangularity of nested braces; array_fill's dimension and lower-bound
-- validation; array_agg over arrays; the *, + and - operators on ranges and multiranges, with the
-- contiguity errors and the empty range; which element type a range's containment operators accept;
-- and the multirange literal, whose members may be spelled "empty" and separated by whitespace.

-- An array written with its own lower bounds keeps them
SELECT '[0:2]={10,20,30}'::int[];
SELECT '[-2:0]={10,20,30}'::int[];
SELECT '[3]={1,2,3}'::int[];
SELECT '[1:2][1:2]={{1,2},{3,4}}'::int[];
SELECT '[2][2]={{1,2},{3,4}}'::int[];
SELECT ' [0:2] = {10,20,30} '::int[];
SELECT '[0:2]=  {10,20,30}'::int[];
SELECT array_dims('[0:2]={10,20,30}'::int[]);
SELECT array_ndims('[0:2]={10,20,30}'::int[]);
SELECT array_lower('[0:2]={10,20,30}'::int[], 1);
SELECT array_upper('[0:2]={10,20,30}'::int[], 1);
SELECT array_length('[0:2]={10,20,30}'::int[], 1);
SELECT cardinality('[0:2]={10,20,30}'::int[]);
SELECT ('[0:2]={10,20,30}'::int[])[0];
SELECT '[0:2]={10,20,30}'::int[] = '{10,20,30}'::int[];
SELECT '[0:2]={10,20,30}'::int[] = '[0:2]={10,20,30}'::int[];

-- The stated bounds have to describe the braces that follow them
SELECT '[0:2]={10,20}'::int[];
SELECT '[2:0]={}'::int[];
SELECT '[1:0]={}'::int[];
SELECT '[0:-1]={}'::int[];
SELECT '[0:2]{10,20,30}'::int[];
SELECT '[0:2]'::int[];
SELECT '[]={1}'::int[];

-- Malformed array literals
SELECT '{"a"b}'::text[];
SELECT '{a"b"}'::text[];
SELECT '{"a""b"}'::text[];
SELECT '{{1,{2}},{2,3}}'::text[];
SELECT '{}}'::text[];
SELECT '{ }}'::text[];
SELECT '}{'::text[];
SELECT '{foo{}}'::text[];
SELECT '{foo,,bar}'::text[];
SELECT '{1,}'::text[];
SELECT '{{1,}}'::text[];
SELECT '{{"1 2" x},{3}}'::text[];
SELECT '{,}'::text[];
SELECT '{1,2,3,}'::int[];
SELECT '{"a" "b"}'::text[];
SELECT '{a,{1}}'::text[];
SELECT '{1,2'::int[];
SELECT '1,2}'::int[];
SELECT '3'::int[];

-- Nested braces must describe a rectangle
SELECT '{{1,2},{3}}'::int[];
SELECT '{{1,2},3}'::int[];
SELECT '{{1},{2,3}}'::int[];
SELECT '{{},{1}}'::int[];
SELECT '{{},{}}'::int[];
SELECT '{{}}'::int[];
SELECT array_length('{{},{}}'::int[],1);

-- Spellings that stay legal
SELECT '{}'::text[];
SELECT '{ }'::text[];
SELECT '  {1,2,3}  '::int[];
SELECT '{ 1 , 2 , 3 }'::int[];
SELECT '{  1  ,  2  }'::int[];
SELECT '{"a b",c}'::text[];
SELECT '{a b,c}'::text[];
SELECT '{"a,b"}'::text[];
SELECT '{"a\"b"}'::text[];
SELECT '{"a\\b"}'::text[];
SELECT '{a\\b}'::text[];
SELECT '{\\NULL}'::text[];
SELECT '{NULL,1}'::int[];
SELECT '{"NULL"}'::text[];
SELECT '{null}'::text[];
SELECT '{Null}'::text[];
SELECT '{" NULL "}'::text[];
SELECT '{ NULL }'::text[];
SELECT '{""}'::text[];
SELECT '{"{"}'::text[];
SELECT '{"}"}'::text[];
SELECT '{","}'::text[];
SELECT '{{1,2},{3,4}}'::int[];
SELECT '{{{1},{2}},{{3},{4}}}'::int[];
SELECT '{"a" ,"b"}'::text[];
SELECT '{01,02}'::text[];

-- An ARRAY constructor resolves one type from its elements
SELECT ARRAY[ARRAY[1,2], 3];
SELECT ARRAY[3, ARRAY[1,2]];
SELECT ARRAY[ARRAY[1,2], NULL];
SELECT ARRAY[ARRAY[1,2], ARRAY[3,4]];
SELECT ARRAY[ARRAY[1,2], ARRAY[3]];
SELECT ARRAY[1,2] || 3;
SELECT 3 || ARRAY[1,2];
SELECT ARRAY[1,2] || ARRAY[3,4];
SELECT ARRAY[1,2] || '{3,4}';
SELECT ARRAY[1,2,3] || ARRAY[[4,5,6],[7,8,9]];

-- array_agg over arrays builds one array of the next dimension up
SELECT array_agg(a) FROM (VALUES ('{1,2}'::int[]),('{3,4}'::int[])) v(a);
SELECT array_agg(a) FROM (VALUES ('{a,b}'::text[]),('{c,d}'::text[])) v(a);
SELECT array_agg(a) FROM (VALUES ('{1,2}'::int[]),('{3}'::int[])) v(a);
SELECT array_agg(a) FROM (VALUES (NULL::int[]),('{3,4}'::int[])) v(a);
SELECT array_agg(x) FROM (VALUES (1),(2)) v(x);
SELECT array_agg(x) FROM (VALUES ('a'),('b c')) v(x);
SELECT array_agg(x) FROM (VALUES (1),(NULL::int)) v(x);

-- array_fill validates its dimensions and lower bounds
SELECT array_fill(1, ARRAY[2,2], NULL::int[]);
SELECT array_fill(1, NULL::int[]);
SELECT array_fill(1, ARRAY[2,2], '{}'::int[]);
SELECT array_fill(1, ARRAY[3,3], ARRAY[1,1,1]);
SELECT array_fill(1, ARRAY[2], ARRAY[2,3]);
SELECT array_fill(1, ARRAY[-1]);
SELECT array_fill(1, ARRAY[]::int[]);
SELECT array_fill(1, ARRAY[2], ARRAY[NULL]::int[]);
SELECT array_fill(1, ARRAY[1,1,1,1,1,1,1]);
SELECT array_fill('x', ARRAY[2]);
SELECT array_fill(NULL, ARRAY[2]);
SELECT array_fill(1, ARRAY[2,2]);
SELECT array_fill(1, ARRAY[2,2], ARRAY[1,1]);
SELECT array_fill(1, ARRAY[3], ARRAY[2]);
SELECT array_dims(array_fill(1, ARRAY[3], ARRAY[2]));
SELECT array_dims(array_fill(1, ARRAY[2,2], ARRAY[0,0]));
SELECT array_ndims(array_fill(1, ARRAY[2,2], ARRAY[0,0]));
SELECT array_lower(array_fill(1, ARRAY[2,2], ARRAY[0,0]), 1);
SELECT array_upper(array_fill(1, ARRAY[2,2], ARRAY[0,0]), 2);
SELECT array_fill(1, ARRAY[0]);
SELECT array_fill(NULL::int, ARRAY[2]);
SELECT array_fill(1, ARRAY[2,2,2]);
SELECT array_fill(1, '{2,2}');
SELECT array_fill(1, '{2,2}', '{0,0}');

-- Range meet, join and difference
SELECT '[1.5,2.5)'::numrange * '[2.0,3.0)'::numrange;
SELECT '(1.5,2.5]'::numrange + '(2.0,3.5]'::numrange;
SELECT '[1.5,2.0)'::numrange + '[2.0,3.5)'::numrange;
SELECT '[1.5,2.0)'::numrange + '[2.4,3.5)'::numrange;
SELECT '[1.5,2.0)'::numrange - '[1.8,1.9)'::numrange;
SELECT '[1,10)'::int4range * '[5,20)'::int4range;
SELECT '[1,10)'::int4range + '[5,20)'::int4range;
SELECT '[1,10)'::int4range - '[5,20)'::int4range;
SELECT '[1,10)'::int4range - '[1,5)'::int4range;
SELECT '[1,10)'::int4range - '[15,20)'::int4range;
SELECT '[1,10)'::int4range * '[20,30)'::int4range;
SELECT '[1,2)'::int4range + '[2,4)'::int4range;
SELECT '[1,2)'::int4range + '[3,4)'::int4range;
SELECT '[1,10)'::int4range + '[1,10)'::numrange;
SELECT '[1,10)'::int4range + '[2,3)';
SELECT '[2,3)' + '[1,10)'::int4range;
SELECT 'empty'::numrange * '[1,2)'::numrange;
SELECT 'empty'::numrange + '[1,2)'::numrange;
SELECT 'empty'::numrange - '[1,2)'::numrange;
SELECT '[1,2)'::numrange - 'empty'::numrange;
SELECT '[1,10)'::int4range * 'empty'::int4range;
SELECT '[1,10)'::int4range + 'empty'::int4range;
SELECT 'empty'::int4range + 'empty'::int4range;
SELECT 'empty'::int4range - 'empty'::int4range;
SELECT '{[1,3)}'::int4multirange + '{[5,7)}'::int4multirange;
SELECT '{[1,5)}'::int4multirange * '{[3,7)}'::int4multirange;
SELECT '{[1,5)}'::int4multirange - '{[3,7)}'::int4multirange;

-- Containment takes exactly the type the range is built over
SELECT '[1,5]'::numrange @> 5;
SELECT '[1,5]'::numrange @> 5::numeric;
SELECT '[1,5]'::numrange @> 5.0;
SELECT '[1,5]'::numrange @> 5::float8;
SELECT '[1,5]'::int4range @> 5;
SELECT '[1,5]'::int4range @> 5.0;
SELECT '[1,5]'::int4range @> 5::bigint;
SELECT '[1,5]'::int8range @> 5;
SELECT '[1,5]'::int8range @> 5::bigint;
SELECT '[1,5]'::numrange <@ 5;
SELECT 5 <@ '[1,5]'::numrange;
SELECT 5 <@ '[1,5]'::int4range;
SELECT '[1,5]'::int4range @> '[2,3]'::int4range;
SELECT '{[1,5)}'::int4multirange @> 3;
SELECT '{[1,5)}'::int4multirange @> 3.0;
SELECT '{[1,5)}'::nummultirange @> 3;
SELECT '[2020-01-01,2020-02-01]'::daterange @> '2020-01-15'::date;
SELECT '[2020-01-01,2020-02-01]'::daterange @> 5;
SELECT '[1,5]'::numrange @> NULL::int;
SELECT '[1,5]'::int4range @> NULL;

-- Multirange literals
SELECT '{[1,3),empty,[5,7)}'::int4multirange;
SELECT '{[1,3),EMPTY}'::int4multirange;
SELECT '{empty}'::int4multirange;
SELECT '{empty,empty}'::int4multirange;
SELECT '{[1,3), [5,7)}'::int4multirange;
SELECT '{ [1,3) , [5,7) }'::int4multirange;
SELECT '{ [1,3),[5,7) }'::int4multirange;
SELECT '{[1.5,3.5), [4,5)}'::nummultirange;
SELECT '{}'::int4multirange;
SELECT '{ }'::int4multirange;
SELECT '{,}'::int4multirange;
SELECT '{[1,3),[5,7),}'::int4multirange;
SELECT '{[1,3),,[5,7)}'::int4multirange;
SELECT '{"[1,3)","[5,7)"}'::int4multirange;
SELECT '{[1,3)'::int4multirange;
SELECT '{}}'::int4multirange;
SELECT ''::int4multirange;
SELECT '  '::int4multirange;
SELECT '{[5,7),[1,3)}'::int4multirange;
SELECT '{[1,3),[3,7)}'::int4multirange;
SELECT '{[1,3),[2,7)}'::int4multirange;
SELECT range_merge('{[1,3),[5,7)}'::int4multirange);
SELECT int4multirange(int4range(1,3), int4range(5,7));

-- Array operators that keep working
SELECT ARRAY[1,2] @> ARRAY[2];
SELECT ARRAY[1,2] <@ ARRAY[1,2,3];
SELECT ARRAY[1,2] && ARRAY[2,3];
SELECT ARRAY[1,2] @> '{}'::int[];
SELECT '{}'::int[] @> '{}'::int[];
SELECT ARRAY[1,2] || NULL::int[];
