\pset pager off
\pset format unaligned
\pset tuples_only off
\pset null <NULL>
\set VERBOSITY verbose
\set SHOW_CONTEXT always
\set ON_ERROR_STOP off

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;
SET client_min_messages = notice;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';

SELECT current_schema() AS current_schema,
       current_setting('TimeZone') AS timezone,
       current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle;

-- deeper arrays
SELECT '[0:2]={10,20,30}'::int[];
SELECT array_lower('[0:2]={10,20,30}'::int[], 1), array_upper('[0:2]={10,20,30}'::int[], 1);
SELECT ARRAY[1,2,3] @> ARRAY[2], ARRAY[1,2] && ARRAY[2,3];
SELECT unnest(ARRAY[1,2,3]);
SELECT generate_subscripts(ARRAY[[1,2],[3,4]], 1), generate_subscripts(ARRAY[[1,2],[3,4]], 2);
SELECT ARRAY[ROW(1,'a')::record, ROW(2,'b')::record];
SELECT ARRAY['sad'::text, 'ok'::text];
SELECT 2 = ANY(ARRAY[1,2,3]), 4 = ALL(ARRAY[4,4,4]);

-- deeper ranges and multiranges
SELECT int4range(1,5,'[]'), int4range(1,5,'[)');
SELECT isempty(int4range(1,1,'[)')), lower(int4range(1,5)), upper(int4range(1,5));
SELECT int4range(1,5) -|- int4range(5,8), int4range(1,5) * int4range(3,8);
SELECT int4range(1,5) + int4range(5,8);
SELECT int4range(1,10) - int4range(3,7);
SELECT daterange(DATE '2024-01-01', DATE '2024-02-01');
SELECT tstzrange(TIMESTAMPTZ '2024-01-01 00:00+00', TIMESTAMPTZ '2024-02-01 00:00+00');
SELECT multirange(int4range(1,5), int4range(10,15));
SELECT int4multirange(int4range(1,5), int4range(5,8));

-- deeper json/jsonb differences
SELECT '{"a":1,"a":2}'::json, '{"a":1,"a":2}'::jsonb;
SELECT '{"b":2,"a":1}'::jsonb = '{"a":1,"b":2}'::jsonb;
SELECT jsonb_set('{"a":[1,2,3]}'::jsonb, '{a,1}', '99'::jsonb);
SELECT jsonb_insert('{"a":[1,2,3]}'::jsonb, '{a,1}', '77'::jsonb);
SELECT '{"a":1,"b":2}'::jsonb - 'a';
SELECT '{"a":[1,2,3]}'::jsonb #- '{a,1}';
SELECT jsonb_path_exists('{"a":[1,2,3]}'::jsonb, '$.a[*] ? (@ > 2)');
SELECT jsonb_path_match('{"a":2}'::jsonb, 'exists($.a ? (@ == 2))');
SELECT jsonb_typeof('null'::jsonb), jsonb_typeof('{"a":1}'::jsonb->'a');
SELECT ('{"a":null}'::jsonb)->'a', ('{"a":null}'::jsonb)->>'a';

-- bad deeper cases
SELECT ARRAY[1,2,3] @> ARRAY['x'];
SELECT 2 = ANY(ARRAY['a','b']);
SELECT int4range(1,5,'bad');
SELECT int4range(1,5) * numrange(1,5);
SELECT jsonb_set('{"a":[1,2,3]}'::jsonb, 'not_a_path', '1'::jsonb);
SELECT jsonb_insert('{"a":[1,2,3]}'::jsonb, '{a,x}', '1'::jsonb);
SELECT jsonb_path_exists('{"a":[1,2,3]}'::jsonb, 'not a path');
SELECT jsonb_path_match('{"a":2}'::jsonb, '$.a');
SELECT '{"a":1}'::json - 'a';

DROP SCHEMA compat CASCADE;
