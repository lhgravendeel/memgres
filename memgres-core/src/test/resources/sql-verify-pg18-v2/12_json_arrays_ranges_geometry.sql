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

SELECT '{"a":1,"b":[10,20,null]}'::json AS j,
       '{"a":1,"b":[10,20,null]}'::jsonb AS jb;

SELECT ('{"a":1,"b":[10,20,null]}'::jsonb)->'b',
       ('{"a":1,"b":[10,20,null]}'::jsonb)->'b'->>1,
       pg_typeof(('{"a":1}'::jsonb)->'a');

SELECT jsonb_build_object('x', 1, 'y', ARRAY[1,2,3]);
SELECT jsonb_path_query('{"a":[1,2,3]}'::jsonb, '$.a[*]');
SELECT ARRAY[1,2,3] || ARRAY[4,5];
SELECT array_length(ARRAY[[1,2],[3,4]], 2);
SELECT int4range(1,5) @> 3, int4range(1,5) && int4range(4,9);
SELECT int4multirange(int4range(1,5), int4range(10,15));

-- built-in geometry
SELECT point(1,1) <@ box(point(0,0), point(2,2));
SELECT box(point(0,0), point(2,2)) @> point(1,1);
SELECT point(0,0) <-> point(3,4);
SELECT path '[(0,0),(1,1),(2,2)]' && box(point(0,0), point(1,1));
SELECT circle(point(0,0), 2) @> point(1,1);

-- bad rich-type cases
SELECT 'not-json'::jsonb;
SELECT ('{"a":1}'::jsonb)->'b'->>0;
SELECT jsonb_path_query('{"a":[1,2,3]}'::jsonb, 'not a path');
SELECT ARRAY[1,2] || 'x';
SELECT array_length(ARRAY[1,2,3], 0);
SELECT int4range(1,5) + int4range(2,3);
SELECT int4multirange('[1,2)');
SELECT point(1,2) + 'abc';
SELECT box(1,2);
SELECT point(1,1) <@ circle(point(0,0), 0.5);

DROP SCHEMA compat CASCADE;



-- more json/array/range/geometry coverage
SELECT jsonb_build_array(1, 'x', true, NULL);
SELECT '{"a":{"b":[1,2,3]}}'::jsonb #> '{a,b,1}';
SELECT '{"a":{"b":[1,2,3]}}'::jsonb #>> '{a,b,1}';
SELECT '{"a":1,"b":2}'::jsonb ? 'a';
SELECT '{"a":1,"b":2}'::jsonb ?| array['x','b'];
SELECT '[1,2,3]'::jsonb @> '[2]'::jsonb;

SELECT array_dims(ARRAY[[1,2],[3,4]]);
SELECT array_lower('[0:2]={1,2,3}'::int[], 1), array_upper('[0:2]={1,2,3}'::int[], 1);
SELECT '[0:2]={1,2,3}'::int[];
SELECT ARRAY[[1,2],[3,4]][1:2][1:1];

SELECT int4range(NULL, 5);
SELECT isempty(int4range(1,1));
SELECT lower_inc(int4range(1,5)), upper_inc(int4range(1,5));
SELECT range_merge(int4range(1,5), int4range(5,8));

SELECT line '{1, -1, 0}';
SELECT line '[(0,0),(1,1)]';
SELECT open '( (0,0), (1,1), (2,2) )'::path;
SELECT pclose(path '((0,0),(1,1),(2,2))');
SELECT area(box(point(0,0), point(2,3)));
SELECT center(circle(point(0,0), 5));
SELECT point(0,0) ~= point(0,0);
SELECT polygon '((0,0),(1,0),(1,1),(0,1))' @> point(0.5,0.5);

-- more bad rich-type cases
SELECT '{"a":1}'::jsonb #> 'not_an_array';
SELECT '{"a":1}'::jsonb ?| 'x';
SELECT array_lower(ARRAY[1,2,3], 0);
SELECT '[2:1]={}'::int[];
SELECT range_merge(int4range(1,5), numrange(1,5));
SELECT line 'bad';
SELECT area(point(1,1));
SELECT center(box(point(0,0), point(1,1), point(2,2)));

