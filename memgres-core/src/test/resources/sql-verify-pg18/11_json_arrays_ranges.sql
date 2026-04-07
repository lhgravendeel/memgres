\echo '=== 11_json_arrays_ranges.sql ==='
\set VERBOSITY verbose
\set SHOW_CONTEXT never
\set ON_ERROR_STOP off
SET search_path = pg_catalog, public;
DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;

SELECT json '{"a":1,"b":[10,20]}' AS j, pg_typeof(json '{"a":1,"b":[10,20]}') AS t;
SELECT jsonb '{"a":1,"b":[10,20]}' AS jb, pg_typeof(jsonb '{"a":1,"b":[10,20]}') AS t;
SELECT ('{"a":1,"b":[10,20]}'::jsonb -> 'b') AS b,
       ('{"a":1,"b":[10,20]}'::jsonb ->> 'a') AS a_text;
SELECT jsonb_build_object('x', 1, 'y', true) AS obj;
SELECT jsonb_build_array(1, 'x', true, null) AS arr;
SELECT '{1,2,3}'::int[] AS arr, pg_typeof('{1,2,3}'::int[]) AS t;
SELECT ARRAY[1,2,3][2] AS arr_elem;
SELECT '[1,10)'::int4range AS r, pg_typeof('[1,10)'::int4range) AS t;
SELECT '{[1,5),[10,20)}'::int4multirange AS mr, pg_typeof('{[1,5),[10,20)}'::int4multirange) AS t;
SELECT int4range(1, 10) @> 5 AS contains1;
SELECT int4range(1, 10) && int4range(5, 15) AS overlap1;
SELECT ARRAY[1,2,3] || ARRAY[4,5] AS arr_concat;
SELECT array_length(ARRAY[1,2,3], 1) AS len1;
SELECT array_dims(ARRAY[[1,2],[3,4]]) AS dims;

-- JSON / array / range errors
SELECT 'not json'::json;
SELECT 'not json'::jsonb;
SELECT '[1,2,]'::json;
SELECT '{"a":1}'::jsonb -> 999999999999999999999;
SELECT jsonb_path_query('{"a":1}'::jsonb, 'not a path');
SELECT '{1,2,x}'::int[];
SELECT ARRAY[[1,2],[3]];
SELECT ARRAY[1,2]['x'];
SELECT array_length(ARRAY[1,2,3], 0);
SELECT '[1,2)'::int4range + 1;
SELECT '[5,1)'::int4range;
SELECT '{[1,5),bad}'::int4multirange;
SELECT int4range(1, 10) @> 'x';

DROP SCHEMA compat CASCADE;
