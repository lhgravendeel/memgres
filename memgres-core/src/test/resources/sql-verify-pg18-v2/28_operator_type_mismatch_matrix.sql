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

-- operator mismatch matrix
SELECT 1 + true;
SELECT 1 + point(1,2);
SELECT 1 + 'x';
SELECT 'a' - 'b';
SELECT true || false;
SELECT ARRAY[1,2] + ARRAY[3,4];
SELECT ARRAY[1,2] || point(1,2);
SELECT ARRAY[1,2] @> ARRAY['x'];
SELECT ARRAY[1,2] && ARRAY['x'];
SELECT int4range(1,5) / int4range(2,3);
SELECT int4range(1,5) * numrange(2,3);
SELECT '{"a":1}'::jsonb * 2;
SELECT '{"a":1}'::jsonb || 1;
SELECT '{"a":1}'::json || '{"b":2}'::json;
SELECT point(1,1) || point(2,2);
SELECT point(1,2) @> 1;
SELECT point(0,0) <-> '(1,1)';
SELECT true AND 1;
SELECT B'1' = 1;
SELECT B'1010' + B'0101';
SELECT 1 OPERATOR(pg_catalog.=) 1;
SELECT OPERATOR(pg_catalog.||)('a','b');
SELECT + + 1;
SELECT @@@ 1;

-- a few valid comparisons for contrast
SELECT 1 + 2, 1 = 1, 'a' || 'b';
SELECT ARRAY[1,2] || ARRAY[3,4];
SELECT '{"a":1}'::jsonb || '{"b":2}'::jsonb;
SELECT point(0,0) <-> point(3,4);

DROP SCHEMA compat CASCADE;
