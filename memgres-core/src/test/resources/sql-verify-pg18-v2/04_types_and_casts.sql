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

CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE DOMAIN posint AS int CHECK (VALUE > 0);
CREATE TYPE pair AS (x int, y text);

SELECT true, false, pg_typeof(true);
SELECT 42::smallint, 42::int, 42::bigint;
SELECT 1.25::numeric(10,2), 1.25::real, 1.25::double precision;
SELECT 'abc'::text, 'abc'::varchar(5), 'abc'::char(5);
SELECT E'\\x41'::bytea;
SELECT '00000000-0000-0000-0000-000000000000'::uuid;
SELECT DATE '2024-02-29', TIME '12:34:56', TIMESTAMP '2024-02-29 12:34:56', TIMESTAMPTZ '2024-02-29 12:34:56+00';
SELECT INTERVAL '1 day 2 hours';
SELECT '{"a":1}'::json, '{"a":1}'::jsonb;
SELECT 'ok'::mood, 5::posint, ROW(1,'a')::pair;
SELECT ARRAY[1,2,3], int4range(1,5), int4multirange(int4range(1,5), int4range(7,9));

-- built-in geometric types
SELECT point(1,2), '(3,4)'::point;
SELECT lseg(point(0,0), point(1,1));
SELECT box(point(0,0), point(2,2));
SELECT path '[(0,0),(1,1),(2,2)]';
SELECT polygon '((0,0),(1,0),(1,1),(0,1))';
SELECT circle(point(0,0), 5);

-- comparisons / casts
SELECT 'sad'::mood < 'happy'::mood;
SELECT CAST(ROW(2,'b') AS pair).x;
SELECT 1/2, 1::numeric/2, 1::float8/2;

-- bad inputs
SELECT 'abc'::int;
SELECT 2147483648::int4;
SELECT 1e400::float8;
SELECT '2024-02-30'::date;
SELECT '25:00:00'::time;
SELECT 'not-json'::json;
SELECT 'nope'::uuid;
SELECT 0::posint;
SELECT 'nope'::mood;
SELECT ROW(1,2,3)::pair;
SELECT ARRAY[[1,2],[3]];
SELECT '[1,2)'::int4range;
SELECT int4multirange('[1,2)');
SELECT '(1)'::point;
SELECT circle(point(0,0), -1);
SELECT polygon '((0,0),(1,1))';

DROP SCHEMA compat CASCADE;

