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

SELECT version();
SELECT 1 AS i, 1.5 AS n, 'x'::text AS t, TRUE AS b, NULL AS z;
SELECT pg_typeof(1), pg_typeof(1.5), pg_typeof('x'::text), pg_typeof(TRUE), pg_typeof(NULL);

CREATE TABLE t(a int);
CREATE TABLE t(a int);
INSERT INTO no_such_table VALUES (1);
SELECT * FROM;

DROP SCHEMA compat CASCADE;

