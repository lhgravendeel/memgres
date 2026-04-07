\echo '=== 00_harness.sql ==='
\pset pager off
\pset format aligned
\pset tuples_only off
\set VERBOSITY verbose
\set SHOW_CONTEXT never
\set ON_ERROR_STOP off

SET client_min_messages = notice;
SET search_path = pg_catalog, public;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';
SET standard_conforming_strings = on;

SELECT current_setting('server_version') AS server_version;
SELECT current_setting('server_encoding') AS server_encoding;
SELECT current_setting('client_encoding') AS client_encoding;
SELECT current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle,
       current_setting('TimeZone') AS timezone;

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;

SELECT 1 AS i, pg_typeof(1) AS i_type,
       1.5 AS n, pg_typeof(1.5) AS n_type,
       'x'::text AS t, pg_typeof('x'::text) AS t_type,
       true AS b, pg_typeof(true) AS b_type,
       NULL::int AS null_i, pg_typeof(NULL::int) AS null_i_type;

CREATE TABLE harness_table(a int, b text);
INSERT INTO harness_table VALUES (1, 'one'), (2, 'two');
TABLE harness_table;

-- duplicate object error
CREATE TABLE harness_table(a int);

-- semantic error
SELECT missing_column FROM harness_table;

-- runtime error
SELECT 1 / 0;

-- parser error: missing expression
SELECT FROM harness_table;

-- parser error: dangling comma
SELECT 1, FROM harness_table;

-- parser error: mismatched parentheses
SELECT (1 + 2 FROM harness_table;

DROP SCHEMA compat CASCADE;
