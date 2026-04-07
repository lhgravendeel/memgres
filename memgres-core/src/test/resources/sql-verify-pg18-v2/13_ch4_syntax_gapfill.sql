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

CREATE TYPE pair AS (x int, y text);
CREATE TABLE gap(
  id int,
  arr int[],
  comp pair,
  txt text COLLATE "C"
);
INSERT INTO gap VALUES
(1, ARRAY[10,20,30], ROW(7,'seven'), 'abc'),
(2, ARRAY[40,50], ROW(8,'eight'), 'ABC');

-- Chapter 4 syntax gap-fill

-- positional parameters via PREPARE/EXECUTE
PREPARE p1(int, text) AS
  SELECT $1 AS a, $2 AS b, pg_typeof($1), pg_typeof($2);
EXECUTE p1(1, 'x');

PREPARE p2(int) AS
  SELECT * FROM gap WHERE id = $1;
EXECUTE p2(1);

PREPARE p3(int, int) AS
  SELECT $1 + $2;
EXECUTE p3(10, 20);

-- field selection
SELECT comp.x, comp.y FROM gap ORDER BY id;
SELECT (ROW(9,'nine')::pair).x, (ROW(9,'nine')::pair).y;
SELECT ((SELECT comp FROM gap WHERE id=1)).x;

-- collation expressions
SELECT txt COLLATE "C" FROM gap ORDER BY txt COLLATE "C";
SELECT 'ä' COLLATE "C" < 'z' COLLATE "C";
SELECT upper(txt COLLATE "C") FROM gap ORDER BY id;

-- function call notation
CREATE FUNCTION fmix(a int, b int DEFAULT 2, c text DEFAULT 'z')
RETURNS text
LANGUAGE SQL
AS $$ SELECT a::text || ':' || b::text || ':' || c $$;

SELECT fmix(1,2,'x');
SELECT fmix(a => 1, b => 2, c => 'x');
SELECT fmix(1, c => 'x');
SELECT fmix(c => 'x', a => 1);

-- operator invocation and parser edge cases
SELECT OPERATOR(pg_catalog.+)(1,2);
SELECT 1 OPERATOR(pg_catalog.+) 2;
SELECT @ -5;
SELECT NOT TRUE AND FALSE, NOT (TRUE AND FALSE);

-- scalar subquery, array constructor, row constructor
SELECT (SELECT id FROM gap WHERE id=1);
SELECT ARRAY(SELECT id FROM gap ORDER BY id);
SELECT ROW(id, txt) FROM gap ORDER BY id;

-- bad positional parameter / Chapter 4 specific cases
PREPARE badp AS SELECT $1;
EXECUTE p1(1);
EXECUTE p2('x');
SELECT ((SELECT comp FROM gap)).x;
SELECT txt COLLATE no_such_collation FROM gap;
SELECT fmix(b => 2);
SELECT fmix(1, a => 2);
SELECT fmix(a => 1, a => 2);
SELECT OPERATOR(pg_catalog.+)(1);
SELECT OPERATOR(pg_catalog.+)(1,2,3);
SELECT (ROW(1,'a')::pair).z;
SELECT ARRAY(SELECT txt FROM gap ORDER BY id) + 1;
DEALLOCATE p1;
DEALLOCATE p2;
DEALLOCATE p3;
EXECUTE p3(1,2);

DROP SCHEMA compat CASCADE;

