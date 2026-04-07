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

CREATE TABLE t(
  id int,
  a text COLLATE "C",
  b text COLLATE "POSIX",
  x int,
  y int
);

INSERT INTO t VALUES
(1, 'abc', 'ABC', 1, NULL),
(2, 'äbc', 'abc', NULL, 2),
(3, NULL, NULL, NULL, NULL);

-- collation derivation/conflict explorations
SELECT a COLLATE "C" FROM t ORDER BY a COLLATE "C";
SELECT b COLLATE "POSIX" FROM t ORDER BY b COLLATE "POSIX";
SELECT (a COLLATE "C") || 'x' FROM t ORDER BY id;
SELECT upper(a COLLATE "C") FROM t ORDER BY id;
SELECT a COLLATE "C" < 'z' COLLATE "C" FROM t ORDER BY id;

-- null semantics depth
SELECT NULL = NULL, NULL IS NULL, NULL IS DISTINCT FROM NULL;
SELECT ROW(NULL, NULL) IS NULL, ROW(1, NULL) IS NULL, ROW(NULL, NULL) IS NOT NULL;
SELECT 1 NOT IN (1, 2, NULL), 3 NOT IN (1, 2, NULL);
SELECT 1 = ANY(ARRAY[1,NULL,3]), 2 = ALL(ARRAY[2,NULL]);
SELECT coalesce(NULL, NULL, 5), nullif(1,1), nullif(1,2);
SELECT count(*), count(x), count(y), sum(x), avg(y) FROM t;
SELECT DISTINCT x FROM t ORDER BY x NULLS LAST;
SELECT x FROM t
UNION
SELECT y FROM t
ORDER BY 1 NULLS LAST;

-- polymorphic / variadic / built-in resolution
SELECT format('%s %s', 1, 'x');
SELECT array_append(ARRAY[1,2], 3), array_prepend(0, ARRAY[1,2]);
SELECT greatest(1,2,3), least('b','a','c');
SELECT concat_ws(',', 'a', NULL, 'b');
SELECT pg_typeof(array_append(ARRAY[1,2], 3));
SELECT pg_typeof(greatest(1,2.5,3));

-- bad collation / null / polymorphic cases
SELECT a || b FROM t;
SELECT a COLLATE no_such_coll FROM t;
SELECT greatest();
SELECT array_append(ARRAY[1,2], 'x');
SELECT format('%2$s %1$s %3$s', 'a', 'b');
SELECT concat_ws(NULL, 'a', 'b');

DROP SCHEMA compat CASCADE;
