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
CREATE TABLE exprs(
  id int PRIMARY KEY,
  a int,
  b int[],
  c pair,
  d text COLLATE "C"
);
INSERT INTO exprs VALUES
(1, 10, ARRAY[1,2,3], ROW(7,'seven'), 'abc'),
(2, NULL, ARRAY[4,5], ROW(8,'eight'), 'ABC');

-- column references
SELECT id, a, b, c, d FROM exprs ORDER BY id;
SELECT exprs.a, exprs.b[1], exprs.b[2:3] FROM exprs ORDER BY id;

-- field selection
SELECT c.x, c.y FROM exprs ORDER BY id;
SELECT (ROW(11,'eleven')::pair).x, (ROW(11,'eleven')::pair).y;
SELECT ((SELECT c FROM exprs WHERE id=1)).x;

-- array constructors / subscripts
SELECT ARRAY[1,2,3] AS arr, pg_typeof(ARRAY[1,2,3]);
SELECT ARRAY[[1,2],[3,4]] AS m, pg_typeof(ARRAY[[1,2],[3,4]]);
SELECT ARRAY(SELECT id FROM exprs ORDER BY id);
SELECT b[1], b[2], b[10] FROM exprs ORDER BY id;
SELECT b[0] FROM exprs WHERE id=1;
SELECT b[2:1] FROM exprs WHERE id=1;

-- row constructors and comparisons
SELECT ROW(1,'a'), pg_typeof(ROW(1,'a'));
SELECT ROW(1,2) = ROW(1,2), ROW(1,2) < ROW(1,3);
SELECT ROW(a, d) FROM exprs ORDER BY id;

-- scalar subqueries
SELECT (SELECT max(id) FROM exprs) AS max_id;
SELECT (SELECT a FROM exprs WHERE id=1) AS scalar_ok;
SELECT (SELECT id FROM exprs) AS scalar_bad;

-- casts and collation expressions
SELECT CAST('123' AS int), '456'::int;
SELECT d COLLATE "C" FROM exprs ORDER BY d COLLATE "C";
SELECT 'ä' COLLATE "C" < 'z' COLLATE "C";
SELECT 'a' COLLATE "C" || 'b';

-- operator invocation
SELECT 1 + 2, - 5, @ -5, 5 !;
SELECT OPERATOR(pg_catalog.+)(1,2);
SELECT 3 OPERATOR(pg_catalog.*) 4;

-- bad expression cases
SELECT c.z FROM exprs;
SELECT b['x'] FROM exprs;
SELECT exprs.nope FROM exprs;
SELECT (ROW(1,'a')::pair).z;
SELECT ARRAY[1,'x'];
SELECT ARRAY[1,2][1][2];
SELECT ROW(1) = ROW(1,2);
SELECT (SELECT id FROM exprs WHERE id IN (1,2));
SELECT 'abc'::int;
SELECT d COLLATE no_such_collation FROM exprs;
SELECT OPERATOR(no_such_schema.+)(1,2);
SELECT 5 ! !;
SELECT (SELECT ROW(1,2)).x;

DROP SCHEMA compat CASCADE;



-- deeper type resolution / unknown-literal behavior
SELECT pg_typeof(NULL);
SELECT pg_typeof(COALESCE(NULL, 1));
SELECT pg_typeof(COALESCE(NULL, 1.5));
SELECT pg_typeof(COALESCE(NULL, 'x'));
SELECT pg_typeof(CASE WHEN true THEN 1 ELSE 2 END);
SELECT pg_typeof(CASE WHEN true THEN 1 ELSE 2.5 END);
SELECT pg_typeof(CASE WHEN true THEN NULL ELSE 2 END);
SELECT pg_typeof(ARRAY[NULL, 1, 2]);
SELECT pg_typeof(ARRAY[1::smallint, 2::bigint]);
SELECT pg_typeof(VALUES (1), (2));
SELECT pg_typeof((SELECT x FROM (VALUES (1), (2)) AS v(x) LIMIT 1));
SELECT pg_typeof(ROW(NULL, 1));

-- empty-array typing and coercion errors
SELECT ARRAY[]::int[];
SELECT ARRAY[];
SELECT ARRAY[NULL];
SELECT ARRAY[1, 2.5, 3];
SELECT ARRAY[1, '2', 3];
SELECT ARRAY[ROW(1,'a')::pair, ROW(2,'b')::pair];
SELECT pg_typeof(ARRAY[ROW(1,'a')::pair, ROW(2,'b')::pair]);

-- VALUES / UNION type resolution
SELECT * FROM (VALUES (1), (2.5)) AS v(x);
SELECT pg_typeof(x) FROM (VALUES (1), (2.5)) AS v(x) LIMIT 1;
SELECT * FROM (VALUES ('1'::text), (NULL)) AS v(x);
SELECT 1 UNION ALL SELECT 2.5;
SELECT pg_typeof(x) FROM (SELECT 1 UNION ALL SELECT 2.5) AS q(x) LIMIT 1;
SELECT 1 UNION ALL SELECT 'x';
SELECT CASE WHEN true THEN ROW(1,'a')::pair ELSE ROW(2,'b')::pair END;

-- expression-evaluation-rule related cases
SELECT CASE WHEN false THEN 1/0 ELSE 1 END;
SELECT CASE WHEN true THEN 1 ELSE 1/0 END;
SELECT COALESCE(1, 1/0);
SELECT NULLIF(1, 1/0);

-- more bad field selection / composite cases
SELECT (ROW(1,'a')).x;
SELECT (ROW(1,'a')).f1, (ROW(1,'a')).f2;
SELECT ((ROW(1,'a')::pair).x).nope;

