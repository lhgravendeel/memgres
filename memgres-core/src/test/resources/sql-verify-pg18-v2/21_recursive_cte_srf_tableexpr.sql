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

CREATE TABLE edges(src int, dst int);
INSERT INTO edges VALUES
(1,2),(1,3),(2,4),(3,4),(4,5),(5,1);

-- recursive basics
WITH RECURSIVE walk(n, depth, path) AS (
  VALUES (1, 0, ARRAY[1])
  UNION ALL
  SELECT e.dst, w.depth + 1, w.path || e.dst
  FROM walk w
  JOIN edges e ON e.src = w.n
  WHERE w.depth < 3
)
SELECT * FROM walk ORDER BY depth, n;

-- SEARCH / CYCLE
WITH RECURSIVE graph(src, dst) AS (
  SELECT src, dst FROM edges
),
search_graph(id, path) AS (
  SELECT 1, ARRAY[1]
  UNION ALL
  SELECT e.dst, sg.path || e.dst
  FROM search_graph sg
  JOIN graph e ON e.src = sg.id
  WHERE cardinality(sg.path) < 4
)
SEARCH DEPTH FIRST BY id SET ordcol
CYCLE id SET is_cycle USING cycle_path
SELECT id, path, ordcol, is_cycle, cycle_path
FROM search_graph
ORDER BY ordcol;

-- data-modifying CTEs
CREATE TABLE cte_t(id int PRIMARY KEY, v text);
WITH ins AS (
  INSERT INTO cte_t VALUES (1,'a'),(2,'b')
  RETURNING id, v
)
SELECT * FROM ins ORDER BY id;

WITH upd AS (
  UPDATE cte_t
  SET v = upper(v)
  WHERE id = 1
  RETURNING *
),
del AS (
  DELETE FROM cte_t
  WHERE id = 2
  RETURNING *
)
SELECT (SELECT count(*) FROM upd) AS upd_count,
       (SELECT count(*) FROM del) AS del_count;

SELECT * FROM cte_t ORDER BY id;

-- SRFs and advanced table expressions
SELECT * FROM generate_series(1,5) AS g(n);
SELECT * FROM generate_series(1,5,2) AS g(n);
SELECT * FROM unnest(ARRAY[10,20,30]) WITH ORDINALITY AS u(val, ord);
SELECT * FROM ROWS FROM (generate_series(1,3), generate_series(10,12)) AS x(a,b);
SELECT * FROM unnest(ARRAY[1,2], ARRAY['a','b']) AS u(i,t);
SELECT *
FROM cte_t
CROSS JOIN LATERAL generate_series(1, length(v)) AS g(n)
ORDER BY id, n;

SELECT *
FROM (VALUES (1,'x'), (2,'y')) AS v(id, txt)
JOIN LATERAL (SELECT txt || txt AS doubled) q ON true
ORDER BY id;

-- TABLESAMPLE and ONLY
CREATE TABLE sample_t(id int, note text);
INSERT INTO sample_t
SELECT g, 'n' || g::text FROM generate_series(1,20) AS g;
SELECT count(*) FROM sample_t TABLESAMPLE SYSTEM (50);
SELECT count(*) FROM sample_t TABLESAMPLE BERNOULLI (50) REPEATABLE (123);
SELECT * FROM ONLY sample_t ORDER BY id LIMIT 3;

-- bad recursive / SRF / table-expression cases
WITH RECURSIVE bad(n) AS (
  SELECT 1
  UNION ALL
  SELECT n, n+1 FROM bad
) SELECT * FROM bad;

WITH RECURSIVE bad2(n) AS (
  SELECT 1
  UNION ALL
  SELECT 'x' FROM bad2
) SELECT * FROM bad2;

WITH RECURSIVE bad3(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM bad3 WHERE n < 2
)
SEARCH BREADTH FIRST SET ord
SELECT * FROM bad3;

SELECT * FROM generate_series('x', 'y');
SELECT * FROM unnest();
SELECT * FROM ROWS FROM ();
SELECT * FROM sample_t TABLESAMPLE SYSTEM (-1);
SELECT * FROM sample_t TABLESAMPLE no_such_method (10);
SELECT * FROM ONLY no_such_table;
WITH x AS (INSERT INTO cte_t VALUES (1,'dup') RETURNING *) SELECT * FROM x;

DROP SCHEMA compat CASCADE;
