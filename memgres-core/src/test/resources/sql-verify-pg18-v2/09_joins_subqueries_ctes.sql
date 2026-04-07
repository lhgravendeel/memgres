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

CREATE TABLE j1(id int PRIMARY KEY, v text);
CREATE TABLE j2(id int PRIMARY KEY, j1_id int, qty int);
INSERT INTO j1 VALUES (1,'a'),(2,'b'),(3,'c');
INSERT INTO j2 VALUES (10,1,5),(11,1,6),(12,2,7);

SELECT j1.id, j2.qty
FROM j1 JOIN j2 ON j1.id = j2.j1_id
ORDER BY 1,2;

SELECT *
FROM j1 LEFT JOIN j2 ON j1.id = j2.j1_id
ORDER BY j1.id, j2.id;

SELECT *
FROM j1 CROSS JOIN j2
ORDER BY j1.id, j2.id;

SELECT *
FROM j1 JOIN j2 USING (id);

SELECT *
FROM j1, LATERAL (SELECT qty FROM j2 WHERE j2.j1_id = j1.id ORDER BY qty LIMIT 1) q
ORDER BY j1.id;

SELECT id
FROM j1
WHERE EXISTS (SELECT 1 FROM j2 WHERE j2.j1_id = j1.id)
ORDER BY id;

SELECT id
FROM j1
WHERE id IN (SELECT j1_id FROM j2)
ORDER BY id;

SELECT id
FROM j1
WHERE id = ANY (SELECT j1_id FROM j2)
ORDER BY id;

WITH c AS (
  SELECT id, v FROM j1 WHERE id < 3
)
SELECT * FROM c ORDER BY id;

WITH RECURSIVE r(n) AS (
  VALUES (1)
  UNION ALL
  SELECT n + 1 FROM r WHERE n < 5
)
SELECT * FROM r ORDER BY n;

-- bad joins/subqueries/CTEs
SELECT * FROM j1 JOIN j2;
SELECT * FROM j1 JOIN j2 ON j1.nope = j2.id;
SELECT * FROM j1 JOIN j2 USING (nope);
SELECT * FROM j1 WHERE id = (SELECT j1_id FROM j2);
SELECT * FROM j1 WHERE id IN ();
WITH c AS () SELECT 1;
WITH RECURSIVE r(n) AS (SELECT 'x' UNION ALL SELECT n + 1 FROM r) SELECT * FROM r;
WITH c(x,y) AS (SELECT 1) SELECT * FROM c;
SELECT * FROM j1 WHERE EXISTS (SELECT);
SELECT * FROM j1 WHERE id = ALL (SELECT v FROM j1);
SELECT * FROM j1, LATERAL (SELECT nope FROM j2) q;

DROP SCHEMA compat CASCADE;



-- deeper scoping / lateral / recursive validation
SELECT j1.id,
       (SELECT max(qty) FROM j2 WHERE j2.j1_id = j1.id) AS max_qty
FROM j1
ORDER BY j1.id;

SELECT *
FROM j1
LEFT JOIN LATERAL (
  SELECT qty FROM j2 WHERE j2.j1_id = j1.id ORDER BY qty DESC LIMIT 1
) AS q ON true
ORDER BY j1.id;

WITH RECURSIVE tree(n, path) AS (
  VALUES (1, ARRAY[1])
  UNION ALL
  SELECT n + 1, path || (n + 1)
  FROM tree
  WHERE n < 4
)
SELECT * FROM tree ORDER BY n;

-- more bad cases
SELECT id FROM j1 JOIN j2 ON id = j1_id;
SELECT * FROM j1 WHERE EXISTS (SELECT 1 FROM j2 WHERE qty > j1.nope);
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
SELECT * FROM j1 LEFT JOIN LATERAL (SELECT qty WHERE j2.j1_id = j1.id) q ON true;

