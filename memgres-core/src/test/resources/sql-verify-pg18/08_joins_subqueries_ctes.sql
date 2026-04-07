\echo '=== 08_joins_subqueries_ctes.sql ==='
\set VERBOSITY verbose
\set SHOW_CONTEXT never
\set ON_ERROR_STOP off
SET search_path = pg_catalog, public;
DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;

CREATE TABLE j1(id int, grp int, val text);
CREATE TABLE j2(id int, grp int, score int);
INSERT INTO j1 VALUES (1, 10, 'a'), (2, 10, 'b'), (3, 20, 'c'), (4, NULL, 'd');
INSERT INTO j2 VALUES (1, 10, 100), (2, 30, 200), (NULL, 20, 300), (4, NULL, 400);

SELECT j1.id, j1.grp, j2.score
FROM j1 INNER JOIN j2 ON j1.id = j2.id
ORDER BY 1;

SELECT j1.id, j1.grp, j2.score
FROM j1 LEFT JOIN j2 ON j1.id = j2.id
ORDER BY 1;

SELECT j1.id AS j1_id, j2.id AS j2_id
FROM j1 FULL JOIN j2 ON j1.grp = j2.grp
ORDER BY 1 NULLS LAST, 2 NULLS LAST;

SELECT *
FROM j1 JOIN j2 USING (id)
ORDER BY id NULLS LAST;

SELECT id, val
FROM j1
WHERE EXISTS (SELECT 1 FROM j2 WHERE j2.id = j1.id)
ORDER BY id;

SELECT id, val
FROM j1
WHERE grp IN (SELECT grp FROM j2)
ORDER BY id;

SELECT id, val
FROM j1
WHERE grp = ANY (SELECT grp FROM j2 WHERE grp IS NOT NULL)
ORDER BY id;

SELECT id, val
FROM j1
WHERE grp < ALL (SELECT grp FROM j2 WHERE grp IS NOT NULL)
ORDER BY id;

WITH cte AS (
    SELECT id, grp FROM j1 WHERE grp IS NOT NULL
)
SELECT * FROM cte ORDER BY id;

WITH RECURSIVE r(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM r WHERE n < 5
)
SELECT n, pg_typeof(n) FROM r ORDER BY n;

-- join/subquery/cte errors
SELECT id FROM j1 JOIN j2 ON id = id;
SELECT * FROM j1 JOIN j2 USING (no_such_col);
SELECT * FROM j1 NATURAL JOIN j2 ORDER BY id;
SELECT * FROM j1 WHERE grp = (SELECT grp FROM j2);
SELECT * FROM j1 WHERE grp < ALL (SELECT val FROM j1);
SELECT * FROM j1 WHERE (id, grp) IN (SELECT id FROM j2);
WITH cte AS (SELECT 1 AS x) SELECT y FROM cte;
WITH RECURSIVE r(n) AS (
    SELECT 'x'
    UNION ALL
    SELECT n + 1 FROM r WHERE n < 5
)
SELECT * FROM r;
WITH RECURSIVE r(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM missing_cte WHERE n < 5
)
SELECT * FROM r;
WITH cte(a, a) AS (SELECT 1, 2) SELECT * FROM cte;
LATERAL SELECT 1;

DROP SCHEMA compat CASCADE;
