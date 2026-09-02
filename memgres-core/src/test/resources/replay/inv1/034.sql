-- source: investigation.md
-- finding: 34
-- title: `FOR UPDATE` legality — the full picture
-- unrunnable: the report wrote this reproducer abbreviated
SELECT count(*) FROM t FOR UPDATE;
-- with aggregates
SELECT DISTINCT v FROM t ORDER BY v FOR UPDATE;
-- with DISTINCT
SELECT v FROM t GROUP BY v FOR UPDATE;
-- with GROUP BY
SELECT id, row_number() OVER (ORDER BY id) FROM t FOR UPDATE;
-- with window functions
SELECT id FROM t UNION SELECT id FROM t FOR UPDATE;
-- with set operations
SELECT a.id FROM t a LEFT JOIN t b ON … FOR UPDATE OF b;
--   PG: cannot be applied to the nullable side of an outer join
SELECT a.id FROM t a FOR UPDATE OF t;
--   PG: 42P01 relation "t" in FOR UPDATE clause not found in FROM clause (the alias hides it);;
