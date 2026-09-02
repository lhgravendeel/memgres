-- source: investigation.md
-- finding: 36
-- title: CTE name resolution allows forward references
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "y" does not exist
-- end-expected-error
WITH x AS (SELECT n FROM y), y AS (SELECT 2 AS n) SELECT * FROM x;
--   PG: 42P01 relation "y" does not exist — a plain WITH sees only earlier CTEs
--   mg: returns 2;
