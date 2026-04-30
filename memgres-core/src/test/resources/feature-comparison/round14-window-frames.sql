-- ============================================================================
-- Feature Comparison: Round 14 — Window / aggregate edge cases
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r14_win CASCADE;
CREATE SCHEMA r14_win;
SET search_path = r14_win, public;

CREATE TABLE r14_w (id int, v int);
INSERT INTO r14_w VALUES (1, NULL), (2, 10), (3, NULL), (4, 20), (5, NULL), (6, 30);

-- ============================================================================
-- SECTION A: IGNORE NULLS / RESPECT NULLS
-- ============================================================================

-- 1. lag IGNORE NULLS skips intermediate NULLs
-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT id, lag(v, 1) IGNORE NULLS OVER (ORDER BY id)::text AS prev FROM r14_w ORDER BY id;

-- 2. lead IGNORE NULLS
-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT id, lead(v, 1) IGNORE NULLS OVER (ORDER BY id)::text AS nxt FROM r14_w ORDER BY id;

-- 3. first_value IGNORE NULLS
-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT first_value(v) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)::text AS v
FROM r14_w ORDER BY id LIMIT 1;

-- ============================================================================
-- SECTION B: nth_value FROM FIRST / LAST
-- ============================================================================

-- 4. FROM FIRST
-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT nth_value(v, 2) FROM FIRST OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)::text AS v
FROM r14_w ORDER BY id LIMIT 1;

-- ============================================================================
-- SECTION C: Named WINDOW inheritance
-- ============================================================================

-- 5. w2 AS (w1 ROWS ...)
SELECT id, sum(id) OVER w2 AS s FROM r14_w
WINDOW w1 AS (ORDER BY id),
       w2 AS (w1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
ORDER BY id;

-- ============================================================================
-- SECTION D: COUNT(DISTINCT composite)
-- ============================================================================

CREATE TABLE r14_cd (a int, b int);
INSERT INTO r14_cd VALUES (1,1),(1,1),(1,2),(2,1),(2,1);

-- 6. count distinct pair
-- begin-expected
-- columns: c
-- row: 3
-- end-expected
SELECT count(DISTINCT (a,b))::text AS c FROM r14_cd;

-- ============================================================================
-- SECTION E: CORRESPONDING
-- ============================================================================

-- 7. UNION CORRESPONDING matches by name
-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT count(*)::text AS n FROM (
  SELECT 1 AS a, 2 AS b UNION CORRESPONDING SELECT 3 AS b, 4 AS a
) q;

-- ============================================================================
-- SECTION F: GROUP BY DISTINCT (PG 16+)
-- ============================================================================

CREATE TABLE r14_gbd (a int, b int);
INSERT INTO r14_gbd VALUES (1,1),(2,2);

-- 8. DISTINCT GROUPING SETS de-duplicated
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) <= 4)::text AS ok FROM (
  SELECT a, b FROM r14_gbd
  GROUP BY DISTINCT GROUPING SETS ((a), (a), (b))
) q;

-- ============================================================================
-- SECTION G: Mutual recursion in CTE
-- ============================================================================

-- 9. Two CTEs referencing each other
-- begin-expected-error
-- message-like: not implemented
-- end-expected-error
WITH RECURSIVE
  a(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM b WHERE n < 3),
  b(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM a WHERE n < 3)
SELECT (count(*) > 0)::text AS ok FROM a;

-- ============================================================================
-- SECTION H: MERGE WHEN NOT MATCHED BY SOURCE
-- ============================================================================

CREATE TABLE r14_mg_t (id int PRIMARY KEY, v int);
INSERT INTO r14_mg_t VALUES (1,10), (2,20), (3,30);
CREATE TABLE r14_mg_s (id int, v int);
INSERT INTO r14_mg_s VALUES (1,100), (2,200);
MERGE INTO r14_mg_t t USING r14_mg_s s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- 10. Rows 1,2 remain (updated); row 3 deleted
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*)::text AS c FROM r14_mg_t;

-- ============================================================================
-- SECTION I: range_agg with FILTER
-- ============================================================================

CREATE TABLE r14_ra (r int4range, incl boolean);
INSERT INTO r14_ra VALUES ('[1,5)', true), ('[10,20)', true), ('[30,40)', false);

-- 11. Only filtered ranges
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (range_agg(r) FILTER (WHERE incl)::text LIKE '%[1,5)%')::text AS ok FROM r14_ra;
