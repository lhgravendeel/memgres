-- ============================================================================
-- Feature Comparison: Round 16 — Query features
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r16_qf CASCADE;
CREATE SCHEMA r16_qf;
SET search_path = r16_qf, public;

-- ============================================================================
-- SECTION H1: CTE MATERIALIZED / NOT MATERIALIZED
-- ============================================================================

-- 1. WITH ... AS MATERIALIZED parses & returns rows
-- begin-expected
-- columns: v
-- row: 1
-- end-expected
WITH w AS MATERIALIZED (SELECT 1 AS v) SELECT v FROM w;

-- 2. WITH ... AS NOT MATERIALIZED parses & returns rows
-- begin-expected
-- columns: v
-- row: 2
-- end-expected
WITH w AS NOT MATERIALIZED (SELECT 2 AS v) SELECT v FROM w;

-- ============================================================================
-- SECTION H2: CTE SEARCH / CYCLE
-- ============================================================================

CREATE TABLE edges (src int, dst int);
INSERT INTO edges VALUES (1,2),(2,3);

-- 3. SEARCH BREADTH FIRST BY n SET ord → projects ord column
-- begin-expected-error
-- message-like: cannot
-- end-expected-error
WITH RECURSIVE t(n, p) AS (
    SELECT src, ARRAY[src] FROM edges WHERE src=1
    UNION ALL
    SELECT e.dst, t.p || e.dst FROM t JOIN edges e ON t.n = e.src
) SEARCH BREADTH FIRST BY n SET ord
SELECT n, ord::int FROM t ORDER BY ord;

CREATE TABLE edges_cycle (src int, dst int);
INSERT INTO edges_cycle VALUES (1,2),(2,1);

-- 4. CYCLE n SET is_cycle USING path projects is_cycle + path
-- begin-expected
-- columns: has_cycle
-- row: t
-- end-expected
SELECT bool_or(is_cycle) AS has_cycle
FROM (
    WITH RECURSIVE t(n) AS (
        SELECT src FROM edges_cycle WHERE src=1
        UNION ALL
        SELECT e.dst FROM t JOIN edges_cycle e ON t.n=e.src
    ) CYCLE n SET is_cycle USING path
    SELECT is_cycle FROM t LIMIT 5
) x;

-- ============================================================================
-- SECTION H3: FOR UPDATE OF table list
-- ============================================================================

CREATE TABLE a (id int primary key, v int);
CREATE TABLE b (id int primary key, v int);
INSERT INTO a VALUES (1,10);
INSERT INTO b VALUES (1,20);

-- 5. SELECT ... FOR UPDATE OF a parses and returns
-- begin-expected
-- columns: v
-- row: 10
-- end-expected
SELECT a.v FROM a JOIN b USING (id) FOR UPDATE OF a;

-- ============================================================================
-- SECTION H4: FILTER on window aggregates
-- ============================================================================

CREATE TABLE wf (id int, x int);
INSERT INTO wf VALUES (1,10),(2,20),(3,30);

-- 6. count(*) FILTER (WHERE x > 15) OVER () = 2
-- begin-expected
-- columns: id, c
-- row: 1, 2
-- row: 2, 2
-- row: 3, 2
-- end-expected
SELECT id, count(*) FILTER (WHERE x > 15) OVER ()::int AS c
FROM wf ORDER BY id;

-- ============================================================================
-- SECTION H5: ORDER BY ... USING operator
-- ============================================================================

-- 7. ORDER BY v USING < sorts ascending
-- begin-expected
-- columns: v
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT v FROM (VALUES (3),(1),(2)) AS t(v) ORDER BY v USING <;

-- ============================================================================
-- SECTION H6: SOME as ANY synonym
-- ============================================================================

-- 8. 1 = SOME (...) works
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT 1 = SOME (SELECT unnest(ARRAY[1,2,3])) AS ok;

-- ============================================================================
-- SECTION H7: EXPLAIN option flags
-- ============================================================================

-- 9. EXPLAIN (BUFFERS) parses
-- begin-expected
-- columns: QUERY PLAN
-- row: Result  (cost=0.00..0.01 rows=1 width=4)
-- end-expected
EXPLAIN (BUFFERS) SELECT 1;

-- 10. EXPLAIN (WAL) parses
-- begin-expected-error
-- message-like: explain option wal
-- end-expected-error
EXPLAIN (WAL) SELECT 1;

-- ============================================================================
-- SECTION H8: TABLESAMPLE SYSTEM(100) returns all rows
-- ============================================================================

CREATE TABLE samp (id int);
INSERT INTO samp SELECT generate_series(1,100);

-- 11. TABLESAMPLE SYSTEM(100) REPEATABLE(1) → 100 rows
-- begin-expected
-- columns: n
-- row: 100
-- end-expected
SELECT count(*)::int AS n FROM samp TABLESAMPLE SYSTEM(100) REPEATABLE(1);
