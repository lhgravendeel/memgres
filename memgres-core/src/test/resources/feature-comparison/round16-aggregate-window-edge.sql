-- ============================================================================
-- Feature Comparison: Round 16 — Aggregate / window edges
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r16_aw CASCADE;
CREATE SCHEMA r16_aw;
SET search_path = r16_aw, public;

-- ============================================================================
-- SECTION L1: Window FILTER
-- ============================================================================

CREATE TABLE wf (id int, x int);
INSERT INTO wf VALUES (1,5),(2,15),(3,25);

-- 1. count(*) FILTER (WHERE x>=15) OVER () = 2 for all rows
-- begin-expected
-- columns: id, c
-- row: 1, 2
-- row: 2, 2
-- row: 3, 2
-- end-expected
SELECT id, count(*) FILTER (WHERE x>=15) OVER ()::int AS c FROM wf ORDER BY id;

-- ============================================================================
-- SECTION L2: array_agg DISTINCT dedup correctness
-- ============================================================================

CREATE TABLE aad (a int, b int);
INSERT INTO aad VALUES (1, NULL), (NULL, 1);

-- 2. (1,NULL) and (NULL,1) are DISTINCT rows → array_length = 2
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT array_length(array_agg(DISTINCT ROW(a,b)), 1) AS n FROM aad;

-- ============================================================================
-- SECTION L3: percentile with NaN → SQLSTATE 22023
-- ============================================================================

-- 3. percentile_disc with NaN must raise 22023
-- begin-expected-error
-- sqlstate: 22023
-- end-expected-error
SELECT percentile_disc(ARRAY['nan'::float8, 0.5])
    WITHIN GROUP (ORDER BY x)
FROM (VALUES (1),(2),(3)) AS t(x);

-- ============================================================================
-- SECTION L4: nth_value FROM LAST / IGNORE NULLS
-- ============================================================================

CREATE TABLE nv (id int, v int);
INSERT INTO nv VALUES (1,10),(2,20),(3,30),(4,40);

-- 4. nth_value(v,2) FROM LAST over full frame = 30
-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT nth_value(v, 2) FROM LAST OVER
    (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS v
FROM nv ORDER BY id LIMIT 1;

CREATE TABLE nvn (id int, v int);
INSERT INTO nvn VALUES (1,NULL),(2,10),(3,NULL),(4,20);

-- 5. nth_value(v,2) IGNORE NULLS over full frame = 20
-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT nth_value(v, 2) IGNORE NULLS OVER
    (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS v
FROM nvn ORDER BY id LIMIT 1;
