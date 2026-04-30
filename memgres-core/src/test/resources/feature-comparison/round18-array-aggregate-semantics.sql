-- ============================================================================
-- Feature Comparison: Round 18 — Array / aggregate semantic-depth gaps
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION AA1: array_positions returns int[]
-- ============================================================================

-- 1. array_positions yields positions 1,3,5
-- begin-expected
-- columns: v
-- row: {1,3,5}
-- end-expected
SELECT array_positions(ARRAY[1,2,1,3,1], 1)::text AS v;

-- ============================================================================
-- SECTION AA2: array_sample 3-arg (seed) determinism
-- ============================================================================

-- 2. Same seed → same sample
-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT (array_sample(ARRAY[1,2,3,4,5,6,7,8,9,10], 3, 42)::text
        = array_sample(ARRAY[1,2,3,4,5,6,7,8,9,10], 3, 42)::text) AS ok;

-- ============================================================================
-- SECTION AA3: string_agg DISTINCT
-- ============================================================================

DROP TABLE IF EXISTS r18_sagg;
CREATE TABLE r18_sagg(v numeric);
INSERT INTO r18_sagg VALUES (1),(1.0),(2);

-- 3. DISTINCT on text(v)
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (string_agg(DISTINCT v::text, ',' ORDER BY v::text) LIKE '%2%') AS ok
  FROM r18_sagg;

-- ============================================================================
-- SECTION AA4: array_agg DISTINCT dedup
-- ============================================================================

DROP TABLE IF EXISTS r18_aagg;
CREATE TABLE r18_aagg(v int);
INSERT INTO r18_aagg VALUES (1),(2),(2),(3);

-- 4. array_agg DISTINCT dedups duplicates
-- begin-expected
-- columns: v
-- row: {1,2,3}
-- end-expected
SELECT array_agg(DISTINCT v ORDER BY v)::text AS v FROM r18_aagg;

-- ============================================================================
-- SECTION AA5: xmlagg registered
-- ============================================================================

-- 5. xmlagg in pg_aggregate
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n
  FROM pg_aggregate a
  JOIN pg_proc p ON p.oid=a.aggfnoid
 WHERE p.proname='xmlagg';
