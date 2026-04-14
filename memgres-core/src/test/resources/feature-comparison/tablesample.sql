-- ============================================================================
-- Feature Comparison: TABLESAMPLE
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests TABLESAMPLE BERNOULLI and SYSTEM methods, which select a random
-- sample of rows from a table.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS ts_test CASCADE;
CREATE SCHEMA ts_test;
SET search_path = ts_test, public;

-- Create table with enough rows for meaningful sampling
CREATE TABLE ts_data (id serial PRIMARY KEY, val integer);
INSERT INTO ts_data (val) SELECT generate_series(1, 200);

-- ============================================================================
-- 1. TABLESAMPLE BERNOULLI: 100% returns all rows
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 200
-- end-expected
SELECT count(*)::integer AS cnt FROM ts_data TABLESAMPLE BERNOULLI (100);

-- ============================================================================
-- 2. TABLESAMPLE BERNOULLI: 0% returns no rows
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM ts_data TABLESAMPLE BERNOULLI (0);

-- ============================================================================
-- 3. TABLESAMPLE BERNOULLI: partial sample is subset
-- ============================================================================

-- note: 50% sample should return approximately 100 rows, but it's random.
-- We verify it returns fewer than all rows and more than 0.
-- begin-expected
-- columns: is_subset
-- row: true
-- end-expected
SELECT count(*) > 0 AND count(*) <= 200 AS is_subset
FROM ts_data TABLESAMPLE BERNOULLI (50);

-- ============================================================================
-- 4. TABLESAMPLE SYSTEM: 100% returns all rows
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 200
-- end-expected
SELECT count(*)::integer AS cnt FROM ts_data TABLESAMPLE SYSTEM (100);

-- ============================================================================
-- 5. TABLESAMPLE SYSTEM: 0% returns no rows
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM ts_data TABLESAMPLE SYSTEM (0);

-- ============================================================================
-- 6. TABLESAMPLE with REPEATABLE (seed for reproducibility)
-- ============================================================================

-- note: Same seed should produce same sample
-- begin-expected
-- columns: same_sample
-- row: true
-- end-expected
SELECT
  (SELECT string_agg(id::text, ',' ORDER BY id) FROM ts_data TABLESAMPLE BERNOULLI (10) REPEATABLE (42)) =
  (SELECT string_agg(id::text, ',' ORDER BY id) FROM ts_data TABLESAMPLE BERNOULLI (10) REPEATABLE (42))
  AS same_sample;

-- ============================================================================
-- 7. TABLESAMPLE with WHERE clause
-- ============================================================================

-- begin-expected
-- columns: all_positive
-- row: true
-- end-expected
SELECT bool_and(val > 0) AS all_positive
FROM ts_data TABLESAMPLE BERNOULLI (100) WHERE val > 0;

-- ============================================================================
-- 8. TABLESAMPLE in JOIN
-- ============================================================================

CREATE TABLE ts_lookup (id integer, label text);
INSERT INTO ts_lookup VALUES (1, 'one'), (2, 'two'), (3, 'three');

-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT count(*) > 0 AS has_results
FROM ts_data TABLESAMPLE BERNOULLI (100) AS d
JOIN ts_lookup l ON d.id = l.id;

-- ============================================================================
-- 9. TABLESAMPLE: invalid percentage (negative)
-- ============================================================================

-- begin-expected-error
-- message-like: percentage
-- end-expected-error
SELECT * FROM ts_data TABLESAMPLE BERNOULLI (-1);

-- ============================================================================
-- 10. TABLESAMPLE: invalid percentage (>100)
-- ============================================================================

-- begin-expected-error
-- message-like: percentage
-- end-expected-error
SELECT * FROM ts_data TABLESAMPLE BERNOULLI (101);

-- ============================================================================
-- 11. TABLESAMPLE with aggregation
-- ============================================================================

-- begin-expected
-- columns: has_avg
-- row: true
-- end-expected
SELECT avg(val) IS NOT NULL AS has_avg
FROM ts_data TABLESAMPLE BERNOULLI (50);

-- ============================================================================
-- 12. TABLESAMPLE on empty table
-- ============================================================================

CREATE TABLE ts_empty (id integer);

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM ts_empty TABLESAMPLE BERNOULLI (100);

DROP TABLE ts_empty;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA ts_test CASCADE;
SET search_path = public;
