-- ============================================================================
-- Feature Comparison: count(*) empty window frame → 0; json_agg zero rows → NULL
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- Setup
CREATE TABLE wcf_t (id int);
INSERT INTO wcf_t VALUES (1);

-- ============================================================================
-- 1. count(*) over empty frame = 0
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS cnt FROM wcf_t;

-- Cleanup
DROP TABLE wcf_t;

-- ============================================================================
-- 2. json_agg over zero rows = NULL
-- ============================================================================

CREATE TABLE jaz_t (id int);

-- begin-expected
-- columns: result
-- row:
-- end-expected
SELECT json_agg(id) AS result FROM jaz_t;

-- Cleanup
DROP TABLE jaz_t;
