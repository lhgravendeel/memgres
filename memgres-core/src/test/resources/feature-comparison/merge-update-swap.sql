-- ============================================================================
-- Feature Comparison: MERGE UPDATE SET evaluates from original row snapshot
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG: SET a = t.b, b = t.a swaps correctly because all SET RHS expressions
-- are evaluated against the original row, not the partially-mutated row.
-- ============================================================================

-- Setup
CREATE TABLE ms_t (id int PRIMARY KEY, a int, b int);
INSERT INTO ms_t VALUES (1, 10, 20);
CREATE TABLE ms_s (id int PRIMARY KEY);
INSERT INTO ms_s VALUES (1);

-- ============================================================================
-- 1. Column swap via MERGE UPDATE
-- ============================================================================

MERGE INTO ms_t t USING ms_s s ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = t.b, b = t.a;

-- begin-expected
-- columns: a|b
-- row: 20|10
-- end-expected
SELECT a, b FROM ms_t WHERE id = 1;

-- Cleanup
DROP TABLE ms_s;
DROP TABLE ms_t;
