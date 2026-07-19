-- ============================================================================
-- Feature Comparison: Self-referential FK RESTRICT allows DELETE of all rows
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG: DELETE FROM emp succeeds when all referencing rows are also deleted.
-- RESTRICT/NO_ACTION is checked at statement end, not per-row.
-- ============================================================================

-- Setup
CREATE TABLE emp_sr (id int PRIMARY KEY, mgr_id int REFERENCES emp_sr(id));
INSERT INTO emp_sr VALUES (1, NULL), (2, 1), (3, 2);

-- ============================================================================
-- 1. DELETE all rows succeeds
-- ============================================================================

DELETE FROM emp_sr;

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*) AS c FROM emp_sr;

-- Cleanup
DROP TABLE emp_sr;
