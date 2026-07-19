-- ============================================================================
-- Feature Comparison: View DML resolution (C1, H3, H4, H5, M5)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- Setup
CREATE TABLE vdml_t (id serial PRIMARY KEY, n INT, label TEXT DEFAULT 'none');
INSERT INTO vdml_t (n, label) VALUES (1, 'one'), (2, 'two'), (3, 'three');

-- ============================================================================
-- C1. DML on aggregate views must error 55000
-- ============================================================================

CREATE VIEW vdml_agg AS SELECT sum(n) AS s FROM vdml_t;

DELETE FROM vdml_agg;

-- begin-expected-error
-- 55000
-- end-expected

INSERT INTO vdml_agg VALUES (99);

-- begin-expected-error
-- 55000
-- end-expected

UPDATE vdml_agg SET s = 0;

-- begin-expected-error
-- 55000
-- end-expected

DROP VIEW vdml_agg;

-- ============================================================================
-- H3. DML through views with renamed columns
-- ============================================================================

CREATE VIEW vdml_renamed AS SELECT id, n AS num, label AS lbl FROM vdml_t;

UPDATE vdml_renamed SET num = 99 WHERE id = 1;

SELECT n FROM vdml_t WHERE id = 1;

-- begin-expected
-- columns: n
-- row: 99
-- end-expected

INSERT INTO vdml_renamed (num, lbl) VALUES (7, 'seven');

SELECT n, label FROM vdml_t WHERE n = 7;

-- begin-expected
-- columns: n|label
-- row: 7|seven
-- end-expected

DELETE FROM vdml_renamed WHERE num = 99;

SELECT count(*) FROM vdml_t WHERE n = 99;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected

DROP VIEW vdml_renamed;

-- ============================================================================
-- H5. WITH CASCADED CHECK OPTION checks parent-view predicates
-- ============================================================================

CREATE VIEW vdml_pos AS SELECT id, n, label FROM vdml_t WHERE n > 0;
CREATE VIEW vdml_bound AS SELECT id, n, label FROM vdml_pos WHERE n < 100 WITH CASCADED CHECK OPTION;

INSERT INTO vdml_bound (n) VALUES (-5);

-- begin-expected-error
-- 44000
-- end-expected

INSERT INTO vdml_bound (n) VALUES (5);

SELECT count(*) FROM vdml_t WHERE n = 5;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected

DROP VIEW vdml_bound;
DROP VIEW vdml_pos;

-- ============================================================================
-- M5. CREATE OR REPLACE VIEW rejects column renames
-- ============================================================================

CREATE VIEW vdml_replace AS SELECT id, n FROM vdml_t;

CREATE OR REPLACE VIEW vdml_replace AS SELECT id, n AS num FROM vdml_t;

-- begin-expected-error
-- 42P16
-- end-expected

DROP VIEW vdml_replace;

-- Cleanup
DROP TABLE vdml_t CASCADE;
