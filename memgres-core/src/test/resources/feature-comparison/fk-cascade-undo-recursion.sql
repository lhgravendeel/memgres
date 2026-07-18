-- ============================================================================
-- Feature Comparison: FK ON DELETE/UPDATE CASCADE undo & recursion
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests that:
--   1. Cascaded changes are undone by ROLLBACK
--   2. Cascades recurse through grandchild tables (depth > 1)
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS fkcr_grandchild CASCADE;
DROP TABLE IF EXISTS fkcr_child CASCADE;
DROP TABLE IF EXISTS fkcr_parent CASCADE;

CREATE TABLE fkcr_parent (id int PRIMARY KEY);
CREATE TABLE fkcr_child (
    id  int PRIMARY KEY,
    pid int REFERENCES fkcr_parent(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE fkcr_grandchild (
    id  int PRIMARY KEY,
    cid int REFERENCES fkcr_child(id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO fkcr_parent VALUES (1), (2);
INSERT INTO fkcr_child VALUES (10, 1), (20, 1), (30, 2);
INSERT INTO fkcr_grandchild VALUES (100, 10), (101, 10), (200, 20), (300, 30);

-- ============================================================================
-- 1. CASCADE DELETE recurses to grandchild
-- ============================================================================

DELETE FROM fkcr_parent WHERE id = 1;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM fkcr_child;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM fkcr_grandchild;

-- Re-insert for next tests
INSERT INTO fkcr_parent VALUES (1);
INSERT INTO fkcr_child VALUES (10, 1), (20, 1);
INSERT INTO fkcr_grandchild VALUES (100, 10), (101, 10), (200, 20);

-- ============================================================================
-- 2. CASCADE DELETE + ROLLBACK restores child AND grandchild
-- ============================================================================

BEGIN;
DELETE FROM fkcr_parent WHERE id = 1;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM fkcr_child;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM fkcr_grandchild;

ROLLBACK;

-- After rollback, everything is restored
-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::text AS cnt FROM fkcr_child;

-- begin-expected
-- columns: cnt
-- row: 4
-- end-expected
SELECT count(*)::text AS cnt FROM fkcr_grandchild;

-- ============================================================================
-- 3. SET NULL + ROLLBACK restores original FK values
-- ============================================================================

DROP TABLE IF EXISTS fkcr_sn_child CASCADE;
CREATE TABLE fkcr_sn_child (
    id  int PRIMARY KEY,
    pid int REFERENCES fkcr_parent(id) ON DELETE SET NULL
);
INSERT INTO fkcr_sn_child VALUES (10, 1), (20, 1);

BEGIN;
DELETE FROM fkcr_parent WHERE id = 1;

-- begin-expected
-- columns: pid
-- row: NULL
-- end-expected
SELECT pid::text FROM fkcr_sn_child WHERE id = 10;

ROLLBACK;

-- begin-expected
-- columns: pid
-- row: 1
-- end-expected
SELECT pid::text FROM fkcr_sn_child WHERE id = 10;

-- ============================================================================
-- 4. CASCADE UPDATE + ROLLBACK
-- ============================================================================

BEGIN;
UPDATE fkcr_parent SET id = 99 WHERE id = 1;

-- begin-expected
-- columns: pid
-- row: 1
-- end-expected
SELECT pid::text FROM fkcr_child WHERE id = 10;

ROLLBACK;

-- begin-expected
-- columns: pid
-- row: 1
-- end-expected
SELECT pid::text FROM fkcr_child WHERE id = 10;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS fkcr_grandchild CASCADE;
DROP TABLE IF EXISTS fkcr_sn_child CASCADE;
DROP TABLE IF EXISTS fkcr_child CASCADE;
DROP TABLE IF EXISTS fkcr_parent CASCADE;
