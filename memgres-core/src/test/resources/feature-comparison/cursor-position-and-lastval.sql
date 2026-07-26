-- ============================================================================
-- Feature Comparison: cursor positioning and lastval
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- WHERE CURRENT OF names the row a cursor is sitting on. A cursor that has not
-- fetched yet is on no row at all, and PostgreSQL says so instead of quietly
-- matching nothing -- which would silently skip the update.
-- ============================================================================

DROP TABLE IF EXISTS cpl CASCADE;
CREATE TABLE cpl (id int PRIMARY KEY, v int);
INSERT INTO cpl VALUES (1,10),(2,20);

-- ============================================================================
-- 1. A cursor before its first fetch is on no row
-- ============================================================================

BEGIN;

DECLARE cpl_c CURSOR FOR SELECT id FROM cpl ORDER BY id;

-- begin-expected-error
-- sqlstate: 24000
-- message-like: is not positioned on a row
-- end-expected-error
UPDATE cpl SET v = 99 WHERE CURRENT OF cpl_c;

ROLLBACK;

-- ============================================================================
-- 2. After a fetch it names that row
-- ============================================================================

BEGIN;

DECLARE cpl_c2 CURSOR FOR SELECT id FROM cpl ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH 1 FROM cpl_c2;

UPDATE cpl SET v = 99 WHERE CURRENT OF cpl_c2;

COMMIT;

-- begin-expected
-- columns: id | v
-- row: 1, 99
-- row: 2, 20
-- end-expected
SELECT id, v FROM cpl ORDER BY id;

DROP TABLE cpl;
