-- ============================================================================
-- Feature Comparison: Command Tags for Prepared Statements & Cursors
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Command tags are the server response strings sent after each command.
-- In psql they appear as the status line after each command.
--
-- Annotation format:
--   -- command: TAG   → expected command tag returned by PG
--   -- note: ...      → informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS tag_test CASCADE;
CREATE TABLE tag_test (id integer PRIMARY KEY, name text);
INSERT INTO tag_test VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');

DEALLOCATE ALL;

-- ============================================================================
-- 1. PREPARE command tag
-- ============================================================================

-- command: PREPARE
PREPARE tag_ps AS SELECT 1;

-- ============================================================================
-- 2. EXECUTE command tag (inherits from inner statement)
-- ============================================================================

-- note: PG EXECUTE returns the tag of the inner statement, not "EXECUTE"

-- command: SELECT 1
EXECUTE tag_ps;

PREPARE tag_ins (integer, text) AS INSERT INTO tag_test VALUES ($1, $2);

-- command: INSERT 0 1
EXECUTE tag_ins(4, 'delta');

PREPARE tag_upd (text, integer) AS UPDATE tag_test SET name = $1 WHERE id = $2;

-- command: UPDATE 1
EXECUTE tag_upd('DELTA', 4);

PREPARE tag_del (integer) AS DELETE FROM tag_test WHERE id = $1;

-- command: DELETE 1
EXECUTE tag_del(4);

DEALLOCATE ALL;

-- ============================================================================
-- 3. DEALLOCATE command tag
-- ============================================================================

PREPARE tag_dealloc AS SELECT 1;

-- command: DEALLOCATE
DEALLOCATE tag_dealloc;

-- ============================================================================
-- 4. DEALLOCATE ALL command tag
-- ============================================================================

PREPARE tag_da1 AS SELECT 1;
PREPARE tag_da2 AS SELECT 2;

-- command: DEALLOCATE ALL
DEALLOCATE ALL;

-- ============================================================================
-- 5. DECLARE CURSOR command tag
-- ============================================================================

BEGIN;

-- command: DECLARE CURSOR
DECLARE tag_cur CURSOR FOR SELECT id FROM tag_test ORDER BY id;

-- ============================================================================
-- 6. FETCH command tag
-- ============================================================================

-- note: PG returns "FETCH <count>" where count is the number of rows fetched

-- command: FETCH 1
FETCH NEXT FROM tag_cur;

-- command: FETCH 2
FETCH FORWARD 2 FROM tag_cur;

-- Fetch past end returns 0 rows
-- command: FETCH 0
FETCH NEXT FROM tag_cur;

CLOSE tag_cur;

-- Fetch all rows
DECLARE tag_cur2 CURSOR FOR SELECT id FROM tag_test ORDER BY id;

-- command: FETCH 3
FETCH ALL FROM tag_cur2;

CLOSE tag_cur2;

-- ============================================================================
-- 7. MOVE command tag
-- ============================================================================

-- note: PG returns "MOVE <count>" where count is the number of rows moved over

DECLARE tag_cur3 SCROLL CURSOR FOR SELECT id FROM tag_test ORDER BY id;

-- command: MOVE 1
MOVE NEXT IN tag_cur3;

-- command: MOVE 2
MOVE FORWARD 2 IN tag_cur3;

-- Already at end, move 0
-- command: MOVE 0
MOVE NEXT IN tag_cur3;

-- Move backward
-- command: MOVE 1
MOVE PRIOR IN tag_cur3;

CLOSE tag_cur3;

-- ============================================================================
-- 8. CLOSE command tag
-- ============================================================================

DECLARE tag_cur4 CURSOR FOR SELECT 1;

-- command: CLOSE CURSOR
CLOSE tag_cur4;

-- ============================================================================
-- 9. CLOSE ALL command tag
-- ============================================================================

DECLARE tag_cur5a CURSOR FOR SELECT 1;
DECLARE tag_cur5b CURSOR FOR SELECT 2;

-- command: CLOSE CURSOR
CLOSE ALL;

COMMIT;

-- ============================================================================
-- 10. DISCARD command tags
-- ============================================================================

-- command: DISCARD ALL
DISCARD ALL;

PREPARE tag_disc_ps AS SELECT 1;

-- command: DISCARD PLANS
DISCARD PLANS;

DEALLOCATE ALL;

CREATE TEMP TABLE tag_disc_temp (x int);

-- command: DISCARD TEMP
DISCARD TEMP;

-- command: DISCARD SEQUENCES
DISCARD SEQUENCES;

-- ============================================================================
-- Cleanup
-- ============================================================================

DEALLOCATE ALL;
DROP TABLE IF EXISTS tag_test CASCADE;
