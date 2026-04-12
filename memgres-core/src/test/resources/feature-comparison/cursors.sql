-- ============================================================================
-- Feature Comparison: Cursors (DECLARE / FETCH / MOVE / CLOSE)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   → expected result set
--   -- begin-expected-error / message-like: / end-expected-error → expected error
--   -- command: TAG                                       → expected command tag
--   -- note: ...                                          → informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS cur_test CASCADE;
CREATE TABLE cur_test (
    id   integer PRIMARY KEY,
    name text NOT NULL
);
INSERT INTO cur_test VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma'), (4, 'delta'), (5, 'epsilon');

-- ============================================================================
-- 1. Basic cursor operations (inside explicit transaction)
-- ============================================================================

BEGIN;

DECLARE c1 CURSOR FOR SELECT id, name FROM cur_test ORDER BY id;

-- 1a. FETCH NEXT
-- begin-expected
-- columns: id|name
-- row: 1|alpha
-- end-expected
FETCH NEXT FROM c1;

-- begin-expected
-- columns: id|name
-- row: 2|beta
-- end-expected
FETCH NEXT FROM c1;

-- 1b. FETCH (bare, same as NEXT)
-- begin-expected
-- columns: id|name
-- row: 3|gamma
-- end-expected
FETCH FROM c1;

-- 1c. FETCH FORWARD N
-- begin-expected
-- columns: id|name
-- row: 4|delta
-- row: 5|epsilon
-- end-expected
FETCH FORWARD 2 FROM c1;

-- 1d. FETCH past end returns empty
-- begin-expected
-- columns: id|name
-- end-expected
FETCH NEXT FROM c1;

CLOSE c1;
COMMIT;

-- ============================================================================
-- 2. FETCH ALL / FORWARD ALL
-- ============================================================================

BEGIN;

DECLARE c2 CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- 2a. FETCH ALL
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- row: 5
-- end-expected
FETCH ALL FROM c2;

-- 2b. FETCH ALL again (already exhausted)
-- begin-expected
-- columns: id
-- end-expected
FETCH ALL FROM c2;

CLOSE c2;

-- 2c. FORWARD ALL (synonym)
DECLARE c2b CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- row: 5
-- end-expected
FETCH FORWARD ALL FROM c2b;

CLOSE c2b;
COMMIT;

-- ============================================================================
-- 3. SCROLL cursor: backward directions
-- ============================================================================

BEGIN;

DECLARE c3 SCROLL CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- 3a. Move forward
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT FROM c3;

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH NEXT FROM c3;

-- begin-expected
-- columns: id
-- row: 3
-- end-expected
FETCH NEXT FROM c3;

-- 3b. FETCH PRIOR
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH PRIOR FROM c3;

-- 3c. FETCH FIRST
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH FIRST FROM c3;

-- 3d. FETCH LAST
-- begin-expected
-- columns: id
-- row: 5
-- end-expected
FETCH LAST FROM c3;

-- 3e. FETCH ABSOLUTE
-- begin-expected
-- columns: id
-- row: 3
-- end-expected
FETCH ABSOLUTE 3 FROM c3;

-- 3f. FETCH ABSOLUTE negative (from end)
-- begin-expected
-- columns: id
-- row: 4
-- end-expected
FETCH ABSOLUTE -2 FROM c3;

-- 3g. FETCH RELATIVE
-- begin-expected
-- columns: id
-- row: 5
-- end-expected
FETCH RELATIVE 1 FROM c3;

-- begin-expected
-- columns: id
-- row: 3
-- end-expected
FETCH RELATIVE -2 FROM c3;

-- 3h. FETCH BACKWARD
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH BACKWARD 1 FROM c3;

-- 3i. FETCH BACKWARD ALL
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH BACKWARD ALL FROM c3;

CLOSE c3;
COMMIT;

-- ============================================================================
-- 4. NO SCROLL enforcement
-- ============================================================================

BEGIN;

DECLARE c4 CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- 4a. Forward is OK
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT FROM c4;

-- 4b. PRIOR rejected
-- begin-expected-error
-- message-like: cursor can only scan forward
-- end-expected-error
FETCH PRIOR FROM c4;

-- 4c. LAST rejected
-- begin-expected-error
-- message-like: cursor can only scan forward
-- end-expected-error
FETCH LAST FROM c4;

-- 4d. FIRST rejected
-- begin-expected-error
-- message-like: cursor can only scan forward
-- end-expected-error
FETCH FIRST FROM c4;

-- 4e. ABSOLUTE rejected
-- begin-expected-error
-- message-like: cursor can only scan forward
-- end-expected-error
FETCH ABSOLUTE 1 FROM c4;

-- 4f. Negative RELATIVE rejected
-- begin-expected-error
-- message-like: cursor can only scan forward
-- end-expected-error
FETCH RELATIVE -1 FROM c4;

-- 4g. Positive RELATIVE allowed (forward)
-- begin-expected
-- columns: id
-- row: 3
-- end-expected
FETCH RELATIVE 1 FROM c4;

-- 4h. BACKWARD rejected
-- begin-expected-error
-- message-like: cursor can only scan forward
-- end-expected-error
FETCH BACKWARD 1 FROM c4;

CLOSE c4;
ROLLBACK;

-- ============================================================================
-- 5. Cursor position edge cases
-- ============================================================================

BEGIN;

DECLARE c5 SCROLL CURSOR FOR SELECT id FROM cur_test WHERE id <= 3 ORDER BY id;

-- 5a. FETCH ABSOLUTE 0 → before first, no row returned
-- begin-expected
-- columns: id
-- end-expected
FETCH ABSOLUTE 0 FROM c5;

-- 5b. After ABSOLUTE 0, NEXT returns first row
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT FROM c5;

-- 5c. FETCH past end → position moves to "after last"
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH NEXT FROM c5;

-- begin-expected
-- columns: id
-- row: 3
-- end-expected
FETCH NEXT FROM c5;

-- begin-expected
-- columns: id
-- end-expected
FETCH NEXT FROM c5;

-- 5d. After past-end, PRIOR returns last row
-- begin-expected
-- columns: id
-- row: 3
-- end-expected
FETCH PRIOR FROM c5;

-- 5e. PRIOR from first row → before first
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH FIRST FROM c5;

-- begin-expected
-- columns: id
-- end-expected
FETCH PRIOR FROM c5;

-- 5f. After before-first, NEXT returns first
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT FROM c5;

CLOSE c5;

-- 5g. BACKWARD ALL positions before first
DECLARE c5b SCROLL CURSOR FOR SELECT id FROM cur_test WHERE id <= 3 ORDER BY id;
FETCH LAST FROM c5b;

-- begin-expected
-- columns: id
-- row: 2
-- row: 1
-- end-expected
FETCH BACKWARD ALL FROM c5b;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT FROM c5b;

CLOSE c5b;

-- 5h. FORWARD ALL positions after last
DECLARE c5c SCROLL CURSOR FOR SELECT id FROM cur_test WHERE id <= 3 ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- end-expected
FETCH FORWARD ALL FROM c5c;

-- begin-expected
-- columns: id
-- row: 3
-- end-expected
FETCH PRIOR FROM c5c;

CLOSE c5c;
COMMIT;

-- ============================================================================
-- 6. MOVE (position without returning rows)
-- ============================================================================

BEGIN;

DECLARE c6 SCROLL CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- 6a. MOVE NEXT
MOVE NEXT IN c6;

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH NEXT FROM c6;

-- 6b. MOVE FORWARD N
MOVE FORWARD 2 IN c6;

-- begin-expected
-- columns: id
-- row: 5
-- end-expected
FETCH NEXT FROM c6;

-- 6c. MOVE ABSOLUTE
MOVE ABSOLUTE 1 IN c6;

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH NEXT FROM c6;

-- 6d. MOVE FIRST
MOVE LAST IN c6;
MOVE FIRST IN c6;

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH NEXT FROM c6;

CLOSE c6;
COMMIT;

-- ============================================================================
-- 7. WITH HOLD cursors
-- ============================================================================

BEGIN;

-- 7a. Regular cursor destroyed on COMMIT
DECLARE c7_nohold CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- 7b. WITH HOLD survives COMMIT
DECLARE c7_hold CURSOR WITH HOLD FOR SELECT id FROM cur_test ORDER BY id;

COMMIT;

-- 7c. Regular cursor gone
-- begin-expected-error
-- message-like: cursor "c7_nohold" does not exist
-- end-expected-error
FETCH NEXT FROM c7_nohold;

-- 7d. Holdable cursor still works
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT FROM c7_hold;

CLOSE c7_hold;

-- ============================================================================
-- 8. WITH HOLD + SCROLL combination
-- ============================================================================

BEGIN;

DECLARE c8 SCROLL CURSOR WITH HOLD FOR SELECT id FROM cur_test ORDER BY id;

COMMIT;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT FROM c8;

-- begin-expected
-- columns: id
-- row: 5
-- end-expected
FETCH LAST FROM c8;

-- begin-expected
-- columns: id
-- row: 4
-- end-expected
FETCH PRIOR FROM c8;

CLOSE c8;

-- ============================================================================
-- 9. BINARY cursor flag
-- ============================================================================

-- note: BINARY affects wire format, not SQL-level results; just test parsing
BEGIN;

DECLARE c9 BINARY CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT FROM c9;

CLOSE c9;
COMMIT;

-- ============================================================================
-- 10. Transaction lifecycle
-- ============================================================================

-- 10a. All cursors destroyed on ROLLBACK (even WITH HOLD)
BEGIN;
DECLARE c10_hold CURSOR WITH HOLD FOR SELECT 1;
DECLARE c10_nohold CURSOR FOR SELECT 2;
ROLLBACK;

-- begin-expected-error
-- message-like: cursor "c10_hold" does not exist
-- end-expected-error
FETCH NEXT FROM c10_hold;

-- begin-expected-error
-- message-like: cursor "c10_nohold" does not exist
-- end-expected-error
FETCH NEXT FROM c10_nohold;

-- 10b. Mixed holdable/non-holdable on COMMIT
BEGIN;
DECLARE c10_a CURSOR WITH HOLD FOR SELECT 'holdable' AS kind;
DECLARE c10_b CURSOR FOR SELECT 'non-holdable' AS kind;
COMMIT;

-- begin-expected
-- columns: kind
-- row: holdable
-- end-expected
FETCH NEXT FROM c10_a;

-- begin-expected-error
-- message-like: cursor "c10_b" does not exist
-- end-expected-error
FETCH NEXT FROM c10_b;

CLOSE c10_a;

-- ============================================================================
-- 11. CLOSE
-- ============================================================================

BEGIN;
DECLARE c11a CURSOR FOR SELECT 1;
DECLARE c11b CURSOR FOR SELECT 2;

-- 11a. Close specific cursor
CLOSE c11a;

-- begin-expected-error
-- message-like: cursor "c11a" does not exist
-- end-expected-error
FETCH NEXT FROM c11a;

-- 11b. Other cursor still open
-- begin-expected
-- columns: ?column?
-- row: 2
-- end-expected
FETCH NEXT FROM c11b;

-- 11c. CLOSE ALL
CLOSE ALL;

-- begin-expected-error
-- message-like: cursor "c11b" does not exist
-- end-expected-error
FETCH NEXT FROM c11b;

ROLLBACK;

-- ============================================================================
-- 12. Error cases
-- ============================================================================

BEGIN;

-- 12a. Fetch from nonexistent cursor
-- begin-expected-error
-- message-like: cursor "nope" does not exist
-- end-expected-error
FETCH NEXT FROM nope;

-- 12b. Duplicate cursor name
DECLARE c12 CURSOR FOR SELECT 1;

-- begin-expected-error
-- message-like: cursor "c12" already exists
-- end-expected-error
DECLARE c12 CURSOR FOR SELECT 2;

-- 12c. Close nonexistent
-- begin-expected-error
-- message-like: cursor "nope" does not exist
-- end-expected-error
CLOSE nope;

ROLLBACK;

-- ============================================================================
-- 13. Cursor with ORDER BY DESC
-- ============================================================================

BEGIN;
DECLARE c13 CURSOR FOR SELECT id FROM cur_test ORDER BY id DESC;

-- begin-expected
-- columns: id
-- row: 5
-- end-expected
FETCH NEXT FROM c13;

-- begin-expected
-- columns: id
-- row: 4
-- end-expected
FETCH NEXT FROM c13;

CLOSE c13;
COMMIT;

-- ============================================================================
-- 14. Cursor with WHERE clause
-- ============================================================================

BEGIN;
DECLARE c14 CURSOR FOR SELECT name FROM cur_test WHERE id BETWEEN 2 AND 4 ORDER BY id;

-- begin-expected
-- columns: name
-- row: beta
-- row: gamma
-- row: delta
-- end-expected
FETCH ALL FROM c14;

CLOSE c14;
COMMIT;

-- ============================================================================
-- 15. Cursor with JOIN
-- ============================================================================

DROP TABLE IF EXISTS cur_tags CASCADE;
CREATE TABLE cur_tags (item_id integer, tag text);
INSERT INTO cur_tags VALUES (1, 'fast'), (2, 'slow'), (3, 'fast');

BEGIN;
DECLARE c15 CURSOR FOR
    SELECT t.name, g.tag
    FROM cur_test t
    JOIN cur_tags g ON t.id = g.item_id
    ORDER BY t.id;

-- begin-expected
-- columns: name|tag
-- row: alpha|fast
-- row: beta|slow
-- row: gamma|fast
-- end-expected
FETCH ALL FROM c15;

CLOSE c15;
COMMIT;

DROP TABLE IF EXISTS cur_tags CASCADE;

-- ============================================================================
-- 16. Cursor with aggregate query
-- ============================================================================

BEGIN;
DECLARE c16 CURSOR FOR
    SELECT count(*) AS cnt, min(id) AS lo, max(id) AS hi FROM cur_test;

-- begin-expected
-- columns: cnt|lo|hi
-- row: 5|1|5
-- end-expected
FETCH NEXT FROM c16;

CLOSE c16;
COMMIT;

-- ============================================================================
-- 17. Multiple cursors open simultaneously
-- ============================================================================

BEGIN;
DECLARE ca CURSOR FOR SELECT id FROM cur_test ORDER BY id;
DECLARE cb CURSOR FOR SELECT name FROM cur_test ORDER BY id DESC;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT FROM ca;

-- begin-expected
-- columns: name
-- row: epsilon
-- end-expected
FETCH NEXT FROM cb;

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH NEXT FROM ca;

-- begin-expected
-- columns: name
-- row: delta
-- end-expected
FETCH NEXT FROM cb;

CLOSE ALL;
COMMIT;

-- ============================================================================
-- 18. Empty result set cursor
-- ============================================================================

BEGIN;
DECLARE c18 CURSOR FOR SELECT id FROM cur_test WHERE id > 999;

-- begin-expected
-- columns: id
-- end-expected
FETCH ALL FROM c18;

CLOSE c18;
COMMIT;

-- ============================================================================
-- 19. FETCH with IN keyword (synonym for FROM)
-- ============================================================================

BEGIN;
DECLARE c19 CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT IN c19;

CLOSE c19;
COMMIT;

-- ============================================================================
-- 20. Explicit NO SCROLL keyword
-- ============================================================================

BEGIN;
DECLARE c20 NO SCROLL CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- Forward OK
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH NEXT FROM c20;

-- Backward rejected (same as implicit no scroll)
-- begin-expected-error
-- message-like: cursor can only scan forward
-- end-expected-error
FETCH PRIOR FROM c20;

CLOSE c20;
ROLLBACK;

-- ============================================================================
-- 21. FETCH FORWARD 0 / BACKWARD 0 (no-op, no movement)
-- ============================================================================

BEGIN;
DECLARE c21 SCROLL CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- Move to row 3
FETCH ABSOLUTE 3 FROM c21;

-- FETCH FORWARD 0: returns nothing, position unchanged
-- begin-expected
-- columns: id
-- end-expected
FETCH FORWARD 0 FROM c21;

-- Verify position unchanged: NEXT returns row 4
-- begin-expected
-- columns: id
-- row: 4
-- end-expected
FETCH NEXT FROM c21;

-- Move back to row 2
FETCH ABSOLUTE 2 FROM c21;

-- FETCH BACKWARD 0: returns nothing, position unchanged
-- begin-expected
-- columns: id
-- end-expected
FETCH BACKWARD 0 FROM c21;

-- Verify position unchanged: NEXT returns row 3
-- begin-expected
-- columns: id
-- row: 3
-- end-expected
FETCH NEXT FROM c21;

CLOSE c21;
COMMIT;

-- ============================================================================
-- 22. MOVE with NO SCROLL enforcement
-- ============================================================================

BEGIN;
DECLARE c22 CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- Forward MOVE OK
MOVE NEXT IN c22;

-- Backward MOVE rejected
-- begin-expected-error
-- message-like: cursor can only scan forward
-- end-expected-error
MOVE PRIOR IN c22;

-- MOVE ABSOLUTE rejected
-- begin-expected-error
-- message-like: cursor can only scan forward
-- end-expected-error
MOVE ABSOLUTE 1 IN c22;

-- MOVE LAST rejected
-- begin-expected-error
-- message-like: cursor can only scan forward
-- end-expected-error
MOVE LAST IN c22;

CLOSE c22;
ROLLBACK;

-- ============================================================================
-- 23. Cursor with LIMIT
-- ============================================================================

BEGIN;
DECLARE c23 CURSOR FOR SELECT id FROM cur_test ORDER BY id LIMIT 3;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- end-expected
FETCH ALL FROM c23;

CLOSE c23;
COMMIT;

-- ============================================================================
-- 24. Cursor with DISTINCT
-- ============================================================================

DROP TABLE IF EXISTS cur_dup CASCADE;
CREATE TABLE cur_dup (val text);
INSERT INTO cur_dup VALUES ('a'), ('b'), ('a'), ('c'), ('b');

BEGIN;
DECLARE c24 CURSOR FOR SELECT DISTINCT val FROM cur_dup ORDER BY val;

-- begin-expected
-- columns: val
-- row: a
-- row: b
-- row: c
-- end-expected
FETCH ALL FROM c24;

CLOSE c24;
COMMIT;

DROP TABLE IF EXISTS cur_dup CASCADE;

-- ============================================================================
-- 25. Cursor with GROUP BY
-- ============================================================================

BEGIN;
DECLARE c25 CURSOR FOR
    SELECT name, count(*) AS cnt
    FROM (VALUES ('a'),('a'),('b'),('c'),('c'),('c')) AS t(name)
    GROUP BY name
    ORDER BY name;

-- begin-expected
-- columns: name|cnt
-- row: a|2
-- row: b|1
-- row: c|3
-- end-expected
FETCH ALL FROM c25;

CLOSE c25;
COMMIT;

-- ============================================================================
-- 26. Cursor with CTE
-- ============================================================================

BEGIN;
DECLARE c26 CURSOR FOR
    WITH top3 AS (SELECT id, name FROM cur_test ORDER BY id LIMIT 3)
    SELECT name FROM top3 ORDER BY id;

-- begin-expected
-- columns: name
-- row: alpha
-- row: beta
-- row: gamma
-- end-expected
FETCH ALL FROM c26;

CLOSE c26;
COMMIT;

-- ============================================================================
-- 27. Cursor with UNION
-- ============================================================================

BEGIN;
DECLARE c27 CURSOR FOR
    SELECT id, name FROM cur_test WHERE id <= 2
    UNION ALL
    SELECT id, name FROM cur_test WHERE id = 5
    ORDER BY id;

-- begin-expected
-- columns: id|name
-- row: 1|alpha
-- row: 2|beta
-- row: 5|epsilon
-- end-expected
FETCH ALL FROM c27;

CLOSE c27;
COMMIT;

-- ============================================================================
-- 28. FETCH count exceeds total rows
-- ============================================================================

BEGIN;
DECLARE c28 CURSOR FOR SELECT id FROM cur_test WHERE id <= 2 ORDER BY id;

-- Fetch more rows than exist: returns only available rows
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
FETCH FORWARD 100 FROM c28;

CLOSE c28;
COMMIT;

-- ============================================================================
-- 29. Cursor name reuse after CLOSE
-- ============================================================================

BEGIN;
DECLARE c29 CURSOR FOR SELECT 'first' AS version;

-- begin-expected
-- columns: version
-- row: first
-- end-expected
FETCH NEXT FROM c29;

CLOSE c29;

-- Reuse same name
DECLARE c29 CURSOR FOR SELECT 'second' AS version;

-- begin-expected
-- columns: version
-- row: second
-- end-expected
FETCH NEXT FROM c29;

CLOSE c29;
COMMIT;

-- ============================================================================
-- 30. DECLARE CURSOR requires transaction block (non-holdable)
-- ============================================================================

-- note: In PG, non-holdable DECLARE CURSOR outside BEGIN/COMMIT fails:
--       "DECLARE CURSOR can only be used in transaction blocks"
-- WITH HOLD cursors can be declared outside transactions.

-- begin-expected-error
-- message-like: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE c30_nohold CURSOR FOR SELECT 1;

-- WITH HOLD works outside transaction
DECLARE c30_hold CURSOR WITH HOLD FOR SELECT 'outside txn' AS val;

-- begin-expected
-- columns: val
-- row: outside txn
-- end-expected
FETCH NEXT FROM c30_hold;

CLOSE c30_hold;

-- ============================================================================
-- 31. Cursor with subquery in WHERE
-- ============================================================================

BEGIN;
DECLARE c31 CURSOR FOR
    SELECT name FROM cur_test
    WHERE id IN (SELECT id FROM cur_test WHERE id % 2 = 1)
    ORDER BY id;

-- begin-expected
-- columns: name
-- row: alpha
-- row: gamma
-- row: epsilon
-- end-expected
FETCH ALL FROM c31;

CLOSE c31;
COMMIT;

-- ============================================================================
-- 32. BACKWARD N returns rows in reverse order
-- ============================================================================

BEGIN;
DECLARE c32 SCROLL CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- Move to end
FETCH LAST FROM c32;

-- BACKWARD 3: returns rows in reverse order from current position
-- begin-expected
-- columns: id
-- row: 4
-- row: 3
-- row: 2
-- end-expected
FETCH BACKWARD 3 FROM c32;

CLOSE c32;
COMMIT;

-- ============================================================================
-- 33. FETCH FORWARD 1 (explicit count, equivalent to NEXT)
-- ============================================================================

BEGIN;
DECLARE c33 CURSOR FOR SELECT id FROM cur_test ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH FORWARD 1 FROM c33;

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH FORWARD 1 FROM c33;

CLOSE c33;
COMMIT;

-- ============================================================================
-- 34. Cursor over expression-only query (no table)
-- ============================================================================

BEGIN;
DECLARE c34 CURSOR FOR SELECT 1 AS a, 'hello' AS b, true AS c;

-- begin-expected
-- columns: a|b|c
-- row: 1|hello|true
-- end-expected
FETCH NEXT FROM c34;

-- Second fetch returns nothing (only one row)
-- begin-expected
-- columns: a|b|c
-- end-expected
FETCH NEXT FROM c34;

CLOSE c34;
COMMIT;

-- ============================================================================
-- 35. Cursor with generate_series
-- ============================================================================

BEGIN;
DECLARE c35 CURSOR FOR SELECT generate_series(1, 5) AS n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- row: 5
-- end-expected
FETCH ALL FROM c35;

CLOSE c35;
COMMIT;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS cur_test CASCADE;
