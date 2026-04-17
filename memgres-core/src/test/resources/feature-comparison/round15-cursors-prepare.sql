-- ============================================================================
-- Feature Comparison: Round 15 — SQL-level cursors + prepared statements
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP TABLE IF EXISTS r15_cur;
CREATE TABLE r15_cur (id int PRIMARY KEY, label text);
INSERT INTO r15_cur VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');

-- ============================================================================
-- SECTION A: DECLARE INSENSITIVE / BINARY / SCROLL / WITH HOLD
-- ============================================================================

BEGIN;

-- 1. INSENSITIVE cursor
DECLARE c_ins INSENSITIVE CURSOR FOR SELECT id FROM r15_cur ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH 1 FROM c_ins;

CLOSE c_ins;

-- 2. BINARY cursor
DECLARE c_bin BINARY CURSOR FOR SELECT id FROM r15_cur ORDER BY id;

FETCH 1 FROM c_bin;

CLOSE c_bin;

ROLLBACK;

-- 3. WITH HOLD cursor outlives transaction
BEGIN;
DECLARE c_hold SCROLL CURSOR WITH HOLD FOR SELECT id FROM r15_cur ORDER BY id;
COMMIT;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH 1 FROM c_hold;

CLOSE c_hold;

-- ============================================================================
-- SECTION B: FETCH positioning
-- ============================================================================

BEGIN;
DECLARE c_b SCROLL CURSOR FOR SELECT id FROM r15_cur ORDER BY id;
MOVE 3 FROM c_b;

-- 4. BACKWARD 1 after MOVE 3 → row 2
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH BACKWARD 1 FROM c_b;

-- 5. Consecutive BACKWARD 1 → row 1
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH BACKWARD 1 FROM c_b;

CLOSE c_b;

-- 6. ABSOLUTE positioning
DECLARE c_abs SCROLL CURSOR FOR SELECT id FROM r15_cur ORDER BY id;
-- begin-expected
-- columns: id
-- row: 3
-- end-expected
FETCH ABSOLUTE 3 FROM c_abs;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
FETCH ABSOLUTE 1 FROM c_abs;

CLOSE c_abs;

-- 7. RELATIVE positioning
DECLARE c_rel SCROLL CURSOR FOR SELECT id FROM r15_cur ORDER BY id;
MOVE 2 FROM c_rel;
-- begin-expected
-- columns: id
-- row: 4
-- end-expected
FETCH RELATIVE 2 FROM c_rel;

CLOSE c_rel;

-- 8. PRIOR
DECLARE c_p SCROLL CURSOR FOR SELECT id FROM r15_cur ORDER BY id;
MOVE 3 FROM c_p;
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH PRIOR FROM c_p;

CLOSE c_p;

-- 9. FORWARD 0 returns current row
DECLARE c_z SCROLL CURSOR FOR SELECT id FROM r15_cur ORDER BY id;
MOVE 2 FROM c_z;
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
FETCH FORWARD 0 FROM c_z;

CLOSE c_z;
ROLLBACK;

-- ============================================================================
-- SECTION C: WHERE CURRENT OF
-- ============================================================================

BEGIN;
DECLARE c_upd CURSOR FOR SELECT id, label FROM r15_cur WHERE id=2 FOR UPDATE;
FETCH 1 FROM c_upd;
UPDATE r15_cur SET label='UPDATED' WHERE CURRENT OF c_upd;

-- 10. UPDATE WHERE CURRENT OF
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM r15_cur WHERE label='UPDATED';

CLOSE c_upd;
ROLLBACK;

BEGIN;
DECLARE c_del CURSOR FOR SELECT id FROM r15_cur WHERE id=3 FOR UPDATE;
FETCH 1 FROM c_del;
DELETE FROM r15_cur WHERE CURRENT OF c_del;

-- 11. DELETE WHERE CURRENT OF
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM r15_cur WHERE id=3;

CLOSE c_del;
ROLLBACK;

-- ============================================================================
-- SECTION D: pg_cursors view
-- ============================================================================

BEGIN;
DECLARE c_view CURSOR FOR SELECT 1;

-- 12. pg_cursors populated
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_cursors WHERE name = 'c_view';

-- 13. pg_cursors has standard columns
SELECT name, statement, is_holdable, is_binary, is_scrollable, creation_time
  FROM pg_cursors WHERE name='c_view';

CLOSE c_view;
ROLLBACK;

-- 14. CLOSE ALL
BEGIN;
DECLARE c_a CURSOR FOR SELECT 1;
DECLARE c_b CURSOR FOR SELECT 2;
CLOSE ALL;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_cursors;

ROLLBACK;

-- ============================================================================
-- SECTION E: PREPARE / EXECUTE / DEALLOCATE
-- ============================================================================

PREPARE r15_p1 (int) AS SELECT id, label FROM r15_cur WHERE id = $1;

-- 15. EXECUTE returns row
-- begin-expected
-- columns: id, label
-- row: 2, b
-- end-expected
EXECUTE r15_p1 (2);

-- 16. pg_prepared_statements populated
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_prepared_statements WHERE name='r15_p1';

DEALLOCATE r15_p1;

-- 17. DEALLOCATE removes
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM pg_prepared_statements WHERE name='r15_p1';

-- 18. pg_prepared_statements columns
PREPARE r15_p4 (int, text) AS SELECT $1, $2;

SELECT name, statement, prepare_time, parameter_types, from_sql, generic_plans, custom_plans
  FROM pg_prepared_statements WHERE name='r15_p4';

DEALLOCATE r15_p4;

-- 19. EXECUTE of nonexistent → error 26000
-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
EXECUTE r15_doesnotexist;
