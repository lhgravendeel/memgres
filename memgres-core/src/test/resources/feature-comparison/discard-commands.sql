-- ============================================================================
-- Feature Comparison: DISCARD Commands
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   → expected result set
--   -- begin-expected-error / message-like: / end-expected-error → expected error
--   -- note: ...                                          → informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DEALLOCATE ALL;

DROP TABLE IF EXISTS disc_test CASCADE;
CREATE TABLE disc_test (id integer PRIMARY KEY, name text);
INSERT INTO disc_test VALUES (1, 'alpha'), (2, 'beta');

-- ============================================================================
-- 1. DISCARD ALL: removes prepared statements
-- ============================================================================

PREPARE disc_ps1 AS SELECT 1;
PREPARE disc_ps2 AS SELECT 2;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*)::integer AS count FROM pg_prepared_statements;

DISCARD ALL;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::integer AS count FROM pg_prepared_statements;

-- ============================================================================
-- 2. DISCARD ALL: removes cursors
-- ============================================================================

BEGIN;
DECLARE disc_cur CURSOR WITH HOLD FOR SELECT 1;
COMMIT;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors;

DISCARD ALL;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors;

-- ============================================================================
-- 3. DISCARD ALL: resets GUC settings
-- ============================================================================

SET statement_timeout = '5000';

-- begin-expected
-- columns: statement_timeout
-- row: 5000
-- end-expected
SHOW statement_timeout;

DISCARD ALL;

-- note: After DISCARD ALL, GUC settings should be back to defaults
-- begin-expected
-- columns: statement_timeout
-- row: 0
-- end-expected
SHOW statement_timeout;

-- ============================================================================
-- 4. DISCARD ALL: drops temp tables
-- ============================================================================

CREATE TEMP TABLE disc_temp (val integer);
INSERT INTO disc_temp VALUES (42);

-- begin-expected
-- columns: val
-- row: 42
-- end-expected
SELECT val FROM disc_temp;

DISCARD ALL;

-- begin-expected-error
-- message-like: relation "disc_temp" does not exist
-- end-expected-error
SELECT val FROM disc_temp;

-- ============================================================================
-- 5. DISCARD ALL: combined effect (everything at once)
-- ============================================================================

-- Set up multiple session state items
PREPARE disc_combo AS SELECT 'prepared' AS kind;
SET work_mem = '64MB';
CREATE TEMP TABLE disc_combo_temp (x integer);

BEGIN;
DECLARE disc_combo_cur CURSOR WITH HOLD FOR SELECT 'cursor' AS kind;
COMMIT;

-- Verify everything exists
-- begin-expected
-- columns: ps_count
-- row: 1
-- end-expected
SELECT count(*)::integer AS ps_count FROM pg_prepared_statements;

-- begin-expected
-- columns: cur_count
-- row: 1
-- end-expected
SELECT count(*)::integer AS cur_count FROM pg_cursors;

-- One DISCARD ALL clears everything
DISCARD ALL;

-- begin-expected
-- columns: ps_count
-- row: 0
-- end-expected
SELECT count(*)::integer AS ps_count FROM pg_prepared_statements;

-- begin-expected
-- columns: cur_count
-- row: 0
-- end-expected
SELECT count(*)::integer AS cur_count FROM pg_cursors;

-- begin-expected-error
-- message-like: relation "disc_combo_temp" does not exist
-- end-expected-error
SELECT * FROM disc_combo_temp;

-- ============================================================================
-- 6. DISCARD PLANS: does NOT remove prepared statements
-- ============================================================================

PREPARE disc_plan1 AS SELECT 'alive' AS status;
PREPARE disc_plan2 AS SELECT 'also alive' AS status;

DISCARD PLANS;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*)::integer AS count FROM pg_prepared_statements;

-- begin-expected
-- columns: status
-- row: alive
-- end-expected
EXECUTE disc_plan1;

-- begin-expected
-- columns: status
-- row: also alive
-- end-expected
EXECUTE disc_plan2;

DEALLOCATE ALL;

-- ============================================================================
-- 7. DISCARD PLANS: does not affect cursors
-- ============================================================================

BEGIN;
DECLARE disc_plan_cur CURSOR WITH HOLD FOR SELECT 1 AS val;
COMMIT;

DISCARD PLANS;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors;

-- begin-expected
-- columns: val
-- row: 1
-- end-expected
FETCH NEXT FROM disc_plan_cur;

CLOSE disc_plan_cur;

-- ============================================================================
-- 8. DISCARD TEMP: drops temp tables only
-- ============================================================================

CREATE TEMP TABLE disc_temp2 (val integer);
INSERT INTO disc_temp2 VALUES (99);
PREPARE disc_temp_ps AS SELECT 'still here' AS status;

DISCARD TEMP;

-- Temp table gone
-- begin-expected-error
-- message-like: relation "disc_temp2" does not exist
-- end-expected-error
SELECT val FROM disc_temp2;

-- Prepared statement still alive
-- begin-expected
-- columns: status
-- row: still here
-- end-expected
EXECUTE disc_temp_ps;

DEALLOCATE ALL;

-- ============================================================================
-- 9. DISCARD SEQUENCES: no visible effect on prepared stmts or cursors
-- ============================================================================

-- note: DISCARD SEQUENCES resets cached sequence state.
--       It should not affect prepared statements, cursors, or temp tables.

PREPARE disc_seq_ps AS SELECT 'intact' AS val;

DISCARD SEQUENCES;

-- begin-expected
-- columns: val
-- row: intact
-- end-expected
EXECUTE disc_seq_ps;

DEALLOCATE ALL;

-- ============================================================================
-- 10. DISCARD ALL: LISTEN/NOTIFY cleanup
-- ============================================================================

-- note: DISCARD ALL includes UNLISTEN * (cancel all notification subscriptions)
LISTEN test_channel;

-- After DISCARD ALL, the subscription should be gone
DISCARD ALL;

-- note: There's no direct way to query active listeners in standard SQL,
--       but the command should not error
-- Re-listen should work fine (no "already listening" issue)
LISTEN test_channel;
UNLISTEN test_channel;

-- ============================================================================
-- 11. Multiple DISCARD ALL calls (idempotent)
-- ============================================================================

DISCARD ALL;
DISCARD ALL;
DISCARD ALL;

-- Should not error — DISCARD ALL is idempotent
-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 12. DISCARD inside transaction
-- ============================================================================

-- note: In PG, DISCARD ALL cannot run inside a transaction block
BEGIN;

-- begin-expected-error
-- message-like: DISCARD ALL cannot run inside a transaction block
-- end-expected-error
DISCARD ALL;

ROLLBACK;

-- DISCARD PLANS can run inside transaction
BEGIN;
PREPARE disc_txn_ps AS SELECT 1;
DISCARD PLANS;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count FROM pg_prepared_statements WHERE name = 'disc_txn_ps';

COMMIT;

DEALLOCATE ALL;

-- ============================================================================
-- Cleanup
-- ============================================================================

DEALLOCATE ALL;
DROP TABLE IF EXISTS disc_test CASCADE;
