-- ============================================================================
-- Feature Comparison: COMMIT in aborted (FAILED) transaction
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL behavior: when an error occurs inside an explicit transaction,
-- the transaction enters an "aborted" state. Any subsequent COMMIT is treated
-- as ROLLBACK — all changes since BEGIN are discarded.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS atc_main CASCADE;
CREATE TABLE atc_main (id int PRIMARY KEY, val text);
INSERT INTO atc_main VALUES (1, 'original');

-- ============================================================================
-- 1. INSERT rolled back after division-by-zero error + COMMIT
-- ============================================================================

BEGIN;
INSERT INTO atc_main VALUES (2, 'should_vanish');

-- begin-expected-error
-- message-like: division by zero
-- end-expected-error
SELECT 1/0;

-- note: COMMIT in aborted transaction should act as ROLLBACK
COMMIT;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM atc_main;

-- ============================================================================
-- 2. UPDATE rolled back after error + COMMIT
-- ============================================================================

BEGIN;
UPDATE atc_main SET val = 'modified' WHERE id = 1;

-- begin-expected-error
-- message-like: division by zero
-- end-expected-error
SELECT 1/0;

COMMIT;

-- begin-expected
-- columns: val
-- row: original
-- end-expected
SELECT val FROM atc_main WHERE id = 1;

-- ============================================================================
-- 3. DELETE rolled back after error + COMMIT
-- ============================================================================

INSERT INTO atc_main VALUES (10, 'ten'), (20, 'twenty');

BEGIN;
DELETE FROM atc_main WHERE id = 10;

-- begin-expected-error
-- message-like: division by zero
-- end-expected-error
SELECT 1/0;

COMMIT;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::text AS cnt FROM atc_main;

-- ============================================================================
-- 4. Multiple inserts before error — all rolled back
-- ============================================================================

BEGIN;
INSERT INTO atc_main VALUES (100, 'a');
INSERT INTO atc_main VALUES (101, 'b');
INSERT INTO atc_main VALUES (102, 'c');

-- begin-expected-error
-- message-like: division by zero
-- end-expected-error
SELECT 1/0;

COMMIT;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::text AS cnt FROM atc_main;

-- ============================================================================
-- 5. Constraint violation aborts transaction — COMMIT acts as ROLLBACK
-- ============================================================================

BEGIN;
INSERT INTO atc_main VALUES (200, 'new_row');

-- begin-expected-error
-- message-like: duplicate key
-- end-expected-error
INSERT INTO atc_main VALUES (1, 'duplicate_pk');

COMMIT;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::text AS cnt FROM atc_main;

-- ============================================================================
-- 6. DDL rolled back after error + COMMIT
-- ============================================================================

BEGIN;
CREATE TABLE atc_temp_table (id int);

-- begin-expected-error
-- message-like: division by zero
-- end-expected-error
SELECT 1/0;

COMMIT;

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT 1 FROM atc_temp_table LIMIT 1;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS atc_main CASCADE;
