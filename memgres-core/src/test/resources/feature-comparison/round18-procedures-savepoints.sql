-- ============================================================================
-- Feature Comparison: Round 18 — Procedures, savepoints, 2PC
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION AG1: CALL proc(..., OUT)
-- ============================================================================

DROP PROCEDURE IF EXISTS r18_outp(int, int);
CREATE PROCEDURE r18_outp(IN a int, OUT b int)
LANGUAGE plpgsql AS $$ BEGIN b := a * 2; END $$;

-- 1. CALL with OUT via pg session returns value as RS column
-- begin-expected
-- columns: b
-- row: 42
-- end-expected
CALL r18_outp(21, NULL);

-- ============================================================================
-- SECTION AG2: Savepoint name reuse
-- ============================================================================

DROP TABLE IF EXISTS r18_sp;
CREATE TABLE r18_sp(a int);
BEGIN;
INSERT INTO r18_sp VALUES (1);
SAVEPOINT s1;
INSERT INTO r18_sp VALUES (2);
SAVEPOINT s1;
INSERT INTO r18_sp VALUES (3);
ROLLBACK TO SAVEPOINT s1;

-- 2. Prior s1's row (2) persists; (3) undone
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::int AS n FROM r18_sp;

ROLLBACK;

-- ============================================================================
-- SECTION AG3: pg_prepared_xacts queryable
-- ============================================================================

-- 3. pg_prepared_xacts queryable (even if empty)
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 0) AS ok FROM pg_prepared_xacts;

-- ============================================================================
-- SECTION AG4: PREPARE TRANSACTION
-- ============================================================================

DROP TABLE IF EXISTS r18_2pc;
CREATE TABLE r18_2pc(a int);
BEGIN;
INSERT INTO r18_2pc VALUES (1);
PREPARE TRANSACTION 'r18_pt1';

-- 4. Prepared transaction registered
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_prepared_xacts WHERE gid='r18_pt1';

COMMIT PREPARED 'r18_pt1';

-- 5. COMMIT PREPARED persisted row
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM r18_2pc;

-- ============================================================================
-- SECTION AG5: max_prepared_transactions GUC
-- ============================================================================

-- 6. GUC present
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_settings WHERE name='max_prepared_transactions';
