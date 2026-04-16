-- ============================================================================
-- Feature Comparison: Round 14 — Row-level locking variants
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Single-session syntactic checks; concurrency tests live in the Java suite.
-- ============================================================================

DROP SCHEMA IF EXISTS r14_lk CASCADE;
CREATE SCHEMA r14_lk;
SET search_path = r14_lk, public;

CREATE TABLE r14_rows (id int PRIMARY KEY, v int);
INSERT INTO r14_rows VALUES (1,10),(2,20),(3,30),(4,40),(5,50);

-- ============================================================================
-- SECTION A: lock strength keywords
-- ============================================================================

-- 1. FOR NO KEY UPDATE
BEGIN;
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM r14_rows WHERE id = 1 FOR NO KEY UPDATE;
ROLLBACK;

-- 2. FOR KEY SHARE
BEGIN;
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM r14_rows WHERE id = 1 FOR KEY SHARE;
ROLLBACK;

-- 3. FOR SHARE
BEGIN;
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM r14_rows WHERE id = 1 FOR SHARE;
ROLLBACK;

-- ============================================================================
-- SECTION B: FOR UPDATE OF <table_list>
-- ============================================================================

CREATE TABLE r14_rows_b (id int);
INSERT INTO r14_rows_b VALUES (1);

-- 4. FOR UPDATE OF only locks rows of the named table
BEGIN;
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT a.id FROM r14_rows a JOIN r14_rows_b b ON a.id = b.id
  WHERE a.id = 1 FOR UPDATE OF a;
ROLLBACK;

-- ============================================================================
-- SECTION C: SKIP LOCKED + ORDER BY + LIMIT (worker-queue pattern)
-- ============================================================================

-- 5. Worker-queue pattern
BEGIN;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM r14_rows ORDER BY id LIMIT 2 FOR UPDATE SKIP LOCKED;
ROLLBACK;

-- ============================================================================
-- SECTION D: Lock-mode upgrade SHARE → EXCLUSIVE within same txn
-- ============================================================================

-- 6. Upgrade in place
BEGIN;
SELECT id FROM r14_rows WHERE id = 1 FOR SHARE;
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM r14_rows WHERE id = 1 FOR UPDATE;
ROLLBACK;

-- ============================================================================
-- SECTION E: Delete+USING+SKIP LOCKED
-- ============================================================================

BEGIN;
DELETE FROM r14_rows WHERE id IN (
  SELECT id FROM r14_rows WHERE v >= 20 ORDER BY id LIMIT 2 FOR UPDATE SKIP LOCKED
);
-- 7. 2 rows removed, 3 remain
-- begin-expected
-- columns: c
-- row: 3
-- end-expected
SELECT count(*)::text AS c FROM r14_rows;
ROLLBACK;
