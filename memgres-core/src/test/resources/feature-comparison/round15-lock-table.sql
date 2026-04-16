-- ============================================================================
-- Feature Comparison: Round 15 — LOCK TABLE + advisory locks surface
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r15_lock CASCADE;
CREATE SCHEMA r15_lock;
SET search_path = r15_lock, public;

-- ============================================================================
-- SECTION A: Basic LOCK TABLE
-- ============================================================================

CREATE TABLE r15_lt_def (id int);

BEGIN;
-- 1. LOCK TABLE with no mode (default = ACCESS EXCLUSIVE)
LOCK TABLE r15_lt_def;
ROLLBACK;

CREATE TABLE r15_lt_modes (id int);

-- 2. LOCK TABLE IN ACCESS SHARE MODE
BEGIN; LOCK TABLE r15_lt_modes IN ACCESS SHARE MODE; ROLLBACK;

-- 3. LOCK TABLE IN ROW SHARE MODE
BEGIN; LOCK TABLE r15_lt_modes IN ROW SHARE MODE; ROLLBACK;

-- 4. LOCK TABLE IN ROW EXCLUSIVE MODE
BEGIN; LOCK TABLE r15_lt_modes IN ROW EXCLUSIVE MODE; ROLLBACK;

-- 5. LOCK TABLE IN SHARE UPDATE EXCLUSIVE MODE
BEGIN; LOCK TABLE r15_lt_modes IN SHARE UPDATE EXCLUSIVE MODE; ROLLBACK;

-- 6. LOCK TABLE IN SHARE MODE
BEGIN; LOCK TABLE r15_lt_modes IN SHARE MODE; ROLLBACK;

-- 7. LOCK TABLE IN SHARE ROW EXCLUSIVE MODE
BEGIN; LOCK TABLE r15_lt_modes IN SHARE ROW EXCLUSIVE MODE; ROLLBACK;

-- 8. LOCK TABLE IN EXCLUSIVE MODE
BEGIN; LOCK TABLE r15_lt_modes IN EXCLUSIVE MODE; ROLLBACK;

-- 9. LOCK TABLE IN ACCESS EXCLUSIVE MODE
BEGIN; LOCK TABLE r15_lt_modes IN ACCESS EXCLUSIVE MODE; ROLLBACK;

-- ============================================================================
-- SECTION B: LOCK TABLE ONLY
-- ============================================================================

CREATE TABLE r15_lt_only_p (a int);
CREATE TABLE r15_lt_only_c () INHERITS (r15_lt_only_p);

-- 10. LOCK TABLE ONLY — does not lock child tables
BEGIN;
LOCK TABLE ONLY r15_lt_only_p;
ROLLBACK;

-- ============================================================================
-- SECTION C: LOCK TABLE multi-table
-- ============================================================================

CREATE TABLE r15_lt_m1 (id int);
CREATE TABLE r15_lt_m2 (id int);
CREATE TABLE r15_lt_m3 (id int);

-- 11. Multiple tables in one LOCK statement
BEGIN;
LOCK TABLE r15_lt_m1, r15_lt_m2, r15_lt_m3 IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

-- ============================================================================
-- SECTION D: LOCK TABLE NOWAIT
-- ============================================================================

CREATE TABLE r15_lt_nw (id int);

-- 12. NOWAIT — succeeds with no contention
BEGIN;
LOCK TABLE r15_lt_nw IN ACCESS EXCLUSIVE MODE NOWAIT;
ROLLBACK;

-- ============================================================================
-- SECTION E: pg_locks view for LOCK TABLE
-- ============================================================================

CREATE TABLE r15_lt_pl (id int);

-- 13. pg_locks shows AccessExclusiveLock after LOCK TABLE
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
BEGIN;
LOCK TABLE r15_lt_pl IN ACCESS EXCLUSIVE MODE;
SELECT (count(*) >= 1)::int AS n
FROM pg_locks l
JOIN pg_class c ON l.relation = c.oid
WHERE c.relname = 'r15_lt_pl' AND l.mode = 'AccessExclusiveLock';
ROLLBACK;

-- ============================================================================
-- SECTION F: Advisory locks — pg_locks rows for locktype='advisory'
-- ============================================================================

-- 14. pg_advisory_lock(id) appears in pg_locks
SELECT pg_advisory_lock(123456);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT (count(*) >= 1)::int AS n
FROM pg_locks
WHERE locktype = 'advisory' AND objid = 123456;

SELECT pg_advisory_unlock(123456);

-- 15. pg_advisory_xact_lock registers in pg_locks and is released at COMMIT
BEGIN;
SELECT pg_advisory_xact_lock(999001);
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT (count(*) >= 1)::int AS n
FROM pg_locks
WHERE locktype = 'advisory' AND objid = 999001;
COMMIT;

-- After commit, no advisory lock row remains
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c
FROM pg_locks
WHERE locktype = 'advisory' AND objid = 999001;

-- 16. pg_try_advisory_lock returns true when free
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT pg_try_advisory_lock(555777) AS ok;

SELECT pg_advisory_unlock(555777);

-- ============================================================================
-- SECTION G: pg_locks view shape
-- ============================================================================

-- 17. pg_locks has PG 18 columns (shape only)
SELECT locktype, database, relation, page, tuple, virtualxid,
       transactionid, classid, objid, objsubid, virtualtransaction,
       pid, mode, granted, fastpath, waitstart
FROM pg_locks
LIMIT 1;
