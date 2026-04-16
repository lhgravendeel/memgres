-- ============================================================================
-- Feature Comparison: Round 13 — Transaction / MVCC Gaps
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Single-session transaction behaviors that PG 18 guarantees.
-- (Multi-session / concurrency tests live in the Java tests, since this
-- harness is single-session.)
-- ============================================================================

DROP SCHEMA IF EXISTS r13_txn CASCADE;
CREATE SCHEMA r13_txn;
SET search_path = r13_txn, public;

-- ============================================================================
-- SECTION A: 2PC — unknown prepared txn → 42704
-- ============================================================================

-- 1. COMMIT PREPARED unknown GID
-- begin-expected-error
-- message-like: prepared transaction
-- end-expected-error
COMMIT PREPARED 'does-not-exist-r13';

-- 2. ROLLBACK PREPARED unknown GID
-- begin-expected-error
-- message-like: prepared transaction
-- end-expected-error
ROLLBACK PREPARED 'nope-r13';

-- ============================================================================
-- SECTION B: Deferred foreign key
-- ============================================================================

CREATE TABLE r13_def_parent (id int PRIMARY KEY);
CREATE TABLE r13_def_child (
  pid int REFERENCES r13_def_parent(id) DEFERRABLE INITIALLY DEFERRED
);

-- 3. Deferred FK validated at commit — success path
BEGIN;
INSERT INTO r13_def_child VALUES (99);  -- deferred, not yet checked
INSERT INTO r13_def_parent VALUES (99); -- fixes it
COMMIT;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM r13_def_child;

-- 4. Deferred FK violation surfaced at commit
CREATE TABLE r13_def_child2 (
  pid int REFERENCES r13_def_parent(id) DEFERRABLE INITIALLY DEFERRED
);

BEGIN;
INSERT INTO r13_def_child2 VALUES (999);

-- begin-expected-error
-- message-like: violates foreign key
-- end-expected-error
COMMIT;

-- ============================================================================
-- SECTION C: txid_* family
-- ============================================================================

-- 5. txid_current stable within txn
BEGIN;

-- begin-expected
-- columns: same
-- row: t
-- end-expected
SELECT (txid_current() = txid_current())::text AS same;

ROLLBACK;

-- 6. txid_status returns a well-formed label
-- begin-expected
-- columns: s
-- row: in progress
-- end-expected
SELECT txid_status(txid_current()) AS s;

-- ============================================================================
-- SECTION D: pg_export_snapshot real value
-- ============================================================================

-- 7. Exported snapshot ID is a hex-like string
BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (pg_export_snapshot() ~ '^[0-9A-Fa-f-]+$')::text AS ok;

ROLLBACK;

-- ============================================================================
-- SECTION E: VACUUM / ANALYZE statistics
-- ============================================================================

CREATE TABLE r13_ana (id int, v text);
INSERT INTO r13_ana SELECT i, 'x'||i FROM generate_series(1, 100) i;

-- 8. ANALYZE populates pg_stats
ANALYZE r13_ana;

-- begin-expected
-- columns: has_stats
-- row: t
-- end-expected
SELECT (count(*) > 0)::text AS has_stats FROM pg_stats WHERE tablename = 'r13_ana';

-- 9. VACUUM ANALYZE updates pg_class.reltuples
CREATE TABLE r13_vac (id int);
INSERT INTO r13_vac SELECT i FROM generate_series(1, 500) i;
VACUUM ANALYZE r13_vac;

-- begin-expected
-- columns: reltuples_positive
-- row: t
-- end-expected
SELECT (reltuples > 0)::text AS reltuples_positive
  FROM pg_class WHERE relname = 'r13_vac';

-- ============================================================================
-- SECTION F: CLUSTER
-- ============================================================================

CREATE TABLE r13_cluster (id int PRIMARY KEY, v text);
INSERT INTO r13_cluster VALUES (3,'c'),(1,'a'),(2,'b');
CLUSTER r13_cluster USING r13_cluster_pkey;

-- begin-expected
-- columns: c
-- row: 3
-- end-expected
SELECT count(*)::text AS c FROM r13_cluster;

-- ============================================================================
-- SECTION G: SHARED advisory locks
-- ============================================================================

-- 10. Shared advisory lock
-- begin-expected
-- columns: got
-- row: t
-- end-expected
SELECT pg_try_advisory_lock_shared(49200)::text AS got;

-- 11. Release shared advisory lock
-- begin-expected
-- columns: released
-- row: t
-- end-expected
SELECT pg_advisory_unlock_shared(49200)::text AS released;

-- ============================================================================
-- SECTION H: READ UNCOMMITTED mapped to READ COMMITTED (PG semantics)
-- ============================================================================

-- 12. READ UNCOMMITTED is accepted but mapped
-- begin-expected
-- columns: status
-- row: SET
-- end-expected
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

RESET ALL;
