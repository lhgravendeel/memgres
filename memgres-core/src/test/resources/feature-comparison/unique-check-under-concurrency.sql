-- ============================================================================
-- Feature Comparison: uniqueness checking around in-flight writes
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- The cross-session part of this behaviour needs two connections and lives in
-- the unit test. What this file pins down is that moving the wait out of the
-- table lock left the ordinary, single-session rules exactly as they were:
-- duplicates still rejected, NULLs still distinct, and a rolled back insert
-- freeing the key it held.
-- ============================================================================

DROP TABLE IF EXISTS ucc_t CASCADE;
CREATE TABLE ucc_t (i int PRIMARY KEY, v int);
INSERT INTO ucc_t VALUES (1,10),(2,20);

-- ============================================================================
-- 1. Duplicates are still rejected
-- ============================================================================
INSERT INTO ucc_t VALUES (1,99);
SELECT count(*)::text AS a FROM ucc_t;
INSERT INTO ucc_t VALUES (3,30),(3,31);
SELECT count(*)::text AS a FROM ucc_t;
UPDATE ucc_t SET i = 2 WHERE i = 1;
SELECT string_agg(i::text, ',' ORDER BY i) AS a FROM ucc_t;

-- ============================================================================
-- 2. A rolled back insert frees its key again
-- ============================================================================
BEGIN;
INSERT INTO ucc_t VALUES (9,90);
SELECT count(*)::text AS a FROM ucc_t WHERE i = 9;
ROLLBACK;
SELECT count(*)::text AS a FROM ucc_t WHERE i = 9;
INSERT INTO ucc_t VALUES (9,91);
SELECT v::text AS a FROM ucc_t WHERE i = 9;
DELETE FROM ucc_t WHERE i = 9;

-- the same through a savepoint
BEGIN;
INSERT INTO ucc_t VALUES (11,110);
SAVEPOINT sp1;
INSERT INTO ucc_t VALUES (12,120);
ROLLBACK TO SAVEPOINT sp1;
INSERT INTO ucc_t VALUES (12,121);
COMMIT;
SELECT string_agg(v::text, ',' ORDER BY v) AS a FROM ucc_t WHERE i IN (11,12);
DELETE FROM ucc_t WHERE i IN (11,12);

-- ============================================================================
-- 3. NULLs remain distinct unless the constraint says otherwise
-- ============================================================================
DROP TABLE IF EXISTS ucc_n CASCADE;
CREATE TABLE ucc_n (i int, k int UNIQUE);
INSERT INTO ucc_n VALUES (1,NULL),(2,NULL),(3,5);
SELECT count(*)::text AS a FROM ucc_n;
INSERT INTO ucc_n VALUES (4,5);
DROP TABLE IF EXISTS ucc_nd CASCADE;
CREATE TABLE ucc_nd (i int, k int UNIQUE NULLS NOT DISTINCT);
INSERT INTO ucc_nd VALUES (1,NULL);
INSERT INTO ucc_nd VALUES (2,NULL);
SELECT count(*)::text AS a FROM ucc_nd;

-- ============================================================================
-- 4. Multi-column, partial and expression uniqueness are unchanged
-- ============================================================================
DROP TABLE IF EXISTS ucc_m CASCADE;
CREATE TABLE ucc_m (a int, b int, UNIQUE (a, b));
INSERT INTO ucc_m VALUES (1,1),(1,2),(2,1);
SELECT count(*)::text AS a FROM ucc_m;
INSERT INTO ucc_m VALUES (1,1);
DROP TABLE IF EXISTS ucc_p CASCADE;
CREATE TABLE ucc_p (i int, k int);
CREATE UNIQUE INDEX ucc_p_ix ON ucc_p (k) WHERE i > 0;
INSERT INTO ucc_p VALUES (1,5);
INSERT INTO ucc_p VALUES (-1,5);
INSERT INTO ucc_p VALUES (2,5);
SELECT count(*)::text AS a FROM ucc_p;
DROP TABLE IF EXISTS ucc_e CASCADE;
CREATE TABLE ucc_e (t text);
CREATE UNIQUE INDEX ucc_e_ix ON ucc_e (lower(t));
INSERT INTO ucc_e VALUES ('Abc');
INSERT INTO ucc_e VALUES ('aBC');
SELECT count(*)::text AS a FROM ucc_e;

-- ============================================================================
-- 5. ON CONFLICT still resolves against committed rows
-- ============================================================================
INSERT INTO ucc_t VALUES (1,111) ON CONFLICT (i) DO UPDATE SET v = EXCLUDED.v;
SELECT v::text AS a FROM ucc_t WHERE i = 1;
INSERT INTO ucc_t VALUES (1,222) ON CONFLICT (i) DO NOTHING;
SELECT v::text AS a FROM ucc_t WHERE i = 1;
INSERT INTO ucc_t VALUES (50,500) ON CONFLICT (i) DO NOTHING;
SELECT count(*)::text AS a FROM ucc_t WHERE i = 50;

-- ============================================================================
-- 6. Uniqueness on a partitioned table
-- ============================================================================
DROP TABLE IF EXISTS ucc_pt CASCADE;
CREATE TABLE ucc_pt (i int, v int, PRIMARY KEY (i)) PARTITION BY RANGE (i);
CREATE TABLE ucc_pt_a PARTITION OF ucc_pt FOR VALUES FROM (1) TO (10);
CREATE TABLE ucc_pt_b PARTITION OF ucc_pt FOR VALUES FROM (10) TO (20);
INSERT INTO ucc_pt VALUES (1,1),(11,11);
INSERT INTO ucc_pt VALUES (1,2);
SELECT count(*)::text AS a FROM ucc_pt;

DROP TABLE IF EXISTS ucc_pt CASCADE;
DROP TABLE IF EXISTS ucc_e CASCADE;
DROP TABLE IF EXISTS ucc_p CASCADE;
DROP TABLE IF EXISTS ucc_m CASCADE;
DROP TABLE IF EXISTS ucc_nd CASCADE;
DROP TABLE IF EXISTS ucc_n CASCADE;
DROP TABLE IF EXISTS ucc_t CASCADE;
