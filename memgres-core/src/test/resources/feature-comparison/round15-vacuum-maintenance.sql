-- ============================================================================
-- Feature Comparison: Round 15 — VACUUM / ANALYZE / CLUSTER / REINDEX
-- Target: PostgreSQL 18 vs Memgres
--
-- In-memory engines can treat physical reclamation as a no-op, but the
-- *observable* surface — stats timestamps, pg_statistic rows, command tags,
-- in-txn error behavior, VERBOSE NOTICEs — must match PG.
-- ============================================================================

DROP SCHEMA IF EXISTS r15_vm CASCADE;
CREATE SCHEMA r15_vm;
SET search_path = r15_vm, public;

-- ============================================================================
-- SECTION A: VACUUM updates last_vacuum/last_analyze
-- ============================================================================

CREATE TABLE r15_v_last (id int);
INSERT INTO r15_v_last VALUES (1),(2),(3);
VACUUM r15_v_last;

-- 1. last_vacuum populated
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (last_vacuum IS NOT NULL)::text AS ok
  FROM pg_stat_user_tables WHERE relname='r15_v_last';

CREATE TABLE r15_va_last (id int, v text);
INSERT INTO r15_va_last VALUES (1,'a'),(2,'b');
VACUUM ANALYZE r15_va_last;

-- 2. last_analyze populated by VACUUM ANALYZE
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (last_analyze IS NOT NULL)::text AS ok
  FROM pg_stat_user_tables WHERE relname='r15_va_last';

CREATE TABLE r15_a_last (id int);
INSERT INTO r15_a_last VALUES (1);
ANALYZE r15_a_last;

-- 3. last_analyze populated by ANALYZE
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (last_analyze IS NOT NULL)::text AS ok
  FROM pg_stat_user_tables WHERE relname='r15_a_last';

-- ============================================================================
-- SECTION B: ANALYZE populates pg_stats
-- ============================================================================

CREATE TABLE r15_ps (id int, v text);
INSERT INTO r15_ps SELECT g, 'val'||(g%5)::text FROM generate_series(0,49) g;
ANALYZE r15_ps;

-- 4. pg_stats has rows for each column
SELECT count(*)::int AS c FROM pg_stats WHERE tablename='r15_ps';

-- 5. n_distinct populated
CREATE TABLE r15_nd (g int);
INSERT INTO r15_nd SELECT g%4 FROM generate_series(0,99) g;
ANALYZE r15_nd;

SELECT n_distinct FROM pg_stats WHERE tablename='r15_nd' AND attname='g';

-- 6. ANALYZE (a,c) → partial
CREATE TABLE r15_ac (a int, b int, c text);
INSERT INTO r15_ac VALUES (1,2,'x');
ANALYZE r15_ac (a, c);

SELECT count(*)::int AS c FROM pg_stats
  WHERE tablename='r15_ac' AND attname IN ('a','c');

-- ============================================================================
-- SECTION C: In-txn error (SQLSTATE 25001)
-- ============================================================================

CREATE TABLE r15_v_txn (id int);

BEGIN;
-- 7. VACUUM inside transaction block must error
-- begin-expected-error
-- sqlstate: 25001
-- end-expected-error
VACUUM r15_v_txn;
ROLLBACK;

CREATE TABLE r15_cl_txn (id int PRIMARY KEY);

BEGIN;
-- 8. CLUSTER inside transaction block must error
CLUSTER r15_cl_txn USING r15_cl_txn_pkey;
ROLLBACK;

-- ============================================================================
-- SECTION D: VACUUM options matrix
-- ============================================================================

CREATE TABLE r15_vopt (id int);

VACUUM (FULL) r15_vopt;
VACUUM (FREEZE) r15_vopt;
VACUUM (VERBOSE) r15_vopt;
VACUUM (ANALYZE) r15_vopt;
VACUUM (SKIP_LOCKED) r15_vopt;
VACUUM (DISABLE_PAGE_SKIPPING) r15_vopt;
VACUUM (INDEX_CLEANUP TRUE) r15_vopt;
VACUUM (TRUNCATE FALSE) r15_vopt;
VACUUM (PROCESS_TOAST) r15_vopt;
VACUUM (ANALYZE, VERBOSE) r15_vopt;

-- ============================================================================
-- SECTION E: REINDEX
-- ============================================================================

CREATE TABLE r15_ri (id int PRIMARY KEY, v text);
CREATE INDEX r15_ri_v_idx ON r15_ri (v);
INSERT INTO r15_ri VALUES (1,'a'),(2,'b');
REINDEX TABLE r15_ri;

-- 9. Table still queryable
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM r15_ri WHERE v='a';

CREATE TABLE r15_rii (id int PRIMARY KEY);
CREATE INDEX r15_rii_idx ON r15_rii (id);
REINDEX INDEX r15_rii_idx;

CREATE TABLE r15_ric (id int PRIMARY KEY);
CREATE INDEX r15_ric_idx ON r15_ric (id);
REINDEX INDEX CONCURRENTLY r15_ric_idx;

CREATE SCHEMA IF NOT EXISTS r15_rischema;
CREATE TABLE r15_rischema.t (id int PRIMARY KEY);
REINDEX SCHEMA r15_rischema;

-- ============================================================================
-- SECTION F: CLUSTER
-- ============================================================================

CREATE TABLE r15_cl (id int PRIMARY KEY, v text);
INSERT INTO r15_cl VALUES (1,'a'),(2,'b'),(3,'c');
CLUSTER r15_cl USING r15_cl_pkey;

-- 10. Rows preserved
-- begin-expected
-- columns: c
-- row: 3
-- end-expected
SELECT count(*)::int AS c FROM r15_cl;

CREATE TABLE r15_cl2 (id int PRIMARY KEY, v text);
CLUSTER r15_cl2 USING r15_cl2_pkey;

-- 11. pg_index.indisclustered flipped
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_index i
  JOIN pg_class c ON i.indexrelid = c.oid
  WHERE c.relname='r15_cl2_pkey' AND i.indisclustered;
