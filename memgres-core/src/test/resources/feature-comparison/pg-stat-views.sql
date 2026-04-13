-- ============================================================================
-- Feature Comparison: pg_stat_* Views & pg_am (B1, B6)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- pg_stat_* views provide runtime statistics. In Memgres (in-memory DB),
-- most are empty stubs. This file verifies:
--   1. Views exist with correct column schemas
--   2. pg_stat_activity is at least partially populated
--   3. pg_am has correct access method entries
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS psv_test CASCADE;
CREATE SCHEMA psv_test;
SET search_path = psv_test, public;

-- ============================================================================
-- 1. pg_stat_user_tables exists with expected columns
-- ============================================================================

-- note: View should exist and have standard columns even if empty
-- begin-expected
-- columns: has_relid, has_schemaname, has_relname, has_seq_scan, has_idx_scan, has_n_tup_ins, has_n_tup_upd, has_n_tup_del
-- row: true, true, true, true, true, true, true, true
-- end-expected
SELECT
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'relid') AS has_relid,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'schemaname') AS has_schemaname,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'relname') AS has_relname,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'seq_scan') AS has_seq_scan,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'idx_scan') AS has_idx_scan,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'n_tup_ins') AS has_n_tup_ins,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'n_tup_upd') AS has_n_tup_upd,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'n_tup_del') AS has_n_tup_del;

-- ============================================================================
-- 2. pg_stat_all_tables exists and is queryable
-- ============================================================================

-- note: Should include system tables at minimum
-- begin-expected
-- columns: queryable
-- row: true
-- end-expected
SELECT count(*) >= 0 AS queryable FROM pg_stat_all_tables;

-- ============================================================================
-- 3. pg_stat_user_indexes exists with expected columns
-- ============================================================================

-- begin-expected
-- columns: has_relid, has_indexrelid, has_idx_scan, has_idx_tup_read
-- row: true, true, true, true
-- end-expected
SELECT
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_indexes' AND column_name = 'relid') AS has_relid,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_indexes' AND column_name = 'indexrelid') AS has_indexrelid,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_indexes' AND column_name = 'idx_scan') AS has_idx_scan,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_indexes' AND column_name = 'idx_tup_read') AS has_idx_tup_read;

-- ============================================================================
-- 4. pg_stat_all_indexes is queryable
-- ============================================================================

-- begin-expected
-- columns: queryable
-- row: true
-- end-expected
SELECT count(*) >= 0 AS queryable FROM pg_stat_all_indexes;

-- ============================================================================
-- 5. pg_stat_database exists with expected columns
-- ============================================================================

-- begin-expected
-- columns: has_datid, has_datname, has_numbackends, has_xact_commit, has_xact_rollback
-- row: true, true, true, true, true
-- end-expected
SELECT
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_database' AND column_name = 'datid') AS has_datid,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_database' AND column_name = 'datname') AS has_datname,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_database' AND column_name = 'numbackends') AS has_numbackends,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_database' AND column_name = 'xact_commit') AS has_xact_commit,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_database' AND column_name = 'xact_rollback') AS has_xact_rollback;

-- ============================================================================
-- 6. pg_stat_database returns at least one row
-- ============================================================================

-- begin-expected
-- columns: has_rows
-- row: true
-- end-expected
SELECT count(*) > 0 AS has_rows FROM pg_stat_database;

-- ============================================================================
-- 7. pg_stat_bgwriter exists and is queryable
-- ============================================================================

-- begin-expected
-- columns: queryable
-- row: true
-- end-expected
SELECT count(*) >= 0 AS queryable FROM pg_stat_bgwriter;

-- ============================================================================
-- 8. pg_stat_wal exists and is queryable
-- ============================================================================

-- note: PG 14+ view; may not exist in all versions
-- begin-expected
-- columns: queryable
-- row: true
-- end-expected
SELECT count(*) >= 0 AS queryable FROM pg_stat_wal;

-- ============================================================================
-- 9. pg_statio_user_tables exists and is queryable
-- ============================================================================

-- begin-expected
-- columns: queryable
-- row: true
-- end-expected
SELECT count(*) >= 0 AS queryable FROM pg_statio_user_tables;

-- ============================================================================
-- 10. pg_stat_activity is partially populated
-- ============================================================================

-- note: Should show at least the current session
-- begin-expected
-- columns: has_current
-- row: true
-- end-expected
SELECT count(*) > 0 AS has_current FROM pg_stat_activity WHERE pid = pg_backend_pid();

-- ============================================================================
-- 11. pg_stat_activity has expected columns
-- ============================================================================

-- begin-expected
-- columns: has_datid, has_pid, has_usename, has_state, has_query, has_backend_start
-- row: true, true, true, true, true, true
-- end-expected
SELECT
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_activity' AND column_name = 'datid') AS has_datid,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_activity' AND column_name = 'pid') AS has_pid,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_activity' AND column_name = 'usename') AS has_usename,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_activity' AND column_name = 'state') AS has_state,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_activity' AND column_name = 'query') AS has_query,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_activity' AND column_name = 'backend_start') AS has_backend_start;

-- ============================================================================
-- 12. pg_stat_activity current session state
-- ============================================================================

-- begin-expected
-- columns: state
-- row: active
-- end-expected
SELECT state FROM pg_stat_activity WHERE pid = pg_backend_pid();

-- ============================================================================
-- 13. pg_stat_replication exists (even if empty)
-- ============================================================================

-- begin-expected
-- columns: queryable
-- row: true
-- end-expected
SELECT count(*) >= 0 AS queryable FROM pg_stat_replication;

-- ============================================================================
-- 14. pg_am has expected access methods
-- ============================================================================

-- begin-expected
-- columns: has_btree, has_hash, has_gist, has_gin, has_spgist, has_brin
-- row: true, true, true, true, true, true
-- end-expected
SELECT
  (SELECT count(*) > 0 FROM pg_am WHERE amname = 'btree') AS has_btree,
  (SELECT count(*) > 0 FROM pg_am WHERE amname = 'hash') AS has_hash,
  (SELECT count(*) > 0 FROM pg_am WHERE amname = 'gist') AS has_gist,
  (SELECT count(*) > 0 FROM pg_am WHERE amname = 'gin') AS has_gin,
  (SELECT count(*) > 0 FROM pg_am WHERE amname = 'spgist') AS has_spgist,
  (SELECT count(*) > 0 FROM pg_am WHERE amname = 'brin') AS has_brin;

-- ============================================================================
-- 15. pg_am column schema
-- ============================================================================

-- begin-expected
-- columns: has_oid, has_amname, has_amhandler, has_amtype
-- row: true, true, true, true
-- end-expected
SELECT
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_am' AND column_name = 'oid') AS has_oid,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_am' AND column_name = 'amname') AS has_amname,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_am' AND column_name = 'amhandler') AS has_amhandler,
  (SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_am' AND column_name = 'amtype') AS has_amtype;

-- ============================================================================
-- 16. pg_am amtype values (i=index, t=table)
-- ============================================================================

-- begin-expected
-- columns: amtype
-- row: i
-- end-expected
SELECT amtype FROM pg_am WHERE amname = 'btree';

-- ============================================================================
-- 17. pg_stat_user_tables shows data after DML
-- ============================================================================

CREATE TABLE psv_dml_test (id integer PRIMARY KEY, val text);
INSERT INTO psv_dml_test VALUES (1, 'a'), (2, 'b'), (3, 'c');
UPDATE psv_dml_test SET val = 'updated' WHERE id = 1;
DELETE FROM psv_dml_test WHERE id = 3;
SELECT * FROM psv_dml_test;

-- note: After DML, pg_stat_user_tables may show this table
-- The exact counts depend on implementation (may be zero in Memgres)
-- begin-expected
-- columns: queryable
-- row: true
-- end-expected
SELECT count(*) >= 0 AS queryable
FROM pg_stat_user_tables
WHERE schemaname = 'psv_test' AND relname = 'psv_dml_test';

DROP TABLE psv_dml_test;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA psv_test CASCADE;
SET search_path = public;
