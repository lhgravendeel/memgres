-- ============================================================================
-- Feature Comparison: Round 18 — Stats / file / control catalogs + replication
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION U1: pg_stat_database PG 17+ columns
-- ============================================================================

-- 1. sessions column exists
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM information_schema.columns
 WHERE table_schema='pg_catalog' AND table_name='pg_stat_database'
   AND column_name='sessions';

-- 2. session_time column exists
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM information_schema.columns
 WHERE table_schema='pg_catalog' AND table_name='pg_stat_database'
   AND column_name='session_time';

-- 3. active_time column exists
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM information_schema.columns
 WHERE table_schema='pg_catalog' AND table_name='pg_stat_database'
   AND column_name='active_time';

-- 4. idle_in_transaction_time column exists
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM information_schema.columns
 WHERE table_schema='pg_catalog' AND table_name='pg_stat_database'
   AND column_name='idle_in_transaction_time';

-- 5. parallel_workers_launched column exists
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM information_schema.columns
 WHERE table_schema='pg_catalog' AND table_name='pg_stat_database'
   AND column_name='parallel_workers_launched';

-- ============================================================================
-- SECTION U2: pg_stat_checkpointer (PG 17 split)
-- ============================================================================

-- 6. pg_stat_checkpointer queryable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 0) AS ok FROM pg_stat_checkpointer;

-- 7. pg_stat_bgwriter no longer has checkpoint_write_time
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM information_schema.columns
 WHERE table_schema='pg_catalog' AND table_name='pg_stat_bgwriter'
   AND column_name='checkpoint_write_time';

-- 8. pg_stat_bgwriter no longer has buffers_checkpoint
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM information_schema.columns
 WHERE table_schema='pg_catalog' AND table_name='pg_stat_bgwriter'
   AND column_name='buffers_checkpoint';

-- ============================================================================
-- SECTION U3: file / ident / hba views
-- ============================================================================

-- 9. pg_hba_file_rules queryable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 0) AS ok FROM pg_hba_file_rules;

-- 10. pg_file_settings queryable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 0) AS ok FROM pg_file_settings;

-- 11. pg_ident_file_mappings queryable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 0) AS ok FROM pg_ident_file_mappings;

-- ============================================================================
-- SECTION U4: pg_control_* SRFs
-- ============================================================================

-- 12. pg_control_checkpoint registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='pg_control_checkpoint';

-- 13. pg_control_system registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='pg_control_system';

-- 14. pg_control_init registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='pg_control_init';

-- 15. pg_control_recovery registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='pg_control_recovery';

-- ============================================================================
-- SECTION U5: pg_database.datfrozenxid typed as xid
-- ============================================================================

-- 16. datfrozenxid is xid type
-- begin-expected
-- columns: udt_name
-- row: xid
-- end-expected
SELECT udt_name FROM information_schema.columns
 WHERE table_schema='pg_catalog' AND table_name='pg_database'
   AND column_name='datfrozenxid';

-- ============================================================================
-- SECTION U6: pg_database PG 15+ columns
-- ============================================================================

-- 17. datcollversion present
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM information_schema.columns
 WHERE table_schema='pg_catalog' AND table_name='pg_database'
   AND column_name='datcollversion';

-- 18. daticulocale present
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM information_schema.columns
 WHERE table_schema='pg_catalog' AND table_name='pg_database'
   AND column_name='daticulocale';

-- ============================================================================
-- SECTION V: pg_replication_origin
-- ============================================================================

-- 19. pg_replication_origin queryable
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 0) AS ok FROM pg_replication_origin;
