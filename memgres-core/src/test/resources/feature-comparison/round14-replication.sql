-- ============================================================================
-- Feature Comparison: Round 14 — Replication infrastructure
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Logical & physical replication, publications, subscriptions, WAL utilities.
-- Most of these are stubbed or missing in Memgres.
-- ============================================================================

DROP SCHEMA IF EXISTS r14_repl CASCADE;
CREATE SCHEMA r14_repl;
SET search_path = r14_repl, public;

-- ============================================================================
-- SECTION A: Replication slots catalog
-- ============================================================================

-- 1. pg_replication_slots is queryable
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_replication_slots;

-- 2. pg_replication_origin queryable
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_replication_origin;

-- ============================================================================
-- SECTION B: Slot management functions exist
-- ============================================================================

-- 3. pg_create_logical_replication_slot
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_create_logical_replication_slot';

-- 4. pg_create_physical_replication_slot
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_create_physical_replication_slot';

-- 5. pg_drop_replication_slot
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_drop_replication_slot';

-- 6. pg_logical_slot_get_changes
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_logical_slot_get_changes';

-- 7. pg_logical_slot_peek_changes
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_logical_slot_peek_changes';

-- 8. pg_replication_slot_advance
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_replication_slot_advance';

-- ============================================================================
-- SECTION C: pg_stat_replication / subscription views
-- ============================================================================

-- 9. pg_stat_replication
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_stat_replication;

-- 10. pg_stat_wal_receiver
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_stat_wal_receiver;

-- 11. pg_stat_subscription
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_stat_subscription;

-- 12. pg_stat_subscription_stats (PG 15+)
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_stat_subscription_stats;

-- ============================================================================
-- SECTION D: Publications
-- ============================================================================

-- 13. FOR ALL TABLES
CREATE PUBLICATION r14_pub_all FOR ALL TABLES;

-- begin-expected
-- columns: all
-- row: t
-- end-expected
SELECT puballtables::text AS all FROM pg_publication WHERE pubname = 'r14_pub_all';

-- 14. FOR TABLE <list>
CREATE TABLE r14_pub_t1 (id int);
CREATE TABLE r14_pub_t2 (id int);
CREATE PUBLICATION r14_pub_tt FOR TABLE r14_pub_t1, r14_pub_t2;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM pg_publication_rel
  WHERE prpubid = (SELECT oid FROM pg_publication WHERE pubname = 'r14_pub_tt');

-- 15. pg_publication_tables view
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*)::text AS c FROM pg_publication_tables WHERE pubname = 'r14_pub_tt';

-- 16. ALTER PUBLICATION ADD TABLE
CREATE TABLE r14_pub_t3 (id int);
ALTER PUBLICATION r14_pub_tt ADD TABLE r14_pub_t3;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*)::text AS n FROM pg_publication_rel
  WHERE prpubid = (SELECT oid FROM pg_publication WHERE pubname = 'r14_pub_tt');

-- 17. FOR TABLES IN SCHEMA (PG 15+)
CREATE SCHEMA r14_pub_sch;
CREATE PUBLICATION r14_pub_bysch FOR TABLES IN SCHEMA r14_pub_sch;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_publication WHERE pubname = 'r14_pub_bysch';

-- ============================================================================
-- SECTION E: Subscriptions
-- ============================================================================

-- 18. pg_subscription queryable
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_subscription;

-- ============================================================================
-- SECTION F: WAL utilities
-- ============================================================================

-- 19. pg_switch_wal
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_switch_wal';

-- 20. pg_walfile_name
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_walfile_name';

-- 21. pg_is_in_recovery returns a boolean
-- begin-expected
-- columns: v
-- row: f
-- end-expected
SELECT pg_is_in_recovery()::text AS v;

-- 22. pg_backup_start / pg_backup_stop (PG 15+)
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname IN ('pg_backup_start', 'pg_backup_stop');

-- 23. pg_promote exists
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_promote';

-- 24. pg_create_restore_point exists
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_create_restore_point';
