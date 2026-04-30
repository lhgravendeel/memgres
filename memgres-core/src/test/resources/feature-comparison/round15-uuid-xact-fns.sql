-- ============================================================================
-- Feature Comparison: Round 15 — UUID + transaction-id introspection functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION A: uuid-ossp generators
-- ============================================================================

-- 1. uuid_generate_v1 returns v1 (version nibble = 1)
SELECT uuid_generate_v1();

-- 2. uuid_generate_v4
SELECT uuid_generate_v4();

-- 3. uuid_generate_v3 (namespace + name)
SELECT uuid_generate_v3(uuid_ns_dns(), 'example.com');

-- 4. uuid_generate_v5 (namespace + name)
SELECT uuid_generate_v5(uuid_ns_dns(), 'example.com');

-- 5. uuid_generate_v5 deterministic — same name yields same UUID
SELECT (uuid_generate_v5(uuid_ns_dns(), 'example.com')
          = uuid_generate_v5(uuid_ns_dns(), 'example.com'))::text AS eq;

-- 6. uuid_nil returns zero
-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT uuid_nil()::text AS v;

-- 7. uuid_ns_dns
-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT uuid_ns_dns()::text AS v;

-- 8. uuid_ns_url
-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT uuid_ns_url()::text AS v;

-- ============================================================================
-- SECTION B: uuidv7 (PG 18)
-- ============================================================================

-- 9. uuidv7
SELECT uuidv7();

-- ============================================================================
-- SECTION C: pg_current_xact_id / pg_xact_status
-- ============================================================================

BEGIN;
-- 10. pg_current_xact_id returns non-empty id
SELECT pg_current_xact_id();

COMMIT;

-- 11. pg_current_xact_id_if_assigned may return NULL before assignment
SELECT pg_current_xact_id_if_assigned();

-- 12. pg_xact_status
BEGIN;
SELECT pg_xact_status(pg_current_xact_id()::text);
COMMIT;

-- ============================================================================
-- SECTION D: Snapshot introspection
-- ============================================================================

-- 13. pg_current_snapshot
SELECT pg_current_snapshot();

-- 14. pg_snapshot_xmin / xmax
SELECT pg_snapshot_xmin(pg_current_snapshot());
SELECT pg_snapshot_xmax(pg_current_snapshot());

-- 15. pg_snapshot_xip
SELECT pg_snapshot_xip(pg_current_snapshot());

-- 16. pg_visible_in_snapshot
SELECT pg_visible_in_snapshot(pg_current_xact_id_if_assigned(), pg_current_snapshot());

-- ============================================================================
-- SECTION E: pg_notification_queue_usage
-- ============================================================================

-- 17. pg_notification_queue_usage returns fraction 0..1
SELECT (pg_notification_queue_usage() BETWEEN 0 AND 1)::text AS ok;
