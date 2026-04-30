-- ============================================================================
-- Feature Comparison: Round 14 — Event Triggers (unimplemented)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Event triggers (PG 9.3+) fire on DDL events. Memgres has no parser,
-- no pg_event_trigger catalog, no dispatch path.
-- ============================================================================

DROP SCHEMA IF EXISTS r14_et CASCADE;
CREATE SCHEMA r14_et;
SET search_path = r14_et, public;

-- ============================================================================
-- SECTION A: pg_event_trigger catalog queryable
-- ============================================================================

-- 1. Catalog exists (count over empty table returns 0)
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_event_trigger;

-- ============================================================================
-- SECTION B: CREATE EVENT TRIGGER
-- ============================================================================

CREATE OR REPLACE FUNCTION r14_et_fn() RETURNS event_trigger LANGUAGE plpgsql
AS $$ BEGIN END $$;

-- 2. ddl_command_start
CREATE EVENT TRIGGER r14_et_start ON ddl_command_start EXECUTE FUNCTION r14_et_fn();

-- begin-expected
-- columns: name
-- row: r14_et_start
-- end-expected
SELECT evtname AS name FROM pg_event_trigger WHERE evtname = 'r14_et_start';

-- 3. ddl_command_end
CREATE EVENT TRIGGER r14_et_end ON ddl_command_end EXECUTE FUNCTION r14_et_fn();

-- begin-expected
-- columns: evt
-- row: ddl_command_end
-- end-expected
SELECT evtevent AS evt FROM pg_event_trigger WHERE evtname = 'r14_et_end';

-- 4. sql_drop
CREATE EVENT TRIGGER r14_et_drop ON sql_drop EXECUTE FUNCTION r14_et_fn();

-- begin-expected
-- columns: evt
-- row: sql_drop
-- end-expected
SELECT evtevent AS evt FROM pg_event_trigger WHERE evtname = 'r14_et_drop';

-- 5. table_rewrite
CREATE EVENT TRIGGER r14_et_rewrite ON table_rewrite EXECUTE FUNCTION r14_et_fn();

-- begin-expected
-- columns: evt
-- row: table_rewrite
-- end-expected
SELECT evtevent AS evt FROM pg_event_trigger WHERE evtname = 'r14_et_rewrite';

-- 6. WHEN TAG IN (...)
CREATE EVENT TRIGGER r14_et_tags ON ddl_command_start
  WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE')
  EXECUTE FUNCTION r14_et_fn();

-- begin-expected
-- columns: has
-- row: t
-- end-expected
SELECT (evttags IS NOT NULL AND array_length(evttags, 1) = 2)::text AS has
  FROM pg_event_trigger WHERE evtname = 'r14_et_tags';

-- ============================================================================
-- SECTION C: ALTER EVENT TRIGGER
-- ============================================================================

-- 7. DISABLE
ALTER EVENT TRIGGER r14_et_start DISABLE;

-- begin-expected
-- columns: en
-- row: D
-- end-expected
SELECT evtenabled::text AS en FROM pg_event_trigger WHERE evtname = 'r14_et_start';

-- 8. ENABLE
ALTER EVENT TRIGGER r14_et_start ENABLE;

-- begin-expected
-- columns: en
-- row: O
-- end-expected
SELECT evtenabled::text AS en FROM pg_event_trigger WHERE evtname = 'r14_et_start';

-- 9. RENAME
ALTER EVENT TRIGGER r14_et_start RENAME TO r14_et_start2;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_event_trigger WHERE evtname = 'r14_et_start2';

-- ============================================================================
-- SECTION D: DROP EVENT TRIGGER
-- ============================================================================

-- 10. DROP
DROP EVENT TRIGGER r14_et_start2;

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_event_trigger WHERE evtname = 'r14_et_start2';

-- 11. DROP IF EXISTS no-op
DROP EVENT TRIGGER IF EXISTS r14_et_never_existed;

-- ============================================================================
-- SECTION E: Helper functions exist
-- ============================================================================

-- 12. pg_event_trigger_ddl_commands
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_event_trigger_ddl_commands';

-- 13. pg_event_trigger_dropped_objects
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_event_trigger_dropped_objects';

-- 14. pg_event_trigger_table_rewrite_oid
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_proc WHERE proname = 'pg_event_trigger_table_rewrite_oid';

-- ============================================================================
-- SECTION F: event_trigger pseudo-type
-- ============================================================================

-- 15. event_trigger type exists
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_type WHERE typname = 'event_trigger';

-- ============================================================================
-- SECTION G: Invalid event name
-- ============================================================================

CREATE OR REPLACE FUNCTION r14_et_fn2() RETURNS event_trigger LANGUAGE plpgsql
AS $$ BEGIN END $$;

-- 16. Invalid event name rejected
-- begin-expected-error
-- message-like: event
-- end-expected-error
CREATE EVENT TRIGGER r14_et_bad ON not_an_event EXECUTE FUNCTION r14_et_fn2();
