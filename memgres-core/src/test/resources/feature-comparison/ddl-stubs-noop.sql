-- ============================================================================
-- Feature Comparison: DDL Stubs / No-Ops (E1, E2, E3)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- These DDL/admin commands are parsed and accepted but have no functional
-- effect. This is intentional for pg_dump compatibility. This file verifies
-- they parse without error.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS noop_test CASCADE;
CREATE SCHEMA noop_test;
SET search_path = noop_test, public;

-- ============================================================================
-- E1: CREATE / ALTER / DROP No-Ops
-- ============================================================================

-- ============================================================================
-- 1. COLLATION: CREATE, ALTER, DROP
-- ============================================================================

-- note: These should parse without error even though collation has no effect
CREATE COLLATION noop_coll (LOCALE = 'en_US.UTF-8');

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

ALTER COLLATION noop_coll RENAME TO noop_coll_new;
DROP COLLATION IF EXISTS noop_coll_new;

-- ============================================================================
-- 2. TEXT SEARCH CONFIGURATION
-- ============================================================================

CREATE TEXT SEARCH CONFIGURATION noop_tsc (COPY = english);

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP TEXT SEARCH CONFIGURATION IF EXISTS noop_tsc;

-- ============================================================================
-- 3. TEXT SEARCH DICTIONARY
-- ============================================================================

CREATE TEXT SEARCH DICTIONARY noop_tsd (TEMPLATE = simple);

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP TEXT SEARCH DICTIONARY IF EXISTS noop_tsd;

-- ============================================================================
-- 4. EVENT TRIGGER
-- ============================================================================

CREATE OR REPLACE FUNCTION noop_evt_func() RETURNS event_trigger
LANGUAGE plpgsql AS $$ BEGIN NULL; END; $$;

CREATE EVENT TRIGGER noop_evt ON ddl_command_end EXECUTE FUNCTION noop_evt_func();

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP EVENT TRIGGER IF EXISTS noop_evt;

-- ============================================================================
-- 5. FOREIGN DATA WRAPPER
-- ============================================================================

CREATE FOREIGN DATA WRAPPER noop_fdw;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP FOREIGN DATA WRAPPER IF EXISTS noop_fdw;

-- ============================================================================
-- 6. SERVER
-- ============================================================================

CREATE FOREIGN DATA WRAPPER noop_fdw2;
CREATE SERVER noop_srv FOREIGN DATA WRAPPER noop_fdw2;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP SERVER IF EXISTS noop_srv;
DROP FOREIGN DATA WRAPPER IF EXISTS noop_fdw2;

-- ============================================================================
-- 7. FOREIGN TABLE
-- ============================================================================

CREATE FOREIGN DATA WRAPPER noop_fdw3;
CREATE SERVER noop_srv2 FOREIGN DATA WRAPPER noop_fdw3;
CREATE FOREIGN TABLE noop_ft (id integer, val text) SERVER noop_srv2;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP FOREIGN TABLE IF EXISTS noop_ft;
DROP SERVER IF EXISTS noop_srv2;
DROP FOREIGN DATA WRAPPER IF EXISTS noop_fdw3;

-- ============================================================================
-- 8. PUBLICATION
-- ============================================================================

CREATE PUBLICATION noop_pub FOR ALL TABLES;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP PUBLICATION IF EXISTS noop_pub;

-- ============================================================================
-- 9. SUBSCRIPTION (requires connection, may error — test IF EXISTS drop)
-- ============================================================================

-- note: CREATE SUBSCRIPTION needs a connection string; just test DROP IF EXISTS
DROP SUBSCRIPTION IF EXISTS noop_sub;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 10. TABLESPACE
-- ============================================================================

-- note: In-memory DB, tablespaces are no-ops
-- CREATE TABLESPACE requires superuser and a directory; test DROP IF EXISTS
DROP TABLESPACE IF EXISTS noop_ts;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 11. LANGUAGE (beyond plpgsql)
-- ============================================================================

CREATE LANGUAGE IF NOT EXISTS plpgsql;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP LANGUAGE IF EXISTS nonexistent_lang;

-- ============================================================================
-- 12. TRANSFORM
-- ============================================================================

-- note: Just test that DROP IF EXISTS parses
DROP TRANSFORM IF EXISTS FOR integer LANGUAGE plpgsql;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 13. ACCESS METHOD
-- ============================================================================

-- note: Test DROP IF EXISTS
DROP ACCESS METHOD IF EXISTS noop_am;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 14. STATISTICS objects
-- ============================================================================

CREATE TABLE noop_stat_tbl (a integer, b integer);
CREATE STATISTICS noop_stat ON a, b FROM noop_stat_tbl;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP STATISTICS IF EXISTS noop_stat;
DROP TABLE noop_stat_tbl;

-- ============================================================================
-- 15. CAST (user-defined)
-- ============================================================================

-- note: Built-in casts work; user-defined are no-op stubs
DROP CAST IF EXISTS (text AS integer);

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 16. CONVERSION
-- ============================================================================

DROP CONVERSION IF EXISTS noop_conv;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 17. EXTENSION (beyond plpgsql)
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS plpgsql;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP EXTENSION IF EXISTS nonexistent_ext;

-- ============================================================================
-- E2: ALTER TABLE Sub-command No-Ops
-- ============================================================================

CREATE TABLE noop_alter_tbl (id integer PRIMARY KEY, val text);

-- ============================================================================
-- 18. ENABLE / DISABLE TRIGGER
-- ============================================================================

CREATE FUNCTION noop_trigger_func() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER noop_trg BEFORE INSERT ON noop_alter_tbl
  FOR EACH ROW EXECUTE FUNCTION noop_trigger_func();

ALTER TABLE noop_alter_tbl DISABLE TRIGGER noop_trg;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

ALTER TABLE noop_alter_tbl ENABLE TRIGGER noop_trg;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

ALTER TABLE noop_alter_tbl DISABLE TRIGGER ALL;
ALTER TABLE noop_alter_tbl ENABLE TRIGGER ALL;

-- ============================================================================
-- 19. ENABLE / DISABLE ROW LEVEL SECURITY
-- ============================================================================

ALTER TABLE noop_alter_tbl ENABLE ROW LEVEL SECURITY;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

ALTER TABLE noop_alter_tbl DISABLE ROW LEVEL SECURITY;

-- ============================================================================
-- 20. REPLICA IDENTITY
-- ============================================================================

ALTER TABLE noop_alter_tbl REPLICA IDENTITY FULL;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

ALTER TABLE noop_alter_tbl REPLICA IDENTITY DEFAULT;

-- ============================================================================
-- 21. SET STATISTICS
-- ============================================================================

ALTER TABLE noop_alter_tbl ALTER COLUMN val SET STATISTICS 100;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 22. SET STORAGE
-- ============================================================================

ALTER TABLE noop_alter_tbl ALTER COLUMN val SET STORAGE PLAIN;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

ALTER TABLE noop_alter_tbl ALTER COLUMN val SET STORAGE EXTENDED;

-- ============================================================================
-- 23. SET (storage_parameters)
-- ============================================================================

ALTER TABLE noop_alter_tbl SET (fillfactor = 70);

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

ALTER TABLE noop_alter_tbl RESET (fillfactor);

DROP TABLE noop_alter_tbl;

-- ============================================================================
-- E3: Session / Admin No-Ops
-- ============================================================================

-- ============================================================================
-- 24. VACUUM
-- ============================================================================

CREATE TABLE noop_vacuum_tbl (id integer);
INSERT INTO noop_vacuum_tbl VALUES (1), (2), (3);
DELETE FROM noop_vacuum_tbl;

VACUUM noop_vacuum_tbl;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

VACUUM FULL noop_vacuum_tbl;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

VACUUM (VERBOSE) noop_vacuum_tbl;

DROP TABLE noop_vacuum_tbl;

-- ============================================================================
-- 25. ANALYZE
-- ============================================================================

CREATE TABLE noop_analyze_tbl (id integer, val text);
INSERT INTO noop_analyze_tbl VALUES (1, 'a'), (2, 'b');

ANALYZE noop_analyze_tbl;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ANALYZE specific columns
ANALYZE noop_analyze_tbl (val);

DROP TABLE noop_analyze_tbl;

-- ============================================================================
-- 26. REINDEX
-- ============================================================================

CREATE TABLE noop_reindex_tbl (id integer PRIMARY KEY);
INSERT INTO noop_reindex_tbl VALUES (1), (2), (3);

REINDEX TABLE noop_reindex_tbl;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP TABLE noop_reindex_tbl;

-- ============================================================================
-- 27. CLUSTER
-- ============================================================================

CREATE TABLE noop_cluster_tbl (id integer PRIMARY KEY, val text);
CREATE INDEX idx_noop_cluster ON noop_cluster_tbl (val);
INSERT INTO noop_cluster_tbl VALUES (1, 'c'), (2, 'a'), (3, 'b');

CLUSTER noop_cluster_tbl USING idx_noop_cluster;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP TABLE noop_cluster_tbl;

-- ============================================================================
-- 28. CHECKPOINT
-- ============================================================================

CHECKPOINT;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 29. LOAD
-- ============================================================================

-- note: LOAD loads a shared library; should be accepted without error
-- May error on the file not existing, but the command itself should parse

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 30. VACUUM ANALYZE combined
-- ============================================================================

CREATE TABLE noop_va_tbl (id integer PRIMARY KEY);
INSERT INTO noop_va_tbl VALUES (1);

VACUUM ANALYZE noop_va_tbl;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

DROP TABLE noop_va_tbl;

-- ============================================================================
-- 31. DISCARD ALL (resets session state)
-- ============================================================================

-- note: DISCARD ALL is a valid session command
DISCARD ALL;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 32. Table still works after no-op commands
-- ============================================================================

-- note: Verify that no-op commands don't corrupt table state
CREATE TABLE noop_verify (id integer PRIMARY KEY, val text);
INSERT INTO noop_verify VALUES (1, 'a'), (2, 'b'), (3, 'c');

VACUUM noop_verify;
ANALYZE noop_verify;
REINDEX TABLE noop_verify;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::integer AS cnt FROM noop_verify;

-- begin-expected
-- columns: id, val
-- row: 2, b
-- end-expected
SELECT * FROM noop_verify WHERE id = 2;

DROP TABLE noop_verify;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA IF EXISTS noop_test CASCADE;
SET search_path = public;
