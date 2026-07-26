-- ============================================================================
-- Feature Comparison: trigger registration and transition-table scoping
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Transition tables are statement-scoped in PostgreSQL, so a REFERENCING name
-- that collides with a real table shadows it for the duration of the trigger and
-- leaves it intact afterwards. Trigger registration belongs to one schema's
-- table, and the conflict arm of INSERT ... ON CONFLICT DO UPDATE is an UPDATE,
-- so it fires UPDATE row triggers.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS trs_real CASCADE;
DROP TABLE IF EXISTS trs_base CASCADE;
DROP TABLE IF EXISTS trs_log CASCADE;
DROP TABLE IF EXISTS trs_upsert CASCADE;
DROP TABLE IF EXISTS trs_tt CASCADE;
DROP SCHEMA IF EXISTS trs_s2 CASCADE;

CREATE TABLE trs_real (x int);
INSERT INTO trs_real VALUES (42);
CREATE TABLE trs_base (id int);

CREATE FUNCTION trs_noop() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;

-- ============================================================================
-- 1. A transition name colliding with a real table leaves that table alone
-- ============================================================================

CREATE TRIGGER trs_trg AFTER INSERT ON trs_base
  REFERENCING NEW TABLE AS trs_real
  FOR EACH STATEMENT EXECUTE FUNCTION trs_noop();

INSERT INTO trs_base VALUES (1);

-- begin-expected
-- columns: x
-- row: 42
-- end-expected
SELECT x FROM trs_real;

-- ============================================================================
-- 2. Dropping a same-named table in another schema keeps this table's triggers
-- ============================================================================

CREATE SCHEMA trs_s2;
CREATE TABLE trs_tt (id int);
CREATE TABLE trs_s2.trs_tt (id int);
CREATE TABLE trs_log (msg text);

CREATE FUNCTION trs_mark() RETURNS trigger AS $$
BEGIN INSERT INTO trs_log VALUES ('fired'); RETURN NEW; END; $$ LANGUAGE plpgsql;

CREATE TRIGGER trs_tt_trg BEFORE INSERT ON trs_tt FOR EACH ROW EXECUTE FUNCTION trs_mark();

DROP TABLE trs_s2.trs_tt;

INSERT INTO trs_tt VALUES (1);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM trs_log;

-- ============================================================================
-- 3. ON CONFLICT DO UPDATE fires UPDATE row triggers
-- ============================================================================

CREATE TABLE trs_upsert (id int PRIMARY KEY, v int);
CREATE TABLE trs_ulog (msg text);

CREATE FUNCTION trs_ulogf() RETURNS trigger AS $$
BEGIN INSERT INTO trs_ulog VALUES (TG_WHEN || ' ' || TG_OP); RETURN NEW; END; $$ LANGUAGE plpgsql;

CREATE TRIGGER trs_ub BEFORE UPDATE ON trs_upsert FOR EACH ROW EXECUTE FUNCTION trs_ulogf();
CREATE TRIGGER trs_ua AFTER UPDATE ON trs_upsert FOR EACH ROW EXECUTE FUNCTION trs_ulogf();

INSERT INTO trs_upsert VALUES (1, 1);
INSERT INTO trs_upsert VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v;

-- begin-expected
-- columns: msg
-- row: AFTER UPDATE
-- row: BEFORE UPDATE
-- end-expected
SELECT msg FROM trs_ulog ORDER BY msg;

-- begin-expected
-- columns: id | v
-- row: 1, 2
-- end-expected
SELECT id, v FROM trs_upsert;

-- A BEFORE trigger may still rewrite the row the conflict arm writes
DROP TABLE IF EXISTS trs_mod CASCADE;
CREATE TABLE trs_mod (id int PRIMARY KEY, v int);

CREATE FUNCTION trs_modf() RETURNS trigger AS $$
BEGIN NEW.v := NEW.v * 10; RETURN NEW; END; $$ LANGUAGE plpgsql;

CREATE TRIGGER trs_modb BEFORE UPDATE ON trs_mod FOR EACH ROW EXECUTE FUNCTION trs_modf();
INSERT INTO trs_mod VALUES (1, 1);
INSERT INTO trs_mod VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v;

-- begin-expected
-- columns: id | v
-- row: 1, 50
-- end-expected
SELECT id, v FROM trs_mod;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE trs_mod CASCADE;
DROP TABLE trs_upsert CASCADE;
DROP TABLE trs_ulog CASCADE;
DROP TABLE trs_tt CASCADE;
DROP TABLE trs_log CASCADE;
DROP TABLE trs_base CASCADE;
DROP TABLE trs_real CASCADE;
DROP SCHEMA trs_s2 CASCADE;
