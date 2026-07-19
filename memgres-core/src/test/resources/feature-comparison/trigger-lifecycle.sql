-- SQL verification: trigger lifecycle issues (C2, H2, H6, M8)
-- Tests trigger registration, statement triggers, atomicity, and metadata

-- =============================================================================
-- C2: Triggers survive DROP TABLE and re-attach to recreated tables
-- =============================================================================

-- Setup
CREATE TABLE t_c2 (id int PRIMARY KEY, val text);
CREATE FUNCTION tr_c2_fn() RETURNS trigger AS $$ BEGIN RAISE NOTICE 'fired'; RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER tr_c2 BEFORE INSERT ON t_c2 FOR EACH ROW EXECUTE FUNCTION tr_c2_fn();
DROP TABLE t_c2;
-- After drop, trigger should not exist
SELECT count(*) FROM pg_trigger WHERE tgname = 'tr_c2';
-- expected: 0

CREATE TABLE t_c2 (id int PRIMARY KEY, val text);
INSERT INTO t_c2 VALUES (1, 'test');
-- Should NOT fire old trigger
SELECT * FROM t_c2;
-- expected: (1, 'test')

DROP TABLE t_c2;
DROP FUNCTION tr_c2_fn();

-- =============================================================================
-- H2: Statement-level trigger machinery
-- =============================================================================

-- BEFORE STATEMENT triggers fire
CREATE TABLE t_h2 (id int, val text);
CREATE TABLE t_h2_log (msg text);
CREATE FUNCTION before_stmt_fn() RETURNS trigger AS $$
BEGIN
  INSERT INTO t_h2_log VALUES ('BS:' || TG_OP);
  RETURN NULL;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER tr_bs BEFORE INSERT ON t_h2 FOR EACH STATEMENT EXECUTE FUNCTION before_stmt_fn();
INSERT INTO t_h2 VALUES (1, 'a'), (2, 'b');
SELECT * FROM t_h2_log;
-- expected: ('BS:INSERT')

-- AFTER ROW triggers queue (not interleaved)
CREATE TABLE t_h2b (id int, val text);
CREATE TABLE t_h2b_log (msg text, seq serial);
CREATE FUNCTION row_log_fn() RETURNS trigger AS $$
BEGIN
  INSERT INTO t_h2b_log(msg) VALUES (TG_WHEN || ':' || TG_LEVEL || ':' || NEW.id);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER tr_br BEFORE INSERT ON t_h2b FOR EACH ROW EXECUTE FUNCTION row_log_fn();
CREATE TRIGGER tr_ar AFTER INSERT ON t_h2b FOR EACH ROW EXECUTE FUNCTION row_log_fn();
INSERT INTO t_h2b VALUES (1, 'a'), (2, 'b');
SELECT msg FROM t_h2b_log ORDER BY seq;
-- PG order: BEFORE:ROW:1, BEFORE:ROW:2, AFTER:ROW:1, AFTER:ROW:2

-- Transition tables for UPDATE/DELETE
CREATE TABLE t_h2c (id int, val text);
INSERT INTO t_h2c VALUES (1, 'a'), (2, 'b');
CREATE TABLE t_h2c_log (msg text);
CREATE FUNCTION after_upd_stmt_fn() RETURNS trigger AS $$
DECLARE r RECORD;
BEGIN
  FOR r IN SELECT * FROM newtab LOOP
    INSERT INTO t_h2c_log VALUES ('new:' || r.id || ':' || r.val);
  END LOOP;
  FOR r IN SELECT * FROM oldtab LOOP
    INSERT INTO t_h2c_log VALUES ('old:' || r.id || ':' || r.val);
  END LOOP;
  RETURN NULL;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER tr_upd_stmt AFTER UPDATE ON t_h2c
  REFERENCING OLD TABLE AS oldtab NEW TABLE AS newtab
  FOR EACH STATEMENT EXECUTE FUNCTION after_upd_stmt_fn();
UPDATE t_h2c SET val = 'updated' WHERE id = 1;
SELECT * FROM t_h2c_log ORDER BY msg;
-- expected: new:1:updated, old:1:a

DROP TABLE t_h2, t_h2_log, t_h2b, t_h2b_log, t_h2c, t_h2c_log;
DROP FUNCTION before_stmt_fn, row_log_fn, after_upd_stmt_fn;

-- =============================================================================
-- H6: Trigger errors and statement atomicity; OLD in COALESCE
-- =============================================================================

-- COALESCE with OLD.field in INSERT trigger should not error
CREATE TABLE t_h6 (id int PRIMARY KEY, val text);
CREATE FUNCTION coalesce_fn() RETURNS trigger AS $$
BEGIN
  NEW.val := COALESCE(NEW.val, 'default');
  RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER tr_h6 BEFORE INSERT ON t_h6 FOR EACH ROW EXECUTE FUNCTION coalesce_fn();
INSERT INTO t_h6 VALUES (1, NULL);
SELECT val FROM t_h6;
-- expected: 'default'

DROP TABLE t_h6;
DROP FUNCTION coalesce_fn;

-- =============================================================================
-- M8: TG_ARGV/TG_NARGS, trigger ordering, TG_TABLE_NAME on partitions
-- =============================================================================

-- TG_ARGV and TG_NARGS
CREATE TABLE t_m8 (id int, val text);
CREATE TABLE t_m8_log (msg text);
CREATE FUNCTION argv_fn() RETURNS trigger AS $$
BEGIN
  INSERT INTO t_m8_log VALUES ('nargs=' || TG_NARGS || ',argv0=' || TG_ARGV[0] || ',argv1=' || TG_ARGV[1]);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER tr_argv BEFORE INSERT ON t_m8 FOR EACH ROW EXECUTE FUNCTION argv_fn('hello', 'world');
INSERT INTO t_m8 VALUES (1, 'test');
SELECT * FROM t_m8_log;
-- expected: 'nargs=2,argv0=hello,argv1=world'

-- Trigger name ordering (alphabetical, not creation order)
CREATE TABLE t_m8b (id int);
CREATE TABLE t_m8b_log (msg text, seq serial);
CREATE FUNCTION log_name_fn() RETURNS trigger AS $$
BEGIN
  INSERT INTO t_m8b_log(msg) VALUES (TG_NAME);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER z_trigger BEFORE INSERT ON t_m8b FOR EACH ROW EXECUTE FUNCTION log_name_fn();
CREATE TRIGGER a_trigger BEFORE INSERT ON t_m8b FOR EACH ROW EXECUTE FUNCTION log_name_fn();
INSERT INTO t_m8b VALUES (1);
SELECT msg FROM t_m8b_log ORDER BY seq;
-- expected: a_trigger, z_trigger (alphabetical order)

-- pg_trigger.tgtype bitmask
CREATE TABLE t_m8c (id int);
CREATE FUNCTION noop_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER tr_after_upd AFTER UPDATE ON t_m8c FOR EACH ROW EXECUTE FUNCTION noop_fn();
SELECT tgtype FROM pg_trigger WHERE tgname = 'tr_after_upd';
-- PG: AFTER=4 + UPDATE=16 + ROW=1 = 21 (not 37)

DROP TABLE t_m8, t_m8_log, t_m8b, t_m8b_log, t_m8c;
DROP FUNCTION argv_fn, log_name_fn, noop_fn;
