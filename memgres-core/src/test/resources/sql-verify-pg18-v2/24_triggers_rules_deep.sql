\pset pager off
\pset format unaligned
\pset tuples_only off
\pset null <NULL>
\set VERBOSITY verbose
\set SHOW_CONTEXT always
\set ON_ERROR_STOP off

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;
SET client_min_messages = notice;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';

SELECT current_schema() AS current_schema,
       current_setting('TimeZone') AS timezone,
       current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle;

CREATE TABLE t(
  id int PRIMARY KEY,
  a int,
  b text
);

CREATE TABLE trig_log(
  seq int GENERATED ALWAYS AS IDENTITY,
  tag text,
  id int,
  a int,
  b text
);

-- BEFORE trigger mutating NEW
CREATE FUNCTION trg_before_ins() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.b := upper(coalesce(NEW.b, 'x'));
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_before_ins
BEFORE INSERT ON t
FOR EACH ROW
EXECUTE FUNCTION trg_before_ins();

-- UPDATE OF trigger
CREATE FUNCTION trg_update_cols() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO trig_log(tag, id, a, b)
  VALUES ('upd_cols', NEW.id, NEW.a, NEW.b);
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_update_cols
AFTER UPDATE OF a, b ON t
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION trg_update_cols();

-- statement trigger with transition tables
CREATE FUNCTION trg_stmt_trans() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO trig_log(tag, id, a, b)
  SELECT 'stmt_trans', id, a, b FROM newtab;
  RETURN NULL;
END;
$$;

CREATE TRIGGER trg_stmt_trans
AFTER INSERT ON t
REFERENCING NEW TABLE AS newtab
FOR EACH STATEMENT
EXECUTE FUNCTION trg_stmt_trans();

INSERT INTO t VALUES (1, 10, 'abc');
INSERT INTO t VALUES (2, 20, NULL);
UPDATE t SET a = a + 1 WHERE id = 1;
SELECT * FROM t ORDER BY id;
SELECT tag, id, a, b FROM trig_log ORDER BY seq;

-- INSTEAD OF trigger on view
CREATE VIEW v_t AS SELECT id, a, b FROM t;
CREATE FUNCTION trg_view_ins() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO t(id, a, b) VALUES (NEW.id, NEW.a, NEW.b);
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_view_ins
INSTEAD OF INSERT ON v_t
FOR EACH ROW
EXECUTE FUNCTION trg_view_ins();

INSERT INTO v_t VALUES (3, 30, 'from_view');
SELECT * FROM t ORDER BY id;

-- rules deep
CREATE VIEW v_rule2 AS SELECT id, a, b FROM t;
CREATE RULE v_rule2_upd AS
ON UPDATE TO v_rule2
DO INSTEAD
  UPDATE t SET a = NEW.a, b = NEW.b WHERE id = OLD.id;

UPDATE v_rule2 SET a = 99, b = 'rule_upd' WHERE id = 1;
SELECT * FROM t ORDER BY id;

-- bad trigger/rule cases
CREATE TRIGGER bad_when AFTER INSERT ON t
FOR EACH ROW
WHEN (no_such_col > 0)
EXECUTE FUNCTION trg_before_ins();

CREATE TRIGGER bad_transition
AFTER UPDATE ON t
REFERENCING NEW TABLE AS newtab
FOR EACH ROW
EXECUTE FUNCTION trg_stmt_trans();

CREATE TRIGGER bad_instead AFTER INSERT ON v_t
FOR EACH ROW
EXECUTE FUNCTION trg_view_ins();

CREATE RULE bad_rule3 AS ON DELETE TO v_rule2 DO INSTEAD NOTHING;
CREATE RULE bad_rule4 AS ON INSERT TO t WHERE NEW.id > 0 DO ALSO SELECT 1;
DROP TRIGGER no_such_trigger ON t;
DROP RULE no_such_rule ON v_rule2;

DROP SCHEMA compat CASCADE;
