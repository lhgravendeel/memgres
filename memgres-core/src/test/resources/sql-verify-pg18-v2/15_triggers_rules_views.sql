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

CREATE TABLE base_t(
  id int PRIMARY KEY,
  qty int,
  note text
);

CREATE TABLE audit_t(
  id int,
  old_qty int,
  new_qty int,
  action text
);

CREATE FUNCTION audit_base_t() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO audit_t VALUES (NEW.id, NULL, NEW.qty, 'INSERT');
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO audit_t VALUES (NEW.id, OLD.qty, NEW.qty, 'UPDATE');
    RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO audit_t VALUES (OLD.id, OLD.qty, NULL, 'DELETE');
    RETURN OLD;
  END IF;
  RETURN NULL;
END;
$$;

CREATE TRIGGER trg_base_t_ins
AFTER INSERT ON base_t
FOR EACH ROW
EXECUTE FUNCTION audit_base_t();

CREATE TRIGGER trg_base_t_upd
AFTER UPDATE ON base_t
FOR EACH ROW
WHEN (OLD.qty IS DISTINCT FROM NEW.qty)
EXECUTE FUNCTION audit_base_t();

CREATE TRIGGER trg_base_t_del
AFTER DELETE ON base_t
FOR EACH ROW
EXECUTE FUNCTION audit_base_t();

INSERT INTO base_t VALUES (1, 10, 'a');
UPDATE base_t SET qty = 11 WHERE id = 1;
DELETE FROM base_t WHERE id = 1;
SELECT * FROM audit_t ORDER BY id, action;

-- statement trigger
CREATE TABLE stmt_log(msg text);
CREATE FUNCTION audit_stmt() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO stmt_log VALUES (TG_OP);
  RETURN NULL;
END;
$$;

CREATE TRIGGER trg_stmt
AFTER INSERT OR UPDATE OR DELETE ON base_t
FOR EACH STATEMENT
EXECUTE FUNCTION audit_stmt();

INSERT INTO base_t VALUES (2, 20, 'b');
UPDATE base_t SET qty = 21 WHERE id = 2;
DELETE FROM base_t WHERE id = 2;
SELECT * FROM stmt_log ORDER BY msg;

-- view and rule coverage
CREATE VIEW v_base AS
SELECT id, qty, note FROM base_t
WITH LOCAL CHECK OPTION;

INSERT INTO base_t VALUES (3, 30, 'c');
SELECT * FROM v_base ORDER BY id;

CREATE VIEW v_rule AS
SELECT id, qty FROM base_t;

CREATE RULE v_rule_ins AS
ON INSERT TO v_rule
DO INSTEAD
  INSERT INTO base_t(id, qty, note) VALUES (NEW.id, NEW.qty, 'rule');

INSERT INTO v_rule VALUES (4, 40);
SELECT * FROM base_t ORDER BY id;

DROP RULE v_rule_ins ON v_rule;
DROP VIEW v_rule;
DROP VIEW v_base;

-- materialized view behavior
CREATE MATERIALIZED VIEW mv_base AS SELECT id, qty FROM base_t;
SELECT * FROM mv_base ORDER BY id;
CREATE UNIQUE INDEX mv_base_uq ON mv_base(id);
REFRESH MATERIALIZED VIEW mv_base;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_base;

-- bad trigger/rule/view cases
CREATE TRIGGER bad_timing DURING INSERT ON base_t FOR EACH ROW EXECUTE FUNCTION audit_base_t();
CREATE TRIGGER bad_event AFTER UPSERT ON base_t FOR EACH ROW EXECUTE FUNCTION audit_base_t();
CREATE TRIGGER bad_row_stmt AFTER INSERT ON base_t FOR EACH ROW FOR EACH STATEMENT EXECUTE FUNCTION audit_base_t();
CREATE TRIGGER bad_missing_func AFTER INSERT ON base_t FOR EACH ROW EXECUTE FUNCTION no_such_trigger_fn();
CREATE FUNCTION bad_trigger_ret() RETURNS int LANGUAGE SQL AS $$ SELECT 1 $$;
CREATE TRIGGER bad_ret AFTER INSERT ON base_t FOR EACH ROW EXECUTE FUNCTION bad_trigger_ret();
CREATE RULE bad_rule AS ON INSERT TO no_such DO INSTEAD NOTHING;
CREATE RULE bad_rule2 AS ON INSERT TO base_t DO ALSO SELECT 1;
CREATE VIEW bad_view AS;
CREATE VIEW bad_view2 AS SELECT * FROM no_such_table;
REFRESH MATERIALIZED VIEW CONCURRENTLY no_such_mv;

DROP SCHEMA compat CASCADE;
