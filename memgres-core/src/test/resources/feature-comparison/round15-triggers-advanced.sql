-- ============================================================================
-- Feature Comparison: Round 15 — Trigger advanced features
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r15_trg CASCADE;
CREATE SCHEMA r15_trg;
SET search_path = r15_trg, public;

-- ============================================================================
-- SECTION A: WHEN clause evaluated at fire time
-- ============================================================================

CREATE TABLE r15_t_when (id int, v int);
CREATE TABLE r15_t_when_log (id int);

CREATE OR REPLACE FUNCTION r15_fn_when() RETURNS trigger AS $$
BEGIN INSERT INTO r15_t_when_log VALUES (NEW.id); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_when AFTER INSERT ON r15_t_when
  FOR EACH ROW WHEN (NEW.v > 10) EXECUTE FUNCTION r15_fn_when();

INSERT INTO r15_t_when VALUES (1, 5);    -- WHEN false
INSERT INTO r15_t_when VALUES (2, 20);   -- WHEN true

-- 1. Only id=2 logged
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM r15_t_when_log;

-- ============================================================================
-- SECTION B: TRUNCATE trigger
-- ============================================================================

CREATE TABLE r15_t_tr (id int);
CREATE TABLE r15_t_tr_log (ev text);

CREATE OR REPLACE FUNCTION r15_fn_tr() RETURNS trigger AS $$
BEGIN INSERT INTO r15_t_tr_log VALUES (TG_OP); RETURN NULL; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_tr AFTER TRUNCATE ON r15_t_tr
  FOR EACH STATEMENT EXECUTE FUNCTION r15_fn_tr();

INSERT INTO r15_t_tr VALUES (1),(2),(3);
TRUNCATE r15_t_tr;

-- 2. TG_OP='TRUNCATE' logged
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM r15_t_tr_log WHERE ev='TRUNCATE';

-- ============================================================================
-- SECTION C: INSTEAD OF trigger on view
-- ============================================================================

CREATE TABLE r15_t_iot_real (id int, v text);
CREATE VIEW r15_t_iot_view AS SELECT id, v FROM r15_t_iot_real;

CREATE OR REPLACE FUNCTION r15_fn_iot() RETURNS trigger AS $$
BEGIN INSERT INTO r15_t_iot_real VALUES (NEW.id, NEW.v); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_iot INSTEAD OF INSERT ON r15_t_iot_view
  FOR EACH ROW EXECUTE FUNCTION r15_fn_iot();

INSERT INTO r15_t_iot_view VALUES (1, 'hello');

-- 3. Underlying table populated via INSTEAD OF trigger
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM r15_t_iot_real WHERE id=1 AND v='hello';

-- ============================================================================
-- SECTION D: TG_* variables
-- ============================================================================

CREATE TABLE r15_tg_ctx (id int);
CREATE TABLE r15_tg_ctx_log (trigger_name text, lvl text, tbl_schema text, op text);

CREATE OR REPLACE FUNCTION r15_fn_ctx() RETURNS trigger AS $$
BEGIN INSERT INTO r15_tg_ctx_log VALUES (TG_NAME, TG_LEVEL, TG_TABLE_SCHEMA, TG_OP);
      RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_ctx AFTER INSERT ON r15_tg_ctx
  FOR EACH ROW EXECUTE FUNCTION r15_fn_ctx();

INSERT INTO r15_tg_ctx VALUES (1);

-- 4. TG_NAME, TG_LEVEL, TG_TABLE_SCHEMA, TG_OP populated
-- begin-expected
-- columns: trigger_name, lvl, op
-- row: tr_ctx, ROW, INSERT
-- end-expected
SELECT trigger_name, lvl, op FROM r15_tg_ctx_log;

-- ============================================================================
-- SECTION E: Transition tables
-- ============================================================================

CREATE TABLE r15_tt (id int, v int);
CREATE TABLE r15_tt_log (sum_new int, cnt_new int);

CREATE OR REPLACE FUNCTION r15_fn_tt() RETURNS trigger AS $$
BEGIN
  INSERT INTO r15_tt_log
  SELECT COALESCE(SUM(v),0)::int, COUNT(*)::int FROM new_rows;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_tt AFTER INSERT ON r15_tt
  REFERENCING NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION r15_fn_tt();

INSERT INTO r15_tt VALUES (1,10),(2,20),(3,30);

-- 5. Transition table aggregates correctly
-- begin-expected
-- columns: sum_new, cnt_new
-- row: 60, 3
-- end-expected
SELECT sum_new, cnt_new FROM r15_tt_log;

-- ============================================================================
-- SECTION F: UPDATE OF column list
-- ============================================================================

CREATE TABLE r15_uof (id int, a int, b int);
CREATE TABLE r15_uof_log (id int);

CREATE OR REPLACE FUNCTION r15_fn_uof() RETURNS trigger AS $$
BEGIN INSERT INTO r15_uof_log VALUES (NEW.id); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_uof AFTER UPDATE OF a ON r15_uof
  FOR EACH ROW EXECUTE FUNCTION r15_fn_uof();

INSERT INTO r15_uof VALUES (1,10,20),(2,30,40);

UPDATE r15_uof SET b=999 WHERE id=1;   -- no col 'a' → no fire
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM r15_uof_log;

UPDATE r15_uof SET a=50 WHERE id=2;    -- col 'a' → fire
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM r15_uof_log;

-- ============================================================================
-- SECTION G: DISABLE / ENABLE TRIGGER
-- ============================================================================

CREATE TABLE r15_dis (id int);
CREATE TABLE r15_dis_log (id int);

CREATE OR REPLACE FUNCTION r15_fn_dis() RETURNS trigger AS $$
BEGIN INSERT INTO r15_dis_log VALUES (NEW.id); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_dis AFTER INSERT ON r15_dis
  FOR EACH ROW EXECUTE FUNCTION r15_fn_dis();

INSERT INTO r15_dis VALUES (1);
ALTER TABLE r15_dis DISABLE TRIGGER tr_dis;
INSERT INTO r15_dis VALUES (2);

-- 8. Only id=1 logged
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM r15_dis_log;

ALTER TABLE r15_dis ENABLE TRIGGER tr_dis;
INSERT INTO r15_dis VALUES (3);

-- 9. Now id=3 logged too
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*)::int AS c FROM r15_dis_log;

-- 10. pg_trigger.tgenabled='D' when disabled
CREATE TABLE r15_tge (id int);
CREATE OR REPLACE FUNCTION r15_fn_tge() RETURNS trigger AS $$
BEGIN RETURN NEW; END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER tr_tge AFTER INSERT ON r15_tge
  FOR EACH ROW EXECUTE FUNCTION r15_fn_tge();

-- begin-expected
-- columns: s
-- row: O
-- end-expected
SELECT tgenabled::text AS s FROM pg_trigger WHERE tgname='tr_tge';

ALTER TABLE r15_tge DISABLE TRIGGER tr_tge;

-- begin-expected
-- columns: s
-- row: D
-- end-expected
SELECT tgenabled::text AS s FROM pg_trigger WHERE tgname='tr_tge';

-- ============================================================================
-- SECTION H: information_schema.triggers
-- ============================================================================

CREATE TABLE r15_is_tg (id int);
CREATE OR REPLACE FUNCTION r15_fn_is_tg() RETURNS trigger AS $$
BEGIN RETURN NEW; END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER tr_is_tg AFTER INSERT ON r15_is_tg
  FOR EACH ROW EXECUTE FUNCTION r15_fn_is_tg();

-- 11. information_schema.triggers populated
SELECT count(*)::int AS c FROM information_schema.triggers
  WHERE trigger_name='tr_is_tg';

-- ============================================================================
-- SECTION I: CONSTRAINT TRIGGER
-- ============================================================================

CREATE TABLE r15_ct (id int);
CREATE TABLE r15_ct_log (note text);
CREATE OR REPLACE FUNCTION r15_fn_ct() RETURNS trigger AS $$
BEGIN INSERT INTO r15_ct_log VALUES ('fired'); RETURN NULL; END;
$$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER tr_ct AFTER INSERT ON r15_ct
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION r15_fn_ct();

BEGIN;
INSERT INTO r15_ct VALUES (1);
-- Before commit, DEFERRED trigger has NOT fired
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM r15_ct_log;
COMMIT;

-- After commit, it has fired
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM r15_ct_log;
