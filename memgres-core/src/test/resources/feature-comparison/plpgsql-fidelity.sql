-- ============================================================================
-- Feature Comparison: PL/pgSQL fidelity
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Covers:
--   1. BEFORE row triggers returning NULL skip the row (count/RETURNING excluded)
--   2. AFTER trigger return values are ignored
--   3. FOUND after UPDATE/INSERT/DELETE, PERFORM, FOR loops (upsert idiom)
--   4. Variable vs column conflicts raise 42702 (plpgsql.variable_conflict=error)
--   5. UPDATE SET targets are never substituted by variables
--   6. EXECUTE ... USING parameter splicing (quoting, $10 vs $1)
--   7. Non-STRICT SELECT INTO with zero rows sets targets to NULL
--   8. FOR loop variables live in an implicit inner block
--   9. FOR ... BY 0 raises 22023
-- ============================================================================

-- Setup
CREATE TABLE pf_items (id int PRIMARY KEY, val int, note text);
INSERT INTO pf_items VALUES (1, 10, NULL), (2, 20, NULL);

-- ============================================================================
-- 1. BEFORE INSERT trigger RETURN NULL skips the row
-- ============================================================================

CREATE OR REPLACE FUNCTION pf_skip_neg() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.val < 0 THEN RETURN NULL; END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER pf_skip_trg BEFORE INSERT ON pf_items FOR EACH ROW EXECUTE FUNCTION pf_skip_neg();

-- stmt: negative row is skipped, positive row lands
INSERT INTO pf_items VALUES (3, -5, 'neg'), (4, 40, 'pos');

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*) AS cnt FROM pf_items;

-- stmt: RETURNING excludes the skipped row
-- begin-expected
-- columns: id
-- row: 5
-- end-expected
INSERT INTO pf_items VALUES (5, 50, 'ok'), (6, -6, 'neg') RETURNING id;

-- ============================================================================
-- 1b. BEFORE UPDATE trigger RETURN NULL leaves the row untouched
-- ============================================================================

CREATE OR REPLACE FUNCTION pf_upd_block() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.val > 100 THEN RETURN NULL; END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER pf_upd_trg BEFORE UPDATE ON pf_items FOR EACH ROW EXECUTE FUNCTION pf_upd_block();

-- stmt: blocked update (val 999 > 100) leaves the row unchanged
UPDATE pf_items SET val = 999 WHERE id = 1;

-- begin-expected
-- columns: val
-- row: 10
-- end-expected
SELECT val FROM pf_items WHERE id = 1;

-- stmt: RETURNING is empty when the row was skipped
-- begin-expected
-- columns: val
-- end-expected
UPDATE pf_items SET val = 500 WHERE id = 2 RETURNING val;

-- stmt: allowed update still works
UPDATE pf_items SET val = 55 WHERE id = 2;

-- begin-expected
-- columns: val
-- row: 55
-- end-expected
SELECT val FROM pf_items WHERE id = 2;

-- ============================================================================
-- 1c. BEFORE DELETE trigger RETURN NULL keeps the row
-- ============================================================================

CREATE OR REPLACE FUNCTION pf_del_block() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.note = 'keep' THEN RETURN NULL; END IF;
  RETURN OLD;
END;
$$;
CREATE TRIGGER pf_del_trg BEFORE DELETE ON pf_items FOR EACH ROW EXECUTE FUNCTION pf_del_block();

UPDATE pf_items SET note = 'keep' WHERE id = 2;

-- stmt: protected row survives the DELETE
DELETE FROM pf_items WHERE id = 2;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM pf_items WHERE id = 2;

-- ============================================================================
-- 2. AFTER trigger return value is ignored
-- ============================================================================

CREATE TABLE pf_after_t (id int PRIMARY KEY, val int);
CREATE OR REPLACE FUNCTION pf_after_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.val := 12345;
  RETURN NULL;
END;
$$;
CREATE TRIGGER pf_after_trg AFTER INSERT ON pf_after_t FOR EACH ROW EXECUTE FUNCTION pf_after_fn();

-- stmt: AFTER trigger RETURN NULL does not skip, and its NEW changes are ignored
INSERT INTO pf_after_t VALUES (1, 10);

-- begin-expected
-- columns: id | val
-- row: 1, 10
-- end-expected
SELECT id, val FROM pf_after_t;

-- ============================================================================
-- 3. FOUND after DML: the upsert idiom
-- ============================================================================

CREATE TABLE pf_kv (k int PRIMARY KEY, v text);
CREATE OR REPLACE FUNCTION pf_upsert(p_k int, p_v text) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  UPDATE pf_kv SET v = p_v WHERE k = p_k;
  IF NOT FOUND THEN
    INSERT INTO pf_kv VALUES (p_k, p_v);
    RETURN 'inserted';
  END IF;
  RETURN 'updated';
END;
$$;

-- begin-expected
-- columns: pf_upsert
-- row: inserted
-- end-expected
SELECT pf_upsert(1, 'a') AS pf_upsert;

-- begin-expected
-- columns: pf_upsert
-- row: updated
-- end-expected
SELECT pf_upsert(1, 'b') AS pf_upsert;

-- begin-expected
-- columns: v
-- row: b
-- end-expected
SELECT v FROM pf_kv WHERE k = 1;

-- FOUND after DELETE hit and miss
CREATE OR REPLACE FUNCTION pf_del_found(p_k int) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM pf_kv WHERE k = p_k;
  RETURN FOUND;
END;
$$;

-- begin-expected
-- columns: pf_del_found
-- row: true
-- end-expected
SELECT pf_del_found(1) AS pf_del_found;

-- begin-expected
-- columns: pf_del_found
-- row: false
-- end-expected
SELECT pf_del_found(999) AS pf_del_found;

-- FOUND after PERFORM and FOR loops
CREATE OR REPLACE FUNCTION pf_found_states() RETURNS text LANGUAGE plpgsql AS $$
DECLARE res text; r record;
BEGIN
  PERFORM 1 FROM pf_items WHERE id = 1;
  res := FOUND::text;
  PERFORM 1 FROM pf_items WHERE id = -1;
  res := res || ',' || FOUND::text;
  FOR i IN 1..2 LOOP NULL; END LOOP;
  res := res || ',' || FOUND::text;
  FOR r IN SELECT id FROM pf_items WHERE id = -1 LOOP NULL; END LOOP;
  res := res || ',' || FOUND::text;
  RETURN res;
END;
$$;

-- begin-expected
-- columns: pf_found_states
-- row: true,false,true,false
-- end-expected
SELECT pf_found_states() AS pf_found_states;

-- ============================================================================
-- 4. Variable vs column conflict raises 42702
-- ============================================================================

CREATE TABLE pf_emp (salary int);
INSERT INTO pf_emp VALUES (100), (200);

CREATE OR REPLACE FUNCTION pf_amb() RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE salary int := 999;
BEGIN
  RETURN (SELECT sum(salary) FROM pf_emp);
END;
$$;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "salary" is ambiguous
-- end-expected-error
SELECT pf_amb();

-- Qualified references stay columns: alias.col is unambiguous
CREATE OR REPLACE FUNCTION pf_qual() RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE salary int := 999; total bigint;
BEGIN
  SELECT sum(e.salary) INTO total FROM pf_emp e;
  RETURN total;
END;
$$;

-- begin-expected
-- columns: pf_qual
-- row: 300
-- end-expected
SELECT pf_qual() AS pf_qual;

-- Function-name qualification reaches PARAMETERS only: the hidden outer block labeled
-- with the function name contains just the parameters, so funcname.declared_var falls
-- through to table resolution and fails with 42P01
CREATE OR REPLACE FUNCTION pf_blockqual() RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE salary int := 7; total bigint;
BEGIN
  SELECT sum(e.salary) + pf_blockqual.salary INTO total FROM pf_emp e;
  RETURN total;
END;
$$;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "pf_blockqual"
-- end-expected-error
SELECT pf_blockqual() AS pf_blockqual;

-- The documented PG idiom: funcname.param disambiguates a parameter from a column
CREATE OR REPLACE FUNCTION pf_paramqual(salary int) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE total bigint;
BEGIN
  SELECT sum(e.salary) + pf_paramqual.salary INTO total FROM pf_emp e;
  RETURN total;
END;
$$;

-- begin-expected
-- columns: pf_paramqual
-- row: 307
-- end-expected
SELECT pf_paramqual(7) AS pf_paramqual;

-- ============================================================================
-- 5. UPDATE SET target is never substituted
-- ============================================================================

CREATE TABLE pf_ut (id int PRIMARY KEY, counter int);
INSERT INTO pf_ut VALUES (1, 0);

CREATE OR REPLACE FUNCTION pf_set_counter() RETURNS int LANGUAGE plpgsql AS $$
DECLARE counter int := 99;
BEGIN
  UPDATE pf_ut SET counter = 5 WHERE id = 1;
  RETURN counter;
END;
$$;

-- begin-expected
-- columns: pf_set_counter
-- row: 99
-- end-expected
SELECT pf_set_counter() AS pf_set_counter;

-- begin-expected
-- columns: counter
-- row: 5
-- end-expected
SELECT counter FROM pf_ut WHERE id = 1;

-- ============================================================================
-- 6. EXECUTE ... USING parameter splicing
-- ============================================================================

CREATE TABLE pf_dyn (id int PRIMARY KEY, name text);
INSERT INTO pf_dyn VALUES (1, 'alice'), (2, 'bob'), (3, 'alice');

CREATE OR REPLACE FUNCTION pf_dyn_count(p_name text) RETURNS int LANGUAGE plpgsql AS $$
DECLARE c int := 0; r record;
BEGIN
  FOR r IN EXECUTE 'SELECT id FROM pf_dyn WHERE name = $1' USING p_name LOOP
    c := c + 1;
  END LOOP;
  RETURN c;
END;
$$;

-- begin-expected
-- columns: pf_dyn_count
-- row: 2
-- end-expected
SELECT pf_dyn_count('alice') AS pf_dyn_count;

-- More than nine USING parameters: $10/$11 must not be corrupted by $1
CREATE OR REPLACE FUNCTION pf_dyn_many() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r record; res text;
BEGIN
  FOR r IN EXECUTE 'SELECT $1 || $10 || $11 AS x'
      USING 'a','b','c','d','e','f','g','h','i','j','k' LOOP
    res := r.x;
  END LOOP;
  RETURN res;
END;
$$;

-- begin-expected
-- columns: pf_dyn_many
-- row: ajk
-- end-expected
SELECT pf_dyn_many() AS pf_dyn_many;

-- ============================================================================
-- 7. Non-STRICT SELECT INTO with zero rows sets targets to NULL
-- ============================================================================

CREATE OR REPLACE FUNCTION pf_zero_rows() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v text := 'sentinel';
BEGIN
  SELECT note INTO v FROM pf_items WHERE id = -1;
  RETURN coalesce(v, 'was-null');
END;
$$;

-- begin-expected
-- columns: pf_zero_rows
-- row: was-null
-- end-expected
SELECT pf_zero_rows() AS pf_zero_rows;

-- ============================================================================
-- 8. Integer FOR loop variable lives in an implicit inner block
-- ============================================================================

CREATE OR REPLACE FUNCTION pf_loop_scope() RETURNS int LANGUAGE plpgsql AS $$
DECLARE i int := 100; s int := 0;
BEGIN
  FOR i IN 1..3 LOOP s := s + i; END LOOP;
  RETURN i * 1000 + s;
END;
$$;

-- begin-expected
-- columns: pf_loop_scope
-- row: 100006
-- end-expected
SELECT pf_loop_scope() AS pf_loop_scope;

-- ============================================================================
-- 9. FOR ... BY 0 raises 22023
-- ============================================================================

CREATE OR REPLACE FUNCTION pf_by_zero() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  FOR i IN 1..3 BY 0 LOOP NULL; END LOOP;
END;
$$;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: BY value of FOR loop must be greater than zero
-- end-expected-error
SELECT pf_by_zero();

-- Cleanup
DROP FUNCTION pf_by_zero();
DROP FUNCTION pf_loop_scope();
DROP FUNCTION pf_zero_rows();
DROP FUNCTION pf_dyn_many();
DROP FUNCTION pf_dyn_count(text);
DROP FUNCTION pf_set_counter();
DROP FUNCTION pf_paramqual(int);
DROP FUNCTION pf_blockqual();
DROP FUNCTION pf_qual();
DROP FUNCTION pf_amb();
DROP FUNCTION pf_found_states();
DROP FUNCTION pf_del_found(int);
DROP FUNCTION pf_upsert(int, text);
DROP TABLE pf_dyn;
DROP TABLE pf_ut;
DROP TABLE pf_emp;
DROP TABLE pf_kv;
DROP TABLE pf_after_t CASCADE;
DROP FUNCTION IF EXISTS pf_after_fn();
DROP TABLE pf_items CASCADE;
DROP FUNCTION IF EXISTS pf_skip_neg();
DROP FUNCTION IF EXISTS pf_upd_block();
DROP FUNCTION IF EXISTS pf_del_block();
