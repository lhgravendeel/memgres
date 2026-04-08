-- pg_forward_ref_non_table_objects.sql
--
-- Purpose:
--   Characterization for non-table objects and additional PL/pgSQL statement
--   forms: domains, enums, sequences, collations, cursors, RETURN QUERY,
--   FOR-IN-SELECT, OPEN FOR, PERFORM, and search_path shadowing around temp
--   objects and views.

DROP SCHEMA IF EXISTS test_forward_ref_obj CASCADE;
CREATE SCHEMA test_forward_ref_obj;
SET search_path = test_forward_ref_obj, public;

-------------------------------------------------------------------------------
-- O01: future domain used in expression inside body is deferred to runtime
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION o01_future_domain_cast()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  SELECT ('hello'::o01_future_domain)::text INTO v;
  RETURN v;
END
$fn$;

-- expect-error: 42704
SELECT o01_future_domain_cast();

CREATE DOMAIN o01_future_domain AS text CHECK (VALUE <> '');

-- begin-expected
-- columns: value
-- row: hello
-- end-expected
SELECT o01_future_domain_cast() AS value;

-------------------------------------------------------------------------------
-- O02: enum alteration after first success
-------------------------------------------------------------------------------
CREATE TYPE o02_status AS ENUM ('queued', 'running');

CREATE OR REPLACE FUNCTION o02_enum_text(p o02_status)
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN p::text;
END
$fn$;

-- begin-expected
-- columns: value
-- row: queued
-- end-expected
SELECT o02_enum_text('queued'::o02_status) AS value;

ALTER TYPE o02_status ADD VALUE 'done';

-- begin-expected
-- columns: value
-- row: done
-- end-expected
SELECT o02_enum_text('done'::o02_status) AS value;

-------------------------------------------------------------------------------
-- O03: future sequence reference in static plpgsql SQL is deferred
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION o03_nextval_future_seq()
RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v bigint;
BEGIN
  SELECT nextval('o03_future_seq') INTO v;
  RETURN v;
END
$fn$;

-- expect-error: 42P01
SELECT o03_nextval_future_seq();

CREATE SEQUENCE o03_future_seq START WITH 10;

-- begin-expected
-- columns: seqval
-- row: 10
-- end-expected
SELECT o03_nextval_future_seq()::text AS seqval;

-------------------------------------------------------------------------------
-- O04: explicit future collation in static SQL is deferred in plpgsql
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION o04_future_collation()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  SELECT ('a' COLLATE o04_missing_collation) INTO v;
  RETURN v;
END
$fn$;

-- expect-error: 42704
SELECT o04_future_collation();

-------------------------------------------------------------------------------
-- O05: PERFORM future function is deferred to runtime in plpgsql body
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION o05_perform_future_function()
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  PERFORM o05_helper();
  RETURN 'ok';
END
$fn$;

-- expect-error: 42883
SELECT o05_perform_future_function();

CREATE OR REPLACE FUNCTION o05_helper()
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN;
END
$fn$;

-- begin-expected
-- columns: value
-- row: ok
-- end-expected
SELECT o05_perform_future_function() AS value;

-------------------------------------------------------------------------------
-- O06: SELECT INTO / FOR IN SELECT / RETURN QUERY / OPEN FOR each defer relation lookup
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION o06_select_into()
RETURNS integer
LANGUAGE plpgsql
AS $fn$
DECLARE
  v integer;
BEGIN
  SELECT id INTO v FROM o06_items ORDER BY id LIMIT 1;
  RETURN v;
END
$fn$;

CREATE OR REPLACE FUNCTION o06_for_loop_sum()
RETURNS integer
LANGUAGE plpgsql
AS $fn$
DECLARE
  r record;
  total integer := 0;
BEGIN
  FOR r IN SELECT id FROM o06_items LOOP
    total := total + r.id;
  END LOOP;
  RETURN total;
END
$fn$;

CREATE OR REPLACE FUNCTION o06_return_query()
RETURNS TABLE(id integer)
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN QUERY SELECT id FROM o06_items ORDER BY id;
END
$fn$;

CREATE OR REPLACE FUNCTION o06_cursor_count()
RETURNS integer
LANGUAGE plpgsql
AS $fn$
DECLARE
  c refcursor;
  r record;
  total integer := 0;
BEGIN
  OPEN c FOR SELECT id FROM o06_items ORDER BY id;
  LOOP
    FETCH c INTO r;
    EXIT WHEN NOT FOUND;
    total := total + 1;
  END LOOP;
  CLOSE c;
  RETURN total;
END
$fn$;

-- expect-error: 42P01
SELECT o06_select_into();

-- expect-error: 42P01
SELECT o06_for_loop_sum();

-- expect-error: 42P01
SELECT * FROM o06_return_query();

-- expect-error: 42P01
SELECT o06_cursor_count();

CREATE TABLE o06_items (id integer PRIMARY KEY);
INSERT INTO o06_items(id) VALUES (1), (2), (3);

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT id::text AS id FROM o06_return_query();

-- begin-expected
-- columns: first_id,total,counted
-- row: 1|6|3
-- end-expected
SELECT o06_select_into()::text AS first_id,
       o06_for_loop_sum()::text AS total,
       o06_cursor_count()::text AS counted;

-------------------------------------------------------------------------------
-- O07: temp table and temp view shadowing under search_path
-------------------------------------------------------------------------------
CREATE TABLE o07_items (
  marker text PRIMARY KEY
);

INSERT INTO o07_items(marker) VALUES ('permanent');

CREATE OR REPLACE FUNCTION o07_lookup()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  SELECT marker INTO v FROM o07_items LIMIT 1;
  RETURN v;
END
$fn$;

-- begin-expected
-- columns: marker
-- row: permanent
-- end-expected
SELECT o07_lookup() AS marker;

CREATE TEMP TABLE o07_items (
  marker text PRIMARY KEY
);

INSERT INTO o07_items(marker) VALUES ('temp');

-- Characterization query: engines may differ depending on plan caching and
-- search_path resolution timing, so this intentionally records current behavior.
-- begin-expected
-- columns: marker
-- row: temp
-- end-expected
SELECT o07_lookup() AS marker;
