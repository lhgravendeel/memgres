-- pg_forward_ref_extended_coverage.sql
--
-- Purpose:
--   Cover forward-reference edge cases not tested elsewhere:
--   CTE, subqueries, UPDATE/DELETE, ON CONFLICT, UNION, EXISTS,
--   search path shadowing, column type changes, and nested function chains.
--
-- All plpgsql body SQL is deferred to execution time (memgres behaves
-- like PG with check_function_bodies = off for body references).

DROP SCHEMA IF EXISTS test_ext_cov CASCADE;
CREATE SCHEMA test_ext_cov;
SET search_path = test_ext_cov, public;

-------------------------------------------------------------------------------
-- EC01: CTE (WITH query) referencing future table in plpgsql body
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ec01_cte_future()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  WITH ranked AS (
    SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn
    FROM ec01_items
  )
  SELECT name INTO v FROM ranked WHERE rn = 1;
  RETURN v;
END
$fn$;

-- expect-error: 42P01
SELECT ec01_cte_future();

CREATE TABLE ec01_items (
  id integer PRIMARY KEY,
  name text NOT NULL
);

INSERT INTO ec01_items(id, name) VALUES (1, 'alpha'), (2, 'beta');

-- begin-expected
-- columns: value
-- row: alpha
-- end-expected
SELECT ec01_cte_future() AS value;

-------------------------------------------------------------------------------
-- EC02: Subquery in SELECT list referencing future table in plpgsql body
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ec02_subquery_future()
RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v bigint;
BEGIN
  SELECT (SELECT count(*) FROM ec02_items) INTO v;
  RETURN v;
END
$fn$;

-- expect-error: 42P01
SELECT ec02_subquery_future();

CREATE TABLE ec02_items (
  id integer PRIMARY KEY
);

INSERT INTO ec02_items(id) VALUES (1), (2), (3);

-- begin-expected
-- columns: value
-- row: 3
-- end-expected
SELECT ec02_subquery_future()::text AS value;

-------------------------------------------------------------------------------
-- EC03: EXISTS subquery referencing future table in plpgsql body
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ec03_exists_future(p_id integer)
RETURNS boolean
LANGUAGE plpgsql
AS $fn$
DECLARE
  v boolean;
BEGIN
  SELECT EXISTS (SELECT 1 FROM ec03_items WHERE id = p_id) INTO v;
  RETURN v;
END
$fn$;

-- expect-error: 42P01
SELECT ec03_exists_future(1);

CREATE TABLE ec03_items (
  id integer PRIMARY KEY
);

INSERT INTO ec03_items(id) VALUES (1), (2);

-- begin-expected
-- columns: found
-- row: true
-- end-expected
SELECT ec03_exists_future(1)::text AS found;

-- begin-expected
-- columns: found
-- row: false
-- end-expected
SELECT ec03_exists_future(99)::text AS found;

-------------------------------------------------------------------------------
-- EC04: UPDATE with subquery referencing future table in plpgsql body
-------------------------------------------------------------------------------
CREATE TABLE ec04_targets (
  id integer PRIMARY KEY,
  label text NOT NULL
);

INSERT INTO ec04_targets(id, label) VALUES (1, 'before');

CREATE OR REPLACE FUNCTION ec04_update_from_future()
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
  UPDATE ec04_targets
  SET label = (SELECT name FROM ec04_source WHERE ec04_source.id = ec04_targets.id)
  WHERE id = 1;
END
$fn$;

-- expect-error: 42P01
SELECT ec04_update_from_future();

CREATE TABLE ec04_source (
  id integer PRIMARY KEY,
  name text NOT NULL
);

INSERT INTO ec04_source(id, name) VALUES (1, 'after');

SELECT ec04_update_from_future();

-- begin-expected
-- columns: label
-- row: after
-- end-expected
SELECT label FROM ec04_targets WHERE id = 1;

-------------------------------------------------------------------------------
-- EC05: DELETE with IN-subquery referencing future table in plpgsql body
-------------------------------------------------------------------------------
CREATE TABLE ec05_data (
  id integer PRIMARY KEY,
  status text NOT NULL
);

INSERT INTO ec05_data(id, status) VALUES (1, 'keep'), (2, 'remove'), (3, 'remove');

CREATE OR REPLACE FUNCTION ec05_delete_flagged()
RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v bigint;
BEGIN
  DELETE FROM ec05_data
  WHERE id IN (SELECT item_id FROM ec05_removals);
  GET DIAGNOSTICS v = ROW_COUNT;
  RETURN v;
END
$fn$;

-- expect-error: 42P01
SELECT ec05_delete_flagged();

CREATE TABLE ec05_removals (
  item_id integer PRIMARY KEY
);

INSERT INTO ec05_removals(item_id) VALUES (2), (3);

-- begin-expected
-- columns: deleted
-- row: 2
-- end-expected
SELECT ec05_delete_flagged()::text AS deleted;

-- begin-expected
-- columns: remaining
-- row: 1
-- end-expected
SELECT count(*)::text AS remaining FROM ec05_data;

-------------------------------------------------------------------------------
-- EC06: INSERT ... ON CONFLICT with forward reference
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ec06_upsert(p_id integer, p_name text)
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  INSERT INTO ec06_items(id, name) VALUES (p_id, p_name)
  ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
  RETURNING name INTO v;
  RETURN v;
END
$fn$;

-- expect-error: 42P01
SELECT ec06_upsert(1, 'first');

CREATE TABLE ec06_items (
  id integer PRIMARY KEY,
  name text NOT NULL
);

-- begin-expected
-- columns: value
-- row: alpha
-- end-expected
SELECT ec06_upsert(1, 'alpha') AS value;

-- begin-expected
-- columns: value
-- row: beta
-- end-expected
SELECT ec06_upsert(1, 'beta') AS value;

-- begin-expected
-- columns: name
-- row: beta
-- end-expected
SELECT name FROM ec06_items WHERE id = 1;

-------------------------------------------------------------------------------
-- EC07: UNION query referencing future table in plpgsql body
-------------------------------------------------------------------------------
CREATE TABLE ec07_existing (
  val text NOT NULL
);

INSERT INTO ec07_existing(val) VALUES ('from_existing');

CREATE OR REPLACE FUNCTION ec07_union_future()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  SELECT string_agg(val, ',' ORDER BY val) INTO v
  FROM (
    SELECT val FROM ec07_existing
    UNION ALL
    SELECT val FROM ec07_future
  ) combined;
  RETURN v;
END
$fn$;

-- expect-error: 42P01
SELECT ec07_union_future();

CREATE TABLE ec07_future (
  val text NOT NULL
);

INSERT INTO ec07_future(val) VALUES ('from_future');

-- begin-expected
-- columns: value
-- row: from_existing,from_future
-- end-expected
SELECT ec07_union_future() AS value;

-------------------------------------------------------------------------------
-- EC08: ALTER TABLE column type change invalidates cached plan
-------------------------------------------------------------------------------
CREATE TABLE ec08_data (
  id integer PRIMARY KEY,
  value integer NOT NULL
);

INSERT INTO ec08_data(id, value) VALUES (1, 42);

CREATE OR REPLACE FUNCTION ec08_read_value()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  SELECT value::text INTO v FROM ec08_data WHERE id = 1;
  RETURN v;
END
$fn$;

-- begin-expected
-- columns: result
-- row: 42
-- end-expected
SELECT ec08_read_value() AS result;

ALTER TABLE ec08_data ALTER COLUMN value TYPE text;

UPDATE ec08_data SET value = 'changed' WHERE id = 1;

-- begin-expected
-- columns: result
-- row: changed
-- end-expected
SELECT ec08_read_value() AS result;

-------------------------------------------------------------------------------
-- EC09: Temp table shadows permanent table
-------------------------------------------------------------------------------
CREATE TABLE ec09_items (
  id integer PRIMARY KEY,
  source text NOT NULL
);

INSERT INTO ec09_items(id, source) VALUES (1, 'permanent');

CREATE OR REPLACE FUNCTION ec09_read_source()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  SELECT source INTO v FROM ec09_items WHERE id = 1;
  RETURN v;
END
$fn$;

-- begin-expected
-- columns: value
-- row: permanent
-- end-expected
SELECT ec09_read_source() AS value;

CREATE TEMP TABLE ec09_items (
  id integer PRIMARY KEY,
  source text NOT NULL
);

INSERT INTO ec09_items(id, source) VALUES (1, 'temporary');

-- begin-expected
-- columns: value
-- row: temporary
-- end-expected
SELECT ec09_read_source() AS value;

DROP TABLE pg_temp.ec09_items;

-- begin-expected
-- columns: value
-- row: permanent
-- end-expected
SELECT ec09_read_source() AS value;

-------------------------------------------------------------------------------
-- EC10: Nested function chain with forward references
--       A -> B -> C, all created before the table they ultimately need
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ec10_outer()
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN ec10_middle();
END
$fn$;

CREATE OR REPLACE FUNCTION ec10_middle()
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN ec10_inner();
END
$fn$;

CREATE OR REPLACE FUNCTION ec10_inner()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v text;
BEGIN
  SELECT name INTO v FROM ec10_data WHERE id = 1;
  RETURN v;
END
$fn$;

-- expect-error: 42P01
SELECT ec10_outer();

CREATE TABLE ec10_data (
  id integer PRIMARY KEY,
  name text NOT NULL
);

INSERT INTO ec10_data(id, name) VALUES (1, 'nested-ok');

-- begin-expected
-- columns: value
-- row: nested-ok
-- end-expected
SELECT ec10_outer() AS value;

-------------------------------------------------------------------------------
-- EC11: SQL-language function rejects CTE with missing table at CREATE time
-------------------------------------------------------------------------------
-- expect-error: 42P01
CREATE OR REPLACE FUNCTION ec11_sql_cte_missing()
RETURNS text
LANGUAGE sql
AS $fn$
  WITH cte AS (SELECT name FROM ec11_no_such_table)
  SELECT name FROM cte LIMIT 1
$fn$;

-------------------------------------------------------------------------------
-- EC12: SQL-language function rejects UNION with missing table at CREATE time
-------------------------------------------------------------------------------
CREATE TABLE ec12_existing (val text);

-- expect-error: 42P01
CREATE OR REPLACE FUNCTION ec12_sql_union_missing()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT val FROM ec12_existing
  UNION ALL
  SELECT val FROM ec12_no_such_table
  LIMIT 1
$fn$;

-------------------------------------------------------------------------------
-- EC13: plpgsql RETURN QUERY with forward reference
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ec13_return_query_future()
RETURNS SETOF text
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN QUERY SELECT name FROM ec13_items ORDER BY name;
END
$fn$;

-- expect-error: 42P01
SELECT * FROM ec13_return_query_future();

CREATE TABLE ec13_items (
  id integer PRIMARY KEY,
  name text NOT NULL
);

INSERT INTO ec13_items(id, name) VALUES (1, 'gamma'), (2, 'delta');

-- begin-expected
-- columns: ec13_return_query_future
-- row: delta
-- row: gamma
-- end-expected
SELECT * FROM ec13_return_query_future();

-------------------------------------------------------------------------------
-- EC14: Exception handler referencing future table
-------------------------------------------------------------------------------
CREATE TABLE ec14_log (
  msg text NOT NULL
);

CREATE OR REPLACE FUNCTION ec14_with_exception()
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  INSERT INTO ec14_main(id) VALUES (1);
  RETURN 'ok';
EXCEPTION
  WHEN undefined_table THEN
    INSERT INTO ec14_log(msg) VALUES ('caught undefined_table');
    RETURN 'caught';
END
$fn$;

-- The main table doesn't exist, so the INSERT raises undefined_table,
-- which the EXCEPTION handler catches.
-- begin-expected
-- columns: value
-- row: caught
-- end-expected
SELECT ec14_with_exception() AS value;

-- begin-expected
-- columns: msg
-- row: caught undefined_table
-- end-expected
SELECT msg FROM ec14_log;

CREATE TABLE ec14_main (
  id integer PRIMARY KEY
);

-- Now that ec14_main exists, the function succeeds normally
-- begin-expected
-- columns: value
-- row: ok
-- end-expected
SELECT ec14_with_exception() AS value;
