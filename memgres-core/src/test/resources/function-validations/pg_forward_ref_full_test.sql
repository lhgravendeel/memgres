-- pg_forward_ref_full_test.sql
--
-- Purpose:
--   Broad regression suite for forward references, deferred validation, and
--   closely related PL/pgSQL behavior in a PostgreSQL-compatible engine.
--
-- Conventions used by the test harness:
--   * A statement may be preceded by:
--       -- expect-error
--       -- expect-error: 42P01
--   * Assertable query results are annotated with:
--       -- begin-expected
--       -- columns: ...
--       -- row: ...
--       -- end-expected

DROP SCHEMA IF EXISTS test_forward_ref CASCADE;
CREATE SCHEMA test_forward_ref;
SET search_path = test_forward_ref, public;

-------------------------------------------------------------------------------
-- T01: plpgsql function can be created before referenced table exists
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_get_first_server_plpgsql()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_server_uuid text;
BEGIN
  SELECT qs.server_uuid
  INTO v_server_uuid
  FROM queue_servers qs
  ORDER BY qs.server_uuid
  LIMIT 1;

  RETURN v_server_uuid;
END
$fn$;

-- begin-expected
-- columns: routine_name
-- row: fn_get_first_server_plpgsql
-- end-expected
SELECT routine_name
FROM information_schema.routines
WHERE routine_schema = 'test_forward_ref'
  AND routine_name = 'fn_get_first_server_plpgsql';

-------------------------------------------------------------------------------
-- T02: first execution before relation exists should fail at runtime
-------------------------------------------------------------------------------
-- expect-error: 42P01
SELECT fn_get_first_server_plpgsql();

-------------------------------------------------------------------------------
-- T03: once the table exists, the same function should execute successfully
-------------------------------------------------------------------------------
CREATE TABLE queue_servers (
  server_uuid text PRIMARY KEY,
  archived boolean NOT NULL DEFAULT false
);

INSERT INTO queue_servers(server_uuid, archived)
VALUES
  ('server-a', false),
  ('server-b', true),
  ('server-c', true);

-- begin-expected
-- columns: first_server_uuid
-- row: server-a
-- end-expected
SELECT fn_get_first_server_plpgsql() AS first_server_uuid;

-- begin-expected
-- columns: archived_count
-- row: 2
-- end-expected
SELECT count(*)::text AS archived_count
FROM queue_servers
WHERE archived;

-------------------------------------------------------------------------------
-- T04: SQL-language function is validated immediately against missing relation
-------------------------------------------------------------------------------
DROP FUNCTION fn_get_first_server_plpgsql();
DROP TABLE queue_servers;

-- expect-error: 42P01
CREATE OR REPLACE FUNCTION fn_get_first_server_sql()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT qs.server_uuid
  FROM queue_servers qs
  ORDER BY qs.server_uuid
  LIMIT 1
$fn$;

-------------------------------------------------------------------------------
-- T05: SQL-language function succeeds once referenced relation exists
-------------------------------------------------------------------------------
CREATE TABLE queue_servers (
  server_uuid text PRIMARY KEY,
  archived boolean NOT NULL DEFAULT false
);

INSERT INTO queue_servers(server_uuid, archived)
VALUES
  ('server-d', false),
  ('server-e', true),
  ('server-f', true);

CREATE OR REPLACE FUNCTION fn_get_first_server_sql()
RETURNS text
LANGUAGE sql
AS $fn$
  SELECT qs.server_uuid
  FROM queue_servers qs
  ORDER BY qs.server_uuid
  LIMIT 1
$fn$;

-- begin-expected
-- columns: first_server_uuid
-- row: server-d
-- end-expected
SELECT fn_get_first_server_sql() AS first_server_uuid;

-------------------------------------------------------------------------------
-- T06: plpgsql function with INSERT ... RETURNING can be created early
-------------------------------------------------------------------------------
DROP FUNCTION fn_get_first_server_sql();
DROP TABLE queue_servers;

CREATE OR REPLACE FUNCTION fn_insert_server_plpgsql(p_server_uuid text, p_archived boolean)
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_server_uuid text;
BEGIN
  INSERT INTO queue_servers(server_uuid, archived)
  VALUES (p_server_uuid, p_archived)
  RETURNING server_uuid INTO v_server_uuid;

  RETURN v_server_uuid;
END
$fn$;

-- expect-error: 42P01
SELECT fn_insert_server_plpgsql('server-x-before-create', true);

CREATE TABLE queue_servers (
  server_uuid text PRIMARY KEY,
  archived boolean NOT NULL DEFAULT false
);

-- begin-expected
-- columns: inserted_server_uuid
-- row: server-x
-- end-expected
SELECT fn_insert_server_plpgsql('server-x', true) AS inserted_server_uuid;

-- begin-expected
-- columns: archived_count
-- row: 1
-- end-expected
SELECT count(*)::text AS archived_count
FROM queue_servers
WHERE archived;

-------------------------------------------------------------------------------
-- T07: duplicate-key errors are not hidden by deferred planning
-------------------------------------------------------------------------------
-- expect-error: 23505
SELECT fn_insert_server_plpgsql('server-x', false);

-------------------------------------------------------------------------------
-- T08: missing column can also be deferred for plpgsql static SQL
-------------------------------------------------------------------------------
DROP FUNCTION fn_insert_server_plpgsql(text, boolean);
DROP TABLE queue_servers;

CREATE OR REPLACE FUNCTION fn_read_missing_column_plpgsql()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_value text;
BEGIN
  SELECT qs.server_label
  INTO v_value
  FROM queue_servers qs
  ORDER BY qs.server_uuid
  LIMIT 1;

  RETURN v_value;
END
$fn$;

CREATE TABLE queue_servers (
  server_uuid text PRIMARY KEY,
  archived boolean NOT NULL DEFAULT false
);

INSERT INTO queue_servers(server_uuid, archived)
VALUES ('server-m', false);

-- expect-error: 42703
SELECT fn_read_missing_column_plpgsql();

ALTER TABLE queue_servers ADD COLUMN server_label text;
UPDATE queue_servers SET server_label = 'label-m' WHERE server_uuid = 'server-m';

-- begin-expected
-- columns: server_label
-- row: label-m
-- end-expected
SELECT fn_read_missing_column_plpgsql() AS server_label;

-------------------------------------------------------------------------------
-- T09: missing type in static SQL can be deferred for plpgsql
-------------------------------------------------------------------------------
DROP FUNCTION fn_read_missing_column_plpgsql();
DROP TABLE queue_servers;

CREATE TABLE queue_servers (
  server_uuid text PRIMARY KEY,
  status_text text NOT NULL
);

INSERT INTO queue_servers(server_uuid, status_text)
VALUES ('server-z', 'ready');

CREATE OR REPLACE FUNCTION fn_cast_to_future_type()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_status text;
BEGIN
  SELECT (qs.status_text::queue_status_enum)::text
  INTO v_status
  FROM queue_servers qs
  WHERE qs.server_uuid = 'server-z';

  RETURN v_status;
END
$fn$;

-- expect-error: 42704
SELECT fn_cast_to_future_type();

CREATE TYPE queue_status_enum AS ENUM ('ready', 'paused');

-- begin-expected
-- columns: status_value
-- row: ready
-- end-expected
SELECT fn_cast_to_future_type() AS status_value;

-------------------------------------------------------------------------------
-- T10: declaration-level missing types are validated immediately
-------------------------------------------------------------------------------
-- expect-error: 42704
CREATE OR REPLACE FUNCTION fn_declares_missing_type()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_missing not_created_type;
BEGIN
  RETURN 'never';
END
$fn$;

-- expect-error: 42704
CREATE OR REPLACE FUNCTION fn_returns_missing_type()
RETURNS not_created_type
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN NULL;
END
$fn$;

-------------------------------------------------------------------------------
-- T11: dynamic SQL defers even parse/analysis until runtime
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_exec_bad_syntax()
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
  EXECUTE 'SELECT FROM';
END
$fn$;

CREATE OR REPLACE FUNCTION fn_exec_missing_table()
RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_count bigint;
BEGIN
  EXECUTE 'SELECT count(*) FROM dyn_future_table' INTO v_count;
  RETURN v_count;
END
$fn$;

-- expect-error: 42601
SELECT fn_exec_bad_syntax();

-- expect-error: 42P01
SELECT fn_exec_missing_table();

CREATE TABLE dyn_future_table (id integer PRIMARY KEY);
INSERT INTO dyn_future_table(id) VALUES (1), (2), (3);

-- begin-expected
-- columns: row_count
-- row: 3
-- end-expected
SELECT fn_exec_missing_table()::text AS row_count;

-------------------------------------------------------------------------------
-- T12: drop/recreate table should not permanently poison plpgsql function
-------------------------------------------------------------------------------
DROP FUNCTION fn_cast_to_future_type();
DROP TYPE queue_status_enum;
DROP TABLE queue_servers;

CREATE OR REPLACE FUNCTION fn_count_servers()
RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_count bigint;
BEGIN
  SELECT count(*) INTO v_count FROM queue_servers;
  RETURN v_count;
END
$fn$;

CREATE TABLE queue_servers (
  server_uuid text PRIMARY KEY
);

INSERT INTO queue_servers(server_uuid) VALUES ('q1'), ('q2');

-- begin-expected
-- columns: row_count
-- row: 2
-- end-expected
SELECT fn_count_servers()::text AS row_count;

DROP TABLE queue_servers;
CREATE TABLE queue_servers (
  server_uuid text PRIMARY KEY
);

INSERT INTO queue_servers(server_uuid) VALUES ('q3'), ('q4'), ('q5');

-- begin-expected
-- columns: row_count
-- row: 3
-- end-expected
SELECT fn_count_servers()::text AS row_count;

-------------------------------------------------------------------------------
-- T13: ALTER TABLE after first execution should invalidate stale assumptions
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_read_server_uuid()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_value text;
BEGIN
  SELECT server_uuid INTO v_value
  FROM queue_servers
  ORDER BY server_uuid
  LIMIT 1;
  RETURN v_value;
END
$fn$;

-- begin-expected
-- columns: first_uuid
-- row: q3
-- end-expected
SELECT fn_read_server_uuid() AS first_uuid;

ALTER TABLE queue_servers RENAME COLUMN server_uuid TO server_key;

-- expect-error: 42703
SELECT fn_read_server_uuid();

ALTER TABLE queue_servers RENAME COLUMN server_key TO server_uuid;

-- begin-expected
-- columns: first_uuid
-- row: q3
-- end-expected
SELECT fn_read_server_uuid() AS first_uuid;

-------------------------------------------------------------------------------
-- T14: exception handling should trap deferred runtime errors by SQLSTATE class
-------------------------------------------------------------------------------
DROP TABLE queue_servers;

CREATE OR REPLACE FUNCTION fn_missing_table_caught()
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_dummy text;
BEGIN
  SELECT server_uuid INTO v_dummy FROM queue_servers LIMIT 1;
  RETURN v_dummy;
EXCEPTION
  WHEN undefined_table THEN
    RETURN 'missing-table';
END
$fn$;

-- begin-expected
-- columns: outcome
-- row: missing-table
-- end-expected
SELECT fn_missing_table_caught() AS outcome;

CREATE TABLE queue_servers (
  server_uuid text PRIMARY KEY
);

INSERT INTO queue_servers(server_uuid) VALUES ('caught-1');

-- begin-expected
-- columns: outcome
-- row: caught-1
-- end-expected
SELECT fn_missing_table_caught() AS outcome;

CREATE OR REPLACE FUNCTION fn_unique_violation_caught()
RETURNS text
LANGUAGE plpgsql
AS $fn$
BEGIN
  INSERT INTO queue_servers(server_uuid) VALUES ('caught-1');
  RETURN 'unexpected';
EXCEPTION
  WHEN unique_violation THEN
    RETURN 'unique-violation';
END
$fn$;

-- begin-expected
-- columns: outcome
-- row: unique-violation
-- end-expected
SELECT fn_unique_violation_caught() AS outcome;

-------------------------------------------------------------------------------
-- T15: dynamic SQL with parameters and generated identifiers
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_dynamic_count(p_table text)
RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_count bigint;
BEGIN
  EXECUTE 'SELECT count(*) FROM ' || quote_ident(p_table) INTO v_count;
  RETURN v_count;
END
$fn$;

CREATE OR REPLACE FUNCTION fn_dynamic_insert_returning(p_table text, p_value text)
RETURNS text
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_out text;
BEGIN
  EXECUTE 'INSERT INTO ' || quote_ident(p_table) || '(server_uuid) VALUES ($1) RETURNING server_uuid'
    INTO v_out
    USING p_value;
  RETURN v_out;
END
$fn$;

-- begin-expected
-- columns: row_count
-- row: 1
-- end-expected
SELECT fn_dynamic_count('queue_servers')::text AS row_count;

-- begin-expected
-- columns: inserted_uuid
-- row: dyn-insert-1
-- end-expected
SELECT fn_dynamic_insert_returning('queue_servers', 'dyn-insert-1') AS inserted_uuid;

-- begin-expected
-- columns: row_count
-- row: 2
-- end-expected
SELECT fn_dynamic_count('queue_servers')::text AS row_count;

-- expect-error: 42P01
SELECT fn_dynamic_count('missing_dyn_table');

-------------------------------------------------------------------------------
-- T16: same-transaction create-function then create-table then execute
-------------------------------------------------------------------------------
DROP FUNCTION fn_missing_table_caught();
DROP FUNCTION fn_unique_violation_caught();
DROP FUNCTION fn_dynamic_count(text);
DROP FUNCTION fn_dynamic_insert_returning(text, text);
DROP FUNCTION fn_count_servers();
DROP FUNCTION fn_read_server_uuid();
DROP FUNCTION fn_exec_bad_syntax();
DROP FUNCTION fn_exec_missing_table();
DROP TABLE queue_servers;
DROP TABLE dyn_future_table;

BEGIN;

CREATE OR REPLACE FUNCTION fn_same_txn_forward_ref()
RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_count bigint;
BEGIN
  SELECT count(*) INTO v_count FROM queue_servers;
  RETURN v_count;
END
$fn$;

CREATE TABLE queue_servers (
  id integer PRIMARY KEY
);

INSERT INTO queue_servers(id) VALUES (10), (20);

-- begin-expected
-- columns: row_count
-- row: 2
-- end-expected
SELECT fn_same_txn_forward_ref()::text AS row_count;

ROLLBACK;

-------------------------------------------------------------------------------
-- T17: after rollback, function and table from rolled-back transaction are gone
-------------------------------------------------------------------------------
-- expect-error: 42883
SELECT fn_same_txn_forward_ref();

-- expect-error: 42P01
SELECT count(*) FROM queue_servers;
