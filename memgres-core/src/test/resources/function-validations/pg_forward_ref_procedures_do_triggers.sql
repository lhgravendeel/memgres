-- pg_forward_ref_procedures_do_triggers.sql
--
-- Purpose:
--   Additional coverage for CREATE PROCEDURE / CALL, DO blocks, trigger
--   functions, NEW/OLD references, trigger attachment timing, and row-shape
--   changes after first success.

DROP SCHEMA IF EXISTS test_forward_ref_proc CASCADE;
CREATE SCHEMA test_forward_ref_proc;
SET search_path = test_forward_ref_proc, public;

-------------------------------------------------------------------------------
-- P01: plpgsql procedure can be created before referenced table exists
-------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE p01_insert_queue_server(p_server_uuid text, p_archived boolean)
LANGUAGE plpgsql
AS $proc$
BEGIN
  INSERT INTO queue_servers(server_uuid, archived)
  VALUES (p_server_uuid, p_archived);
END
$proc$;

-- begin-expected
-- columns: routine_name,routine_type
-- row: p01_insert_queue_server|PROCEDURE
-- end-expected
SELECT routine_name, routine_type
FROM information_schema.routines
WHERE routine_schema = 'test_forward_ref_proc'
  AND routine_name = 'p01_insert_queue_server';

-- expect-error: 42P01
CALL p01_insert_queue_server('before-create', true);

CREATE TABLE queue_servers (
  server_uuid text PRIMARY KEY,
  archived boolean NOT NULL DEFAULT false
);

CALL p01_insert_queue_server('after-create', true);

-- begin-expected
-- columns: archived_count
-- row: 1
-- end-expected
SELECT count(*)::text AS archived_count
FROM queue_servers
WHERE archived;

-------------------------------------------------------------------------------
-- P02: SQL-language procedure validates referenced table at CREATE time
-------------------------------------------------------------------------------
DROP PROCEDURE p01_insert_queue_server(text, boolean);
DROP TABLE queue_servers;

-- expect-error: 42P01
CREATE OR REPLACE PROCEDURE p02_insert_queue_server_sql(p_server_uuid text)
LANGUAGE sql
AS $proc$
  INSERT INTO queue_servers(server_uuid) VALUES (p_server_uuid)
$proc$;

CREATE TABLE queue_servers (
  server_uuid text PRIMARY KEY
);

CREATE OR REPLACE PROCEDURE p02_insert_queue_server_sql(p_server_uuid text)
LANGUAGE sql
AS $proc$
  INSERT INTO queue_servers(server_uuid) VALUES (p_server_uuid)
$proc$;

CALL p02_insert_queue_server_sql('sql-proc');

-- begin-expected
-- columns: server_uuid
-- row: sql-proc
-- end-expected
SELECT server_uuid
FROM queue_servers;

-------------------------------------------------------------------------------
-- P03: DO block static SQL validates object existence at execution time
-------------------------------------------------------------------------------
DROP TABLE queue_servers;

-- expect-error: 42P01
DO $do$
DECLARE
  v_count bigint;
BEGIN
  SELECT count(*) INTO v_count FROM queue_servers;
END
$do$ LANGUAGE plpgsql;

CREATE TABLE queue_servers (
  id integer PRIMARY KEY
);

INSERT INTO queue_servers(id) VALUES (1), (2), (3);

DO $do$
DECLARE
  v_count bigint;
BEGIN
  SELECT count(*) INTO v_count FROM queue_servers;
  IF v_count <> 3 THEN
    RAISE EXCEPTION 'unexpected count: %', v_count;
  END IF;
END
$do$ LANGUAGE plpgsql;

-- begin-expected
-- columns: row_count
-- row: 3
-- end-expected
SELECT count(*)::text AS row_count
FROM queue_servers;

-------------------------------------------------------------------------------
-- P04: DO block dynamic EXECUTE defers syntax errors until execution
-------------------------------------------------------------------------------
-- expect-error: 42601
DO $do$
BEGIN
  EXECUTE 'SELECT FROM definitely_bad_syntax';
END
$do$ LANGUAGE plpgsql;

-------------------------------------------------------------------------------
-- P05: trigger function can be created before target table exists
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION p05_set_default_status()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
BEGIN
  IF NEW.status IS NULL THEN
    NEW.status := 'queued';
  END IF;
  RETURN NEW;
END
$fn$;

CREATE TABLE p05_tasks (
  task_id integer PRIMARY KEY,
  title text NOT NULL,
  status text
);

CREATE TRIGGER p05_tasks_bi
BEFORE INSERT ON p05_tasks
FOR EACH ROW
EXECUTE FUNCTION p05_set_default_status();

INSERT INTO p05_tasks(task_id, title, status)
VALUES
  (1, 'one', NULL),
  (2, 'two', 'running');

-- begin-expected
-- columns: task_id,title,status
-- row: 1|one|queued
-- row: 2|two|running
-- end-expected
SELECT task_id::text AS task_id, title, status
FROM p05_tasks
ORDER BY task_id;

-------------------------------------------------------------------------------
-- P06: trigger body using NEW.column fails after column rename/drop
-------------------------------------------------------------------------------
ALTER TABLE p05_tasks RENAME COLUMN status TO task_status;

-- expect-error: 42703
INSERT INTO p05_tasks(task_id, title)
VALUES (3, 'three');

CREATE OR REPLACE FUNCTION p05_set_default_status()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
BEGIN
  IF NEW.task_status IS NULL THEN
    NEW.task_status := 'queued';
  END IF;
  RETURN NEW;
END
$fn$;

INSERT INTO p05_tasks(task_id, title)
VALUES (3, 'three');

-- begin-expected
-- columns: task_id,title,task_status
-- row: 1|one|queued
-- row: 2|two|running
-- row: 3|three|queued
-- end-expected
SELECT task_id::text AS task_id, title, task_status
FROM p05_tasks
ORDER BY task_id;

-------------------------------------------------------------------------------
-- P07: OLD references in AFTER DELETE trigger still work after initial success
-------------------------------------------------------------------------------
CREATE TABLE p07_task_delete_audit (
  task_id integer NOT NULL,
  title text NOT NULL
);

CREATE OR REPLACE FUNCTION p07_audit_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
BEGIN
  INSERT INTO p07_task_delete_audit(task_id, title)
  VALUES (OLD.task_id, OLD.title);
  RETURN OLD;
END
$fn$;

CREATE TRIGGER p07_tasks_ad
AFTER DELETE ON p05_tasks
FOR EACH ROW
EXECUTE FUNCTION p07_audit_delete();

DELETE FROM p05_tasks WHERE task_id = 2;

-- begin-expected
-- columns: task_id,title
-- row: 2|two
-- end-expected
SELECT task_id::text AS task_id, title
FROM p07_task_delete_audit
ORDER BY task_id;

-------------------------------------------------------------------------------
-- P08: statement-level trigger attachment still requires the table to exist
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION p08_stmt_notice()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
BEGIN
  RETURN NULL;
END
$fn$;

-- expect-error: 42P01
CREATE TRIGGER p08_missing_table_trigger
AFTER INSERT ON p08_no_such_table
FOR EACH STATEMENT
EXECUTE FUNCTION p08_stmt_notice();
