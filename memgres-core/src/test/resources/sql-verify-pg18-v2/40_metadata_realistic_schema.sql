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

-- realistic application-ish schema for metadata/introspection tests
CREATE TYPE app_status AS ENUM ('new', 'active', 'disabled');

CREATE TABLE account(
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  email text NOT NULL,
  display_name text,
  status app_status NOT NULL DEFAULT 'new',
  version int NOT NULL DEFAULT 0,
  deleted boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT account_email_uq UNIQUE (email)
);

CREATE TABLE project(
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_id bigint NOT NULL,
  code text NOT NULL,
  title text NOT NULL,
  archived boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT project_account_fk
    FOREIGN KEY (account_id) REFERENCES account(id),
  CONSTRAINT project_code_uq UNIQUE (code)
);

CREATE TABLE task(
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id bigint NOT NULL,
  ext_ref text,
  title text NOT NULL,
  state text NOT NULL DEFAULT 'open',
  priority int NOT NULL DEFAULT 0,
  due_date date,
  created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT task_project_fk
    FOREIGN KEY (project_id) REFERENCES project(id)
);

CREATE INDEX task_project_idx ON task(project_id);
CREATE INDEX task_open_priority_idx ON task(priority DESC) WHERE state = 'open';

CREATE VIEW task_summary AS
SELECT t.id, p.code AS project_code, t.title, t.state, t.priority
FROM task t
JOIN project p ON p.id = t.project_id;

INSERT INTO account(email, display_name, status)
VALUES ('a@example.com', 'Ann', 'active');

INSERT INTO project(account_id, code, title)
VALUES (1, 'P1', 'Project One');

INSERT INTO task(project_id, ext_ref, title, state, priority)
VALUES
(1, 'EXT-1', 'First', 'open', 10),
(1, 'EXT-2', 'Second', 'done', 5);

SELECT * FROM task_summary ORDER BY id;

-- information_schema + pg_catalog checks over a more realistic schema
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'compat'
ORDER BY table_name;

SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'compat' AND table_name IN ('account', 'project', 'task')
ORDER BY table_name, ordinal_position;

SELECT constraint_name, table_name, constraint_type
FROM information_schema.table_constraints
WHERE table_schema = 'compat'
ORDER BY table_name, constraint_name;

SELECT c.relname, c.relkind, n.nspname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'compat'
ORDER BY c.relname;

SELECT a.attname, a.attnum, a.atttypid::regtype
FROM pg_attribute a
WHERE a.attrelid = 'compat.task'::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;

SELECT conname, contype
FROM pg_constraint
WHERE conrelid IN ('compat.account'::regclass, 'compat.project'::regclass, 'compat.task'::regclass)
ORDER BY conname;

SELECT i.indexrelid::regclass, i.indisunique, i.indisprimary
FROM pg_index i
WHERE i.indrelid = 'compat.task'::regclass
ORDER BY i.indexrelid::regclass::text;

SELECT t.typname, t.typtype
FROM pg_type t
WHERE t.typname IN ('app_status', 'text', 'int4')
ORDER BY t.typname;

DROP SCHEMA compat CASCADE;
