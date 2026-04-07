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

CREATE TABLE smoke_a(
  id int PRIMARY KEY,
  note text NOT NULL DEFAULT 'x',
  qty int,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX smoke_a_note_idx ON smoke_a(note);
CREATE VIEW smoke_v AS SELECT id, note FROM smoke_a;

-- information_schema queries similar to tool/framework introspection
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'compat'
ORDER BY table_name;

SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'compat' AND table_name = 'smoke_a'
ORDER BY ordinal_position;

SELECT constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE table_schema = 'compat' AND table_name = 'smoke_a'
ORDER BY constraint_name;

SELECT kcu.column_name, tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
WHERE tc.table_schema = 'compat' AND tc.table_name = 'smoke_a'
ORDER BY kcu.ordinal_position;

-- pg_catalog queries often used by tooling
SELECT c.relname, c.relkind, n.nspname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'compat'
ORDER BY c.relname;

SELECT a.attname, a.attnum, a.atttypid::regtype
FROM pg_attribute a
WHERE a.attrelid = 'compat.smoke_a'::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;

SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'compat.smoke_a'::regclass
ORDER BY conname;

SELECT i.indexrelid::regclass, i.indisunique, i.indisprimary
FROM pg_index i
WHERE i.indrelid = 'compat.smoke_a'::regclass
ORDER BY i.indexrelid::regclass::text;

SELECT t.typname, t.typtype
FROM pg_type t
WHERE t.typname IN ('int4', 'text')
ORDER BY t.typname;

-- a couple of bad/introspection miss cases
SELECT a.attname
FROM pg_attribute a
WHERE a.attrelid = 'compat.missing_table'::regclass;

SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'compat' AND table_name = 'missing_table'
ORDER BY ordinal_position;

DROP SCHEMA compat CASCADE;
