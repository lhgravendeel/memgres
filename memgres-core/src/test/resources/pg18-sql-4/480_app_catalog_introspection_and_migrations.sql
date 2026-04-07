-- 480_app_catalog_introspection_and_migrations.sql
-- Common catalog/introspection queries seen in migration systems and admin tooling.

DROP SCHEMA IF EXISTS test_480 CASCADE;
CREATE SCHEMA test_480;
SET search_path TO test_480;

CREATE TABLE widgets (
    id integer PRIMARY KEY,
    code text NOT NULL UNIQUE,
    status text NOT NULL DEFAULT 'active'
);

CREATE INDEX widgets_status_idx ON widgets(status);

CREATE TYPE widget_kind AS ENUM ('small', 'medium', 'large');

ALTER TABLE widgets ADD COLUMN kind widget_kind NOT NULL DEFAULT 'small';

-- information_schema existence query.
-- begin-expected
-- columns: column_name,data_type,is_nullable
-- row: code|text|NO
-- row: id|integer|NO
-- row: kind|USER-DEFINED|NO
-- row: status|text|NO
-- end-expected
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'test_480'
  AND table_name = 'widgets'
ORDER BY column_name;

-- pg_catalog index inspection.
-- begin-expected
-- columns: index_name,is_unique,is_primary
-- row: widgets_code_key|t|f
-- row: widgets_pkey|t|t
-- row: widgets_status_idx|f|f
-- end-expected
SELECT
    i.relname AS index_name,
    ix.indisunique AS is_unique,
    ix.indisprimary AS is_primary
FROM pg_class t
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_index ix ON ix.indrelid = t.oid
JOIN pg_class i ON i.oid = ix.indexrelid
WHERE n.nspname = 'test_480'
  AND t.relname = 'widgets'
ORDER BY index_name;

-- Enum label discovery.
-- begin-expected
-- columns: enumlabel
-- row: large
-- row: medium
-- row: small
-- end-expected
SELECT e.enumlabel
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
JOIN pg_enum e ON e.enumtypid = t.oid
WHERE n.nspname = 'test_480'
  AND t.typname = 'widget_kind'
ORDER BY e.enumlabel;

-- regclass / to_regclass existence checks used in migrations.
-- begin-expected
-- columns: widgets_regclass,status_idx_exists,missing_table_exists
-- row: widgets|widgets_status_idx|
-- end-expected
SELECT
    to_regclass('test_480.widgets') AS widgets_regclass,
    to_regclass('test_480.widgets_status_idx') AS status_idx_exists,
    to_regclass('test_480.no_such_table') AS missing_table_exists;

-- Constraint discovery from catalog.
-- begin-expected
-- columns: conname,contype
-- row: widgets_code_key|u
-- row: widgets_code_not_null|n
-- row: widgets_id_not_null|n
-- row: widgets_kind_not_null|n
-- row: widgets_pkey|p
-- row: widgets_status_not_null|n
-- end-expected
SELECT conname, contype
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'test_480'
  AND t.relname = 'widgets'
ORDER BY conname;
