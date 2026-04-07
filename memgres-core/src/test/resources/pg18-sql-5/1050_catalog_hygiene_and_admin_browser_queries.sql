DROP SCHEMA IF EXISTS test_1050 CASCADE;
CREATE SCHEMA test_1050;
SET search_path TO test_1050;

CREATE TABLE with_pk (
    id integer PRIMARY KEY,
    name text
);

CREATE TABLE without_pk (
    id integer,
    name text
);

CREATE TABLE parent (
    id integer PRIMARY KEY
);

CREATE TABLE child (
    id integer PRIMARY KEY,
    parent_id integer REFERENCES parent(id)
);

CREATE INDEX child_parent_idx ON child(parent_id);

-- begin-expected
-- columns: relname
-- row: without_pk
-- end-expected
SELECT c.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'test_1050'
  AND c.relkind = 'r'
  AND NOT EXISTS (
      SELECT 1
      FROM pg_constraint con
      WHERE con.conrelid = c.oid
        AND con.contype = 'p'
  )
ORDER BY c.relname;

-- begin-expected
-- columns: table_name
-- row: child
-- end-expected
SELECT tc.table_name
FROM information_schema.table_constraints tc
WHERE tc.table_schema = 'test_1050'
  AND tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name;

-- begin-expected
-- columns: indexname
-- row: child_parent_idx
-- row: child_pkey
-- end-expected
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'test_1050'
  AND tablename = 'child'
ORDER BY indexname;

