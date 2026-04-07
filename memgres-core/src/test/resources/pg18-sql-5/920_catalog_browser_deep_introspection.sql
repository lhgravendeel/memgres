DROP SCHEMA IF EXISTS test_920 CASCADE;
CREATE SCHEMA test_920;
SET search_path TO test_920;

CREATE TYPE priority_enum AS ENUM ('low', 'medium', 'high');

CREATE TABLE projects (
    project_id integer PRIMARY KEY,
    priority priority_enum NOT NULL,
    updated_at timestamp
);

CREATE TABLE tasks (
    task_id integer PRIMARY KEY,
    project_id integer REFERENCES projects(project_id),
    updated_at timestamp
);

CREATE FUNCTION project_label(p priority_enum)
RETURNS text
LANGUAGE SQL
AS $$
  SELECT upper(p::text)
$$;

CREATE TRIGGER projects_noop_trigger
BEFORE UPDATE ON projects
FOR EACH ROW
EXECUTE FUNCTION pg_catalog.suppress_redundant_updates_trigger();

-- begin-expected
-- columns: table_name,column_name
-- row: projects|priority
-- end-expected
SELECT c.table_name, c.column_name
FROM information_schema.columns c
WHERE c.table_schema = 'test_920'
  AND c.udt_name = 'priority_enum'
ORDER BY c.table_name, c.column_name;

-- begin-expected
-- columns: relname
-- row: projects
-- row: tasks
-- end-expected
SELECT relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'test_920'
  AND c.relkind = 'r'
ORDER BY relname;

-- begin-expected
-- columns: tgname
-- row: projects_noop_trigger
-- end-expected
SELECT t.tgname
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'test_920'
  AND c.relname = 'projects'
  AND NOT t.tgisinternal
ORDER BY t.tgname;

-- begin-expected
-- columns: proname
-- row: project_label
-- end-expected
SELECT p.proname
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'test_920'
ORDER BY p.proname;

