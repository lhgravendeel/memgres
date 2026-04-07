DROP SCHEMA IF EXISTS test_850 CASCADE;
CREATE SCHEMA test_850;
SET search_path TO test_850;

CREATE TABLE accounts (
    account_id integer PRIMARY KEY,
    updated_at timestamp
);

CREATE TABLE projects (
    project_id integer PRIMARY KEY,
    account_id integer REFERENCES accounts(account_id),
    updated_at timestamp
);

CREATE TABLE comments (
    comment_id integer PRIMARY KEY,
    project_id integer REFERENCES projects(project_id),
    body text
);

-- begin-expected
-- columns: table_name
-- row: accounts
-- row: projects
-- end-expected
SELECT table_name
FROM information_schema.columns
WHERE table_schema = 'test_850'
  AND column_name = 'updated_at'
ORDER BY table_name;

-- begin-expected
-- columns: table_name,constraint_name
-- row: comments|comments_project_id_fkey
-- row: projects|projects_account_id_fkey
-- end-expected
SELECT tc.table_name, tc.constraint_name
FROM information_schema.table_constraints tc
WHERE tc.table_schema = 'test_850'
  AND tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name, tc.constraint_name;

-- begin-expected
-- columns: table_name
-- row: accounts
-- row: comments
-- row: projects
-- end-expected
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'test_850'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;

