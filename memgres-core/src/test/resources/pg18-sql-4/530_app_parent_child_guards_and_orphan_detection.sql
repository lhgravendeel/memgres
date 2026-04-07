DROP SCHEMA IF EXISTS test_530 CASCADE;
CREATE SCHEMA test_530;
SET search_path TO test_530;

CREATE TABLE projects (
    project_id integer PRIMARY KEY,
    name text NOT NULL,
    archived boolean NOT NULL DEFAULT false
);

CREATE TABLE tasks (
    task_id integer PRIMARY KEY,
    project_id integer,
    status text NOT NULL
);

INSERT INTO projects VALUES
    (1, 'alpha', false),
    (2, 'beta', false),
    (3, 'gamma', false);

INSERT INTO tasks VALUES
    (1, 1, 'open'),
    (2, 1, 'done'),
    (3, 2, 'done'),
    (4, NULL, 'open'),
    (5, 99, 'open');

-- begin-expected
-- columns: project_id|can_archive
-- row: 1|f
-- row: 2|t
-- row: 3|t
-- end-expected
SELECT p.project_id,
       NOT EXISTS (
           SELECT 1
           FROM tasks t
           WHERE t.project_id = p.project_id
             AND t.status = 'open'
       ) AS can_archive
FROM projects p
ORDER BY p.project_id;

UPDATE projects p
SET archived = true
WHERE p.project_id = 2
  AND NOT EXISTS (
      SELECT 1
      FROM tasks t
      WHERE t.project_id = p.project_id
        AND t.status = 'open'
  );

-- begin-expected
-- columns: project_id|archived
-- row: 1|f
-- row: 2|t
-- row: 3|f
-- end-expected
SELECT project_id, archived
FROM projects
ORDER BY project_id;

-- begin-expected
-- columns: task_id|project_id
-- row: 4|
-- row: 5|99
-- end-expected
SELECT t.task_id, t.project_id
FROM tasks t
LEFT JOIN projects p ON p.project_id = t.project_id
WHERE p.project_id IS NULL
ORDER BY t.task_id;

-- begin-expected
-- columns: project_id|task_count
-- row: 1|2
-- row: 2|1
-- row: 3|0
-- end-expected
SELECT p.project_id, COUNT(t.task_id) AS task_count
FROM projects p
LEFT JOIN tasks t ON t.project_id = p.project_id
GROUP BY p.project_id
ORDER BY p.project_id;

DROP SCHEMA test_530 CASCADE;
