DROP SCHEMA IF EXISTS test_1280 CASCADE;
CREATE SCHEMA test_1280;
SET search_path TO test_1280;

CREATE TABLE projects (
    project_id integer PRIMARY KEY,
    archived boolean NOT NULL
);

CREATE TABLE jobs (
    job_id integer PRIMARY KEY,
    project_id integer NOT NULL,
    active boolean NOT NULL,
    succeeded boolean NOT NULL
);

INSERT INTO projects VALUES
(1, false),
(2, false);

INSERT INTO jobs VALUES
(10, 1, false, true),
(11, 1, false, true),
(20, 2, true, false),
(21, 2, false, true);

-- begin-expected
-- columns: project_id,all_succeeded,any_active
-- row: 1|t|f
-- row: 2|f|t
-- end-expected
SELECT project_id,
       bool_and(succeeded) AS all_succeeded,
       bool_or(active) AS any_active
FROM jobs
GROUP BY project_id
ORDER BY project_id;

-- begin-expected
-- columns: project_id,can_archive
-- row: 1|t
-- row: 2|f
-- end-expected
SELECT p.project_id,
       NOT EXISTS (
         SELECT 1
         FROM jobs j
         WHERE j.project_id = p.project_id
           AND j.active
       ) AS can_archive
FROM projects p
ORDER BY p.project_id;

UPDATE projects p
SET archived = true
WHERE NOT EXISTS (
    SELECT 1
    FROM jobs j
    WHERE j.project_id = p.project_id
      AND j.active
);

-- begin-expected
-- columns: project_id,archived
-- row: 1|t
-- row: 2|f
-- end-expected
SELECT project_id, archived
FROM projects
ORDER BY project_id;

