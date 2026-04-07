DROP SCHEMA IF EXISTS test_320 CASCADE;
CREATE SCHEMA test_320;
SET search_path TO test_320;

CREATE TABLE projects (
    tenant_id integer NOT NULL,
    project_id integer NOT NULL,
    slug text NOT NULL,
    deleted_at timestamp,
    PRIMARY KEY (tenant_id, project_id)
);

CREATE TABLE tasks (
    tenant_id integer NOT NULL,
    task_id integer NOT NULL,
    project_id integer NOT NULL,
    title text NOT NULL,
    status text NOT NULL,
    deleted_at timestamp,
    PRIMARY KEY (tenant_id, task_id),
    FOREIGN KEY (tenant_id, project_id)
        REFERENCES projects(tenant_id, project_id)
);

CREATE UNIQUE INDEX uniq_live_project_slug
ON projects(tenant_id, slug)
WHERE deleted_at IS NULL;

INSERT INTO projects(tenant_id, project_id, slug, deleted_at) VALUES
    (1, 101, 'alpha', NULL),
    (1, 102, 'beta',  TIMESTAMP '2024-02-01 00:00:00'),
    (1, 103, 'gamma', NULL),
    (2, 201, 'alpha', NULL),
    (2, 202, 'delta', NULL);

INSERT INTO tasks(tenant_id, task_id, project_id, title, status, deleted_at) VALUES
    (1, 1, 101, 'design',   'open',   NULL),
    (1, 2, 101, 'review',   'done',   NULL),
    (1, 3, 102, 'legacy',   'open',   NULL),
    (1, 4, 103, 'qa',       'open',   TIMESTAMP '2024-02-05 12:00:00'),
    (2, 1, 201, 'import',   'open',   NULL),
    (2, 2, 201, 'cleanup',  'done',   NULL),
    (2, 3, 202, 'ship',     'open',   NULL);

-- begin-expected
-- columns: tenant_id|project_id|slug
-- row: 1|101|alpha
-- row: 1|103|gamma
-- row: 2|201|alpha
-- row: 2|202|delta
-- end-expected
SELECT tenant_id, project_id, slug
FROM projects
WHERE deleted_at IS NULL
ORDER BY tenant_id, project_id;

-- begin-expected
-- columns: tenant_id|project_id|task_count
-- row: 1|101|2
-- row: 1|103|0
-- row: 2|201|2
-- row: 2|202|1
-- end-expected
SELECT p.tenant_id, p.project_id, count(t.task_id) AS task_count
FROM projects AS p
LEFT JOIN tasks AS t
  ON t.tenant_id = p.tenant_id
 AND t.project_id = p.project_id
 AND t.deleted_at IS NULL
WHERE p.deleted_at IS NULL
GROUP BY p.tenant_id, p.project_id
ORDER BY p.tenant_id, p.project_id;

-- begin-expected
-- columns: tenant_id|task_id|title
-- row: 1|1|design
-- row: 2|1|import
-- row: 2|3|ship
-- end-expected
SELECT t.tenant_id, t.task_id, t.title
FROM tasks AS t
JOIN projects AS p
  ON p.tenant_id = t.tenant_id
 AND p.project_id = t.project_id
WHERE t.deleted_at IS NULL
  AND p.deleted_at IS NULL
  AND t.status = 'open'
ORDER BY t.tenant_id, t.task_id;

-- begin-expected
-- columns: tenant_id|project_id|slug|status
-- row: 1|101|alpha|done
-- row: 1|101|alpha|open
-- row: 2|201|alpha|done
-- row: 2|201|alpha|open
-- end-expected
SELECT p.tenant_id, p.project_id, p.slug, t.status
FROM projects AS p
JOIN tasks AS t
  ON t.tenant_id = p.tenant_id
 AND t.project_id = p.project_id
WHERE p.slug = 'alpha'
  AND p.deleted_at IS NULL
  AND t.deleted_at IS NULL
ORDER BY p.tenant_id, p.project_id, t.status;

-- begin-expected
-- columns: tenant_id|project_id
-- row: 1|103
-- end-expected
SELECT p.tenant_id, p.project_id
FROM projects AS p
LEFT JOIN tasks AS t
  ON t.tenant_id = p.tenant_id
 AND t.project_id = p.project_id
 AND t.deleted_at IS NULL
WHERE p.deleted_at IS NULL
GROUP BY p.tenant_id, p.project_id
HAVING count(t.task_id) = 0
ORDER BY p.tenant_id, p.project_id;

DROP SCHEMA test_320 CASCADE;
