DROP SCHEMA IF EXISTS test_500 CASCADE;
CREATE SCHEMA test_500;
SET search_path TO test_500;

CREATE TABLE projects (
    project_id integer PRIMARY KEY,
    name text NOT NULL,
    archived_at timestamp,
    created_at timestamp NOT NULL
);

CREATE TABLE tasks (
    task_id integer PRIMARY KEY,
    project_id integer NOT NULL REFERENCES projects(project_id),
    status text NOT NULL,
    assignee_id integer,
    created_at timestamp NOT NULL,
    completed_at timestamp
);

CREATE TABLE notifications (
    notification_id integer PRIMARY KEY,
    user_id integer NOT NULL,
    read_at timestamp,
    created_at timestamp NOT NULL
);

INSERT INTO projects VALUES
    (1, 'alpha', NULL, TIMESTAMP '2024-01-01 09:00:00'),
    (2, 'beta',  NULL, TIMESTAMP '2024-01-02 09:00:00'),
    (3, 'gamma', TIMESTAMP '2024-01-07 00:00:00', TIMESTAMP '2024-01-03 09:00:00');

INSERT INTO tasks VALUES
    (1, 1, 'open',    10, TIMESTAMP '2024-01-01 10:00:00', NULL),
    (2, 1, 'open',    11, TIMESTAMP '2024-01-01 11:00:00', NULL),
    (3, 1, 'done',    10, TIMESTAMP '2024-01-02 09:00:00', TIMESTAMP '2024-01-03 09:00:00'),
    (4, 2, 'open',    12, TIMESTAMP '2024-01-04 09:00:00', NULL),
    (5, 2, 'done',    12, TIMESTAMP '2024-01-04 10:00:00', TIMESTAMP '2024-01-05 10:00:00'),
    (6, 3, 'open',    13, TIMESTAMP '2024-01-05 09:00:00', NULL);

INSERT INTO notifications VALUES
    (1, 10, NULL, TIMESTAMP '2024-01-05 08:00:00'),
    (2, 10, TIMESTAMP '2024-01-05 09:00:00', TIMESTAMP '2024-01-05 08:05:00'),
    (3, 10, NULL, TIMESTAMP '2024-01-06 08:00:00'),
    (4, 11, NULL, TIMESTAMP '2024-01-05 08:10:00');

-- begin-expected
-- columns: total_projects|active_projects|archived_projects
-- row: 3|2|1
-- end-expected
SELECT COUNT(*) AS total_projects,
       COUNT(*) FILTER (WHERE archived_at IS NULL) AS active_projects,
       COUNT(*) FILTER (WHERE archived_at IS NOT NULL) AS archived_projects
FROM projects;

-- begin-expected
-- columns: project_id|name|task_count|open_task_count|latest_task_created_at
-- row: 1|alpha|3|2|2024-01-02 09:00:00
-- row: 2|beta|2|1|2024-01-04 10:00:00
-- row: 3|gamma|1|1|2024-01-05 09:00:00
-- end-expected
SELECT p.project_id,
       p.name,
       (SELECT COUNT(*) FROM tasks t WHERE t.project_id = p.project_id) AS task_count,
       (SELECT COUNT(*) FROM tasks t WHERE t.project_id = p.project_id AND t.status = 'open') AS open_task_count,
       (SELECT MAX(t.created_at) FROM tasks t WHERE t.project_id = p.project_id) AS latest_task_created_at
FROM projects p
ORDER BY p.project_id;

-- begin-expected
-- columns: user_id|unread_count
-- row: 10|2
-- row: 11|1
-- end-expected
SELECT user_id, COUNT(*) AS unread_count
FROM notifications
WHERE read_at IS NULL
GROUP BY user_id
ORDER BY user_id;

-- begin-expected
-- columns: project_id|name|is_busy
-- row: 1|alpha|t
-- row: 2|beta|f
-- row: 3|gamma|f
-- end-expected
SELECT p.project_id,
       p.name,
       ((SELECT COUNT(*) FROM tasks t WHERE t.project_id = p.project_id AND t.status = 'open') >= 2) AS is_busy
FROM projects p
ORDER BY p.project_id;

-- begin-expected
-- columns: project_count_page
-- row: 2
-- end-expected
SELECT COUNT(*) AS project_count_page
FROM (
    SELECT p.project_id
    FROM projects p
    WHERE p.archived_at IS NULL
    ORDER BY p.created_at, p.project_id
    LIMIT 2
) AS page_ids;

DROP SCHEMA test_500 CASCADE;
