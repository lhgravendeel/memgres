DROP SCHEMA IF EXISTS test_1010 CASCADE;
CREATE SCHEMA test_1010;
SET search_path TO test_1010;

CREATE TABLE tasks (
    task_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    title text NOT NULL,
    status text NOT NULL
);

CREATE TABLE archived_tasks (
    archive_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id integer NOT NULL,
    title text NOT NULL,
    archived_from_status text NOT NULL
);

INSERT INTO tasks(title, status) VALUES
('one', 'open'),
('two', 'done'),
('three', 'done');

-- begin-expected
-- columns: task_id,title,status
-- row: 4|four|open
-- end-expected
INSERT INTO tasks(title, status)
VALUES ('four', 'open')
RETURNING task_id, title, status;

-- begin-expected
-- columns: archived_count
-- row: 2
-- end-expected
WITH deleted AS (
    DELETE FROM tasks
    WHERE status = 'done'
    RETURNING task_id, title, status
), archived AS (
    INSERT INTO archived_tasks(task_id, title, archived_from_status)
    SELECT task_id, title, status
    FROM deleted
    RETURNING task_id
)
SELECT COUNT(*) AS archived_count
FROM archived;

-- begin-expected
-- columns: updated_count
-- row: 2
-- end-expected
WITH updated AS (
    UPDATE tasks
    SET status = 'in_progress'
    WHERE title IN ('one', 'four')
    RETURNING task_id, title, status
)
SELECT COUNT(*) AS updated_count
FROM updated;

-- begin-expected
-- columns: task_id,title,status
-- row: 1|one|in_progress
-- row: 4|four|in_progress
-- end-expected
SELECT task_id, title, status
FROM tasks
ORDER BY task_id;

-- begin-expected
-- columns: task_id,title,archived_from_status
-- row: 2|two|done
-- row: 3|three|done
-- end-expected
SELECT task_id, title, archived_from_status
FROM archived_tasks
ORDER BY task_id;

