DROP SCHEMA IF EXISTS test_520 CASCADE;
CREATE SCHEMA test_520;
SET search_path TO test_520;

CREATE TABLE jobs (
    job_id integer PRIMARY KEY,
    status text NOT NULL,
    archived boolean NOT NULL,
    muted boolean NOT NULL,
    worker_id integer,
    started_at timestamp,
    completed_at timestamp
);

INSERT INTO jobs VALUES
    (1, 'pending', false, false, NULL, NULL, NULL),
    (2, 'running', false, false, 50, TIMESTAMP '2024-01-03 09:00:00', NULL),
    (3, 'done',    false, false, 51, TIMESTAMP '2024-01-02 09:00:00', TIMESTAMP '2024-01-02 10:00:00'),
    (4, 'pending', true,  true,  NULL, NULL, NULL);

UPDATE jobs
SET status = 'running', worker_id = 99, started_at = TIMESTAMP '2024-01-10 09:00:00'
WHERE job_id = 1 AND status = 'pending'
RETURNING job_id, status, worker_id, started_at;

-- begin-expected
-- columns: job_id|status|worker_id|started_at
-- row: 1|running|99|2024-01-10 09:00:00
-- end-expected
SELECT job_id, status, worker_id, started_at
FROM jobs
WHERE job_id = 1;

UPDATE jobs
SET status = 'done', completed_at = TIMESTAMP '2024-01-10 09:15:00'
WHERE job_id = 1 AND status = 'running'
RETURNING job_id, status, completed_at;

-- begin-expected
-- columns: job_id|status|completed_at
-- row: 1|done|2024-01-10 09:15:00
-- end-expected
SELECT job_id, status, completed_at
FROM jobs
WHERE job_id = 1;

UPDATE jobs
SET archived = NOT archived
WHERE job_id IN (3, 4);

UPDATE jobs
SET muted = NOT muted
WHERE job_id IN (2, 4);

-- begin-expected
-- columns: job_id|archived|muted
-- row: 2|f|t
-- row: 3|t|f
-- row: 4|f|f
-- end-expected
SELECT job_id, archived, muted
FROM jobs
WHERE job_id IN (2, 3, 4)
ORDER BY job_id;

-- begin-expected
-- columns: updated_rows
-- row: 0
-- end-expected
SELECT COUNT(*) AS updated_rows
FROM (
    UPDATE jobs
    SET status = 'running'
    WHERE job_id = 3 AND status = 'pending'
    RETURNING job_id
) AS guarded_update;

DROP SCHEMA test_520 CASCADE;
