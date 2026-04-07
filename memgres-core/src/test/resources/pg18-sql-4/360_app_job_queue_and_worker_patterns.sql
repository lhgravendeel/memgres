DROP SCHEMA IF EXISTS test_360 CASCADE;
CREATE SCHEMA test_360;
SET search_path TO test_360;

CREATE TABLE jobs (
    job_id integer PRIMARY KEY,
    queue_name text NOT NULL,
    state text NOT NULL,
    run_at timestamp NOT NULL,
    attempts integer NOT NULL,
    payload jsonb NOT NULL
);

INSERT INTO jobs(job_id, queue_name, state, run_at, attempts, payload) VALUES
    (1, 'email',   'pending', TIMESTAMP '2024-01-01 09:00:00', 0, '{"to":"a@example.com"}'),
    (2, 'email',   'pending', TIMESTAMP '2024-01-01 09:05:00', 1, '{"to":"b@example.com"}'),
    (3, 'email',   'running', TIMESTAMP '2024-01-01 08:50:00', 1, '{"to":"c@example.com"}'),
    (4, 'billing', 'pending', TIMESTAMP '2024-01-01 09:02:00', 0, '{"invoice":10}'),
    (5, 'email',   'failed',  TIMESTAMP '2024-01-01 08:30:00', 3, '{"to":"d@example.com"}');

-- begin-expected
-- columns: job_id|queue_name|attempts
-- row: 1|email|0
-- row: 4|billing|0
-- row: 2|email|1
-- end-expected
SELECT job_id, queue_name, attempts
FROM jobs
WHERE state = 'pending'
ORDER BY run_at, job_id
LIMIT 3;

WITH next_jobs AS (
    SELECT job_id
    FROM jobs
    WHERE state = 'pending'
      AND queue_name = 'email'
    ORDER BY run_at, job_id
    LIMIT 2
)
UPDATE jobs AS j
SET state = 'running',
    attempts = attempts + 1
FROM next_jobs
WHERE j.job_id = next_jobs.job_id;

-- begin-expected
-- columns: job_id|queue_name|state|attempts
-- row: 1|email|running|1
-- row: 2|email|running|2
-- row: 3|email|running|1
-- row: 4|billing|pending|0
-- row: 5|email|failed|3
-- end-expected
SELECT job_id, queue_name, state, attempts
FROM jobs
ORDER BY job_id;

UPDATE jobs
SET state = 'pending',
    run_at = run_at + INTERVAL '1 hour',
    attempts = attempts + 1
WHERE state = 'failed';

-- begin-expected
-- columns: job_id|state|run_at|attempts
-- row: 5|pending|2024-01-01 09:30:00|4
-- end-expected
SELECT job_id, state, run_at, attempts
FROM jobs
WHERE job_id = 5;

-- begin-expected
-- columns: queue_name|state|cnt
-- row: billing|pending|1
-- row: email|pending|1
-- row: email|running|3
-- end-expected
SELECT queue_name, state, count(*) AS cnt
FROM jobs
GROUP BY queue_name, state
ORDER BY queue_name, state;

DROP SCHEMA test_360 CASCADE;
