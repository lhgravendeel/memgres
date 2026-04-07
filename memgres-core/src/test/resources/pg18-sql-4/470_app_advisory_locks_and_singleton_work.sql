-- 470_app_advisory_locks_and_singleton_work.sql
-- Advisory lock query patterns used by migrations, schedulers, and singleton jobs.

DROP SCHEMA IF EXISTS test_470 CASCADE;
CREATE SCHEMA test_470;
SET search_path TO test_470;

CREATE TABLE singleton_jobs (
    id integer PRIMARY KEY,
    job_name text NOT NULL UNIQUE,
    last_started_at timestamp NULL,
    last_finished_at timestamp NULL,
    run_count integer NOT NULL DEFAULT 0
);

INSERT INTO singleton_jobs(id, job_name, last_started_at, last_finished_at, run_count) VALUES
(1, 'nightly-maintenance', NULL, NULL, 0),
(2, 'billing-rollup', TIMESTAMP '2025-03-01 00:00:00', TIMESTAMP '2025-03-01 00:10:00', 5);

-- Advisory lock acquisition should return a boolean.
-- begin-expected
-- columns: got_lock
-- row: t
-- end-expected
SELECT pg_try_advisory_lock(470001) AS got_lock;

-- Re-acquiring in the same session succeeds.
-- begin-expected
-- columns: got_lock_again
-- row: t
-- end-expected
SELECT pg_try_advisory_lock(470001) AS got_lock_again;

-- Start singleton work only when lock is held.
UPDATE singleton_jobs
SET last_started_at = TIMESTAMP '2025-03-02 00:00:00',
    run_count = run_count + 1
WHERE job_name = 'nightly-maintenance';

-- begin-expected
-- columns: job_name,run_count,last_started_at
-- row: nightly-maintenance|1|2025-03-02 00:00:00
-- end-expected
SELECT job_name, run_count, last_started_at
FROM singleton_jobs
WHERE job_name = 'nightly-maintenance';

-- Unlock explicitly.
-- begin-expected
-- columns: unlocked
-- row: t
-- end-expected
SELECT pg_advisory_unlock(470001) AS unlocked;

-- Two-key lock variant often used to namespace lock IDs.
-- begin-expected
-- columns: got_two_key_lock
-- row: t
-- end-expected
SELECT pg_try_advisory_lock(47, 2) AS got_two_key_lock;

-- begin-expected
-- columns: unlocked_two_key_lock
-- row: t
-- end-expected
SELECT pg_advisory_unlock(47, 2) AS unlocked_two_key_lock;
