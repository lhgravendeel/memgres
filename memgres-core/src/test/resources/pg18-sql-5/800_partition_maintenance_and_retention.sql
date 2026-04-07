DROP SCHEMA IF EXISTS test_800 CASCADE;
CREATE SCHEMA test_800;
SET search_path TO test_800;

CREATE TABLE logs (
    log_id integer NOT NULL,
    created_on date NOT NULL,
    message text NOT NULL
) PARTITION BY RANGE (created_on);

CREATE TABLE logs_2024_01 PARTITION OF logs
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE logs_2024_02 PARTITION OF logs
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

INSERT INTO logs VALUES
(1, '2024-01-10', 'jan row'),
(2, '2024-02-10', 'feb row');

-- begin-expected
-- columns: tableoid_name,log_id
-- row: logs_2024_01|1
-- row: logs_2024_02|2
-- end-expected
SELECT tableoid::regclass::text AS tableoid_name, log_id
FROM logs
ORDER BY log_id;

-- begin-expected
-- columns: child_name
-- end-expected
SELECT c.relname AS child_name
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE p.relname = 'logs'
  AND n.nspname = 'test_800'
ORDER BY c.relname;

ALTER TABLE logs DETACH PARTITION logs_2024_01;

-- begin-expected
-- columns: log_id
-- row: 2
-- end-expected
SELECT log_id
FROM logs
ORDER BY log_id;

