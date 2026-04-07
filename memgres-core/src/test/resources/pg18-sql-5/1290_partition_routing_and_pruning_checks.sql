DROP SCHEMA IF EXISTS test_1290 CASCADE;
CREATE SCHEMA test_1290;
SET search_path TO test_1290;

CREATE TABLE events (
    event_id integer NOT NULL,
    created_on date NOT NULL,
    payload text NOT NULL
) PARTITION BY RANGE (created_on);

CREATE TABLE events_jan PARTITION OF events
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE events_feb PARTITION OF events
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

INSERT INTO events VALUES
(1, '2024-01-10', 'jan'),
(2, '2024-02-10', 'feb');

-- begin-expected
-- columns: part_name,event_id
-- row: events_jan|1
-- row: events_feb|2
-- end-expected
SELECT tableoid::regclass::text AS part_name, event_id
FROM events
ORDER BY event_id;

-- begin-expected
-- columns: event_id
-- row: 1
-- end-expected
SELECT event_id
FROM events
WHERE created_on >= DATE '2024-01-01'
  AND created_on < DATE '2024-02-01'
ORDER BY event_id;

-- begin-expected
-- columns: child_name
-- end-expected
SELECT c.relname AS child_name
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE p.relname = 'events'
  AND n.nspname = 'test_1290'
ORDER BY child_name;

