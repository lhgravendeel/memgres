DROP SCHEMA IF EXISTS test_710 CASCADE;
CREATE SCHEMA test_710;
SET search_path TO test_710;

CREATE TABLE demo (id integer PRIMARY KEY, name text);
INSERT INTO demo VALUES (1, 'x');

-- begin-expected
-- columns: current_pid_is_positive
-- row: t
-- end-expected
SELECT pg_backend_pid() > 0 AS current_pid_is_positive;

-- A relation lock belongs to the transaction that took it. This statement reads pg_locks, not
-- demo, and nothing else is open, so PostgreSQL holds no lock on demo to find. Verified against
-- PostgreSQL 18: the answer is f.
-- begin-expected
-- columns: found_relation_lock
-- row: f
-- end-expected
SELECT EXISTS (
    SELECT 1
    FROM pg_locks l
    JOIN pg_class c ON c.oid = l.relation
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'test_710'
      AND c.relname = 'demo'
) AS found_relation_lock;

-- begin-expected
-- columns: found_self_activity_row
-- row: t
-- end-expected
SELECT EXISTS (
    SELECT 1
    FROM pg_stat_activity
    WHERE pid = pg_backend_pid()
) AS found_self_activity_row;

