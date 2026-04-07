\pset pager off
\pset format unaligned
\pset tuples_only off
\pset null <NULL>
\set VERBOSITY verbose
\set SHOW_CONTEXT always
\set ON_ERROR_STOP off

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;
SET client_min_messages = notice;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';

SELECT current_schema() AS current_schema,
       current_setting('TimeZone') AS timezone,
       current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle;

CREATE TABLE work_item(
  id int PRIMARY KEY,
  status text NOT NULL,
  priority int NOT NULL,
  attempts int NOT NULL DEFAULT 0,
  payload text
);

INSERT INTO work_item VALUES
(1, 'ready', 10, 0, 'a'),
(2, 'ready', 5, 1, 'b'),
(3, 'done',  1, 0, 'c');

-- single-session lock syntax shapes
SELECT * FROM work_item ORDER BY id FOR UPDATE;
SELECT * FROM work_item ORDER BY id FOR NO KEY UPDATE;
SELECT * FROM work_item ORDER BY id FOR SHARE;
SELECT * FROM work_item ORDER BY id FOR KEY SHARE;
SELECT * FROM work_item ORDER BY priority DESC, id FOR UPDATE NOWAIT;
SELECT * FROM work_item ORDER BY priority DESC, id FOR UPDATE SKIP LOCKED LIMIT 1;

-- queue-like update/returning patterns
WITH next_item AS (
  SELECT id
  FROM work_item
  WHERE status = 'ready'
  ORDER BY priority DESC, id
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE work_item w
SET status = 'running',
    attempts = attempts + 1
FROM next_item n
WHERE w.id = n.id
RETURNING w.id, w.status, w.attempts, w.payload;

SELECT * FROM work_item ORDER BY id;

-- optimistic locking pattern
CREATE TABLE versioned_row(
  id int PRIMARY KEY,
  version int NOT NULL,
  note text
);

INSERT INTO versioned_row VALUES
(1, 1, 'v1'),
(2, 5, 'v2');

UPDATE versioned_row
SET note = 'v1-updated',
    version = version + 1
WHERE id = 1
  AND version = 1
RETURNING *;

UPDATE versioned_row
SET note = 'stale-update',
    version = version + 1
WHERE id = 1
  AND version = 1
RETURNING *;

SELECT * FROM versioned_row ORDER BY id;

-- bad lock syntax / bad queue shapes
SELECT * FROM work_item FOR UPDATE FOR SHARE;
SELECT * FROM work_item FOR no_such_lock;
SELECT * FROM work_item ORDER BY id FOR UPDATE OF no_such_table;
WITH next_item AS (
  SELECT id FROM work_item WHERE status = 'ready' LIMIT 1
)
UPDATE work_item
SET status = 'running'
FROM next_item
WHERE work_item.id = (SELECT id FROM next_item)
RETURNING nope;

DROP SCHEMA compat CASCADE;
