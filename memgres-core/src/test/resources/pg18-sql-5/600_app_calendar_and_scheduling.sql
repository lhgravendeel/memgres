DROP SCHEMA IF EXISTS test_600 CASCADE;
CREATE SCHEMA test_600;
SET search_path TO test_600;

CREATE TABLE events (
    event_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    room text NOT NULL,
    title text NOT NULL,
    starts_at timestamp NOT NULL,
    ends_at timestamp NOT NULL
);

INSERT INTO events(room, title, starts_at, ends_at) VALUES
('alpha','standup','2024-01-10 09:00','2024-01-10 09:30'),
('alpha','planning','2024-01-10 10:00','2024-01-10 11:00'),
('alpha','retro','2024-01-10 15:00','2024-01-10 16:00'),
('beta','sync','2024-01-10 09:15','2024-01-10 10:15'),
('beta','review','2024-01-10 11:00','2024-01-10 12:00');

-- begin-expected
-- columns: title
-- row: standup
-- row: planning
-- end-expected
SELECT title
FROM events
WHERE room = 'alpha'
  AND starts_at < TIMESTAMP '2024-01-10 10:30'
  AND ends_at > TIMESTAMP '2024-01-10 09:15'
ORDER BY starts_at;

-- begin-expected
-- columns: next_event
-- row: planning
-- end-expected
SELECT title AS next_event
FROM events
WHERE room = 'alpha'
  AND starts_at > TIMESTAMP '2024-01-10 09:30'
ORDER BY starts_at
LIMIT 1;

-- begin-expected
-- columns: room,count
-- row: alpha|3
-- row: beta|2
-- end-expected
SELECT room, COUNT(*)
FROM events
GROUP BY room
ORDER BY room;

-- begin-expected
-- columns: room,is_free
-- row: alpha|f
-- row: beta|f
-- end-expected
SELECT r.room,
       NOT EXISTS (
           SELECT 1
           FROM events e
           WHERE e.room = r.room
             AND e.starts_at < TIMESTAMP '2024-01-10 09:20'
             AND e.ends_at > TIMESTAMP '2024-01-10 09:10'
       ) AS is_free
FROM (VALUES ('alpha'), ('beta')) AS r(room)
ORDER BY r.room;

