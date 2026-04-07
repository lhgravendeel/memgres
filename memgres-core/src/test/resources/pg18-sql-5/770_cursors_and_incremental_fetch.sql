DROP SCHEMA IF EXISTS test_770 CASCADE;
CREATE SCHEMA test_770;
SET search_path TO test_770;

CREATE TABLE events (
    event_id integer PRIMARY KEY,
    event_name text NOT NULL
);

INSERT INTO events VALUES
(1, 'a'),
(2, 'b'),
(3, 'c'),
(4, 'd');

BEGIN;

DECLARE event_cur CURSOR FOR
SELECT event_id, event_name
FROM events
ORDER BY event_id;

-- begin-expected
-- columns: event_id,event_name
-- row: 1|a
-- row: 2|b
-- end-expected
FETCH 2 FROM event_cur;

-- begin-expected
-- columns: event_id,event_name
-- row: 3|c
-- end-expected
FETCH 1 FROM event_cur;

MOVE FORWARD 1 FROM event_cur;

-- begin-expected
-- columns: event_id,event_name
-- end-expected
FETCH 1 FROM event_cur;

CLOSE event_cur;

COMMIT;

