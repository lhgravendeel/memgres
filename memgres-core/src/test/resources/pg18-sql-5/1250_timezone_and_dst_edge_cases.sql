DROP SCHEMA IF EXISTS test_1250 CASCADE;
CREATE SCHEMA test_1250;
SET search_path TO test_1250;

CREATE TABLE tz_events (
    event_id integer PRIMARY KEY,
    ts_utc timestamptz NOT NULL
);

INSERT INTO tz_events VALUES
(1, '2024-03-31 00:30:00+00'),
(2, '2024-03-31 01:30:00+00'),
(3, '2024-10-27 00:30:00+00'),
(4, '2024-10-27 01:30:00+00');

-- begin-expected
-- columns: event_id,ams_local
-- row: 1|2024-03-31 01:30:00
-- row: 2|2024-03-31 03:30:00
-- row: 3|2024-10-27 02:30:00
-- row: 4|2024-10-27 02:30:00
-- end-expected
SELECT event_id,
       ts_utc AT TIME ZONE 'Europe/Amsterdam' AS ams_local
FROM tz_events
ORDER BY event_id;

-- begin-expected
-- columns: event_id,ny_local
-- row: 1|2024-03-30 20:30:00
-- row: 2|2024-03-30 21:30:00
-- row: 3|2024-10-26 20:30:00
-- row: 4|2024-10-26 21:30:00
-- end-expected
SELECT event_id,
       ts_utc AT TIME ZONE 'America/New_York' AS ny_local
FROM tz_events
ORDER BY event_id;

-- begin-expected
-- columns: distinct_local_times
-- row: 1
-- end-expected
SELECT COUNT(DISTINCT (ts_utc AT TIME ZONE 'Europe/Amsterdam')) AS distinct_local_times
FROM tz_events
WHERE event_id IN (3,4);

