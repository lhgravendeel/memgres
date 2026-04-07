DROP SCHEMA IF EXISTS test_100 CASCADE;
CREATE SCHEMA test_100;
SET search_path TO test_100;

-- begin-expected
-- columns: plus_days|month_num|truncated|diff_days
-- row: 2024-01-06|1|2024-01-05 00:00:00|4
-- end-expected
SELECT
    DATE '2024-01-01' + 5 AS plus_days,
    EXTRACT(MONTH FROM DATE '2024-01-05') AS month_num,
    DATE_TRUNC('day', TIMESTAMP '2024-01-05 12:34:56') AS truncated,
    (DATE '2024-01-05' - DATE '2024-01-01') AS diff_days;

-- begin-expected
-- columns: shifted_ts|hours_component
-- row: 2024-01-01 13:30:00|5
-- end-expected
SELECT
    TIMESTAMP '2024-01-01 10:00:00' + INTERVAL '3 hours 30 minutes' AS shifted_ts,
    EXTRACT(HOUR FROM INTERVAL '5 hours 10 minutes') AS hours_component;

-- begin-expected
-- columns: utc_ts|ams_local
-- row: 2024-01-01 12:00:00|2024-01-01 13:00:00
-- end-expected
SELECT
    TIMESTAMP WITH TIME ZONE '2024-01-01 13:00:00 Europe/Amsterdam' AT TIME ZONE 'UTC' AS utc_ts,
    TIMESTAMP WITH TIME ZONE '2024-01-01 12:00:00+00' AT TIME ZONE 'Europe/Amsterdam' AS ams_local;

DROP SCHEMA test_100 CASCADE;
