DROP SCHEMA IF EXISTS test_370 CASCADE;
CREATE SCHEMA test_370;
SET search_path TO test_370;

CREATE TABLE events (
    event_id integer PRIMARY KEY,
    event_type text NOT NULL,
    created_at timestamp NOT NULL,
    account_id integer NOT NULL
);

INSERT INTO events(event_id, event_type, created_at, account_id) VALUES
    (1, 'signup', TIMESTAMP '2024-01-01 09:00:00', 10),
    (2, 'login',  TIMESTAMP '2024-01-01 10:00:00', 10),
    (3, 'signup', TIMESTAMP '2024-01-02 11:00:00', 11),
    (4, 'login',  TIMESTAMP '2024-01-02 12:00:00', 11),
    (5, 'login',  TIMESTAMP '2024-01-02 13:00:00', 10),
    (6, 'login',  TIMESTAMP '2024-01-03 08:00:00', 12),
    (7, 'signup', TIMESTAMP '2024-01-03 09:30:00', 12),
    (8, 'login',  TIMESTAMP '2024-01-03 10:00:00', 12);

-- begin-expected
-- columns: day_bucket|event_count
-- row: 2024-01-01 00:00:00|2
-- row: 2024-01-02 00:00:00|3
-- row: 2024-01-03 00:00:00|3
-- end-expected
SELECT date_trunc('day', created_at) AS day_bucket, count(*) AS event_count
FROM events
GROUP BY date_trunc('day', created_at)
ORDER BY day_bucket;

-- begin-expected
-- columns: day_bucket|signup_count|login_count
-- row: 2024-01-01 00:00:00|1|1
-- row: 2024-01-02 00:00:00|1|2
-- row: 2024-01-03 00:00:00|1|2
-- end-expected
SELECT date_trunc('day', created_at) AS day_bucket,
       count(*) FILTER (WHERE event_type = 'signup') AS signup_count,
       count(*) FILTER (WHERE event_type = 'login') AS login_count
FROM events
GROUP BY date_trunc('day', created_at)
ORDER BY day_bucket;

-- begin-expected
-- columns: account_id|day_bucket|daily_count|running_total
-- row: 10|2024-01-01 00:00:00|2|2
-- row: 10|2024-01-02 00:00:00|1|3
-- row: 11|2024-01-02 00:00:00|2|2
-- row: 12|2024-01-03 00:00:00|3|3
-- end-expected
SELECT account_id,
       day_bucket,
       daily_count,
       sum(daily_count) OVER (PARTITION BY account_id ORDER BY day_bucket) AS running_total
FROM (
    SELECT account_id,
           date_trunc('day', created_at) AS day_bucket,
           count(*) AS daily_count
    FROM events
    GROUP BY account_id, date_trunc('day', created_at)
) AS daily
ORDER BY account_id, day_bucket;

-- begin-expected
-- columns: day_bucket|distinct_accounts
-- row: 2024-01-01 00:00:00|1
-- row: 2024-01-02 00:00:00|2
-- row: 2024-01-03 00:00:00|1
-- end-expected
SELECT date_trunc('day', created_at) AS day_bucket,
       count(DISTINCT account_id) AS distinct_accounts
FROM events
GROUP BY date_trunc('day', created_at)
ORDER BY day_bucket;

DROP SCHEMA test_370 CASCADE;
