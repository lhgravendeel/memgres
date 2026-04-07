DROP SCHEMA IF EXISTS test_510 CASCADE;
CREATE SCHEMA test_510;
SET search_path TO test_510;

CREATE TABLE subscriptions (
    subscription_id integer PRIMARY KEY,
    user_id integer NOT NULL,
    starts_at timestamp NOT NULL,
    ends_at timestamp,
    updated_at timestamp NOT NULL,
    status text NOT NULL
);

INSERT INTO subscriptions VALUES
    (1, 10, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-10 00:00:00', TIMESTAMP '2024-01-01 12:00:00', 'expired'),
    (2, 11, TIMESTAMP '2024-01-05 00:00:00', NULL,                              TIMESTAMP '2024-01-06 12:00:00', 'active'),
    (3, 12, TIMESTAMP '2024-01-08 00:00:00', TIMESTAMP '2024-01-20 00:00:00', TIMESTAMP '2024-01-09 12:00:00', 'active'),
    (4, 13, TIMESTAMP '2024-01-15 00:00:00', TIMESTAMP '2024-01-18 00:00:00', TIMESTAMP '2024-01-15 12:00:00', 'trial'),
    (5, 14, TIMESTAMP '2024-01-21 00:00:00', NULL,                              TIMESTAMP '2024-01-22 12:00:00', 'active');

-- begin-expected
-- columns: subscription_id|user_id
-- row: 1|10
-- row: 2|11
-- row: 3|12
-- row: 4|13
-- end-expected
SELECT subscription_id, user_id
FROM subscriptions
WHERE starts_at < TIMESTAMP '2024-01-17 00:00:00'
  AND COALESCE(ends_at, TIMESTAMP '9999-12-31 00:00:00') > TIMESTAMP '2024-01-09 00:00:00'
ORDER BY subscription_id;

-- begin-expected
-- columns: subscription_id|updated_at
-- row: 3|2024-01-09 12:00:00
-- row: 4|2024-01-15 12:00:00
-- row: 5|2024-01-22 12:00:00
-- end-expected
SELECT subscription_id, updated_at
FROM subscriptions
WHERE updated_at >= TIMESTAMP '2024-01-09 00:00:00'
ORDER BY updated_at, subscription_id;

-- begin-expected
-- columns: subscription_id|user_id
-- row: 3|12
-- row: 4|13
-- end-expected
SELECT subscription_id, user_id
FROM subscriptions
WHERE subscription_id > 2
ORDER BY subscription_id
LIMIT 2;

-- begin-expected
-- columns: subscription_id|user_id
-- row: 2|11
-- row: 3|12
-- row: 4|13
-- end-expected
SELECT subscription_id, user_id
FROM subscriptions
WHERE subscription_id BETWEEN 2 AND 4
ORDER BY subscription_id;

-- begin-expected
-- columns: status|count_rows
-- row: active|3
-- row: expired|1
-- row: trial|1
-- end-expected
SELECT status, COUNT(*) AS count_rows
FROM subscriptions
WHERE updated_at >= TIMESTAMP '2024-01-01 00:00:00'
GROUP BY status
ORDER BY status;

DROP SCHEMA test_510 CASCADE;
