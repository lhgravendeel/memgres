DROP SCHEMA IF EXISTS test_610 CASCADE;
CREATE SCHEMA test_610;
SET search_path TO test_610;

CREATE TABLE api_requests (
    request_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id integer NOT NULL,
    created_at timestamp NOT NULL
);

CREATE TABLE quotas (
    user_id integer PRIMARY KEY,
    daily_limit integer NOT NULL,
    used_count integer NOT NULL
);

INSERT INTO api_requests(user_id, created_at) VALUES
(1,'2024-01-01 10:00'),
(1,'2024-01-01 10:00:20'),
(1,'2024-01-01 10:00:50'),
(1,'2024-01-01 10:02'),
(2,'2024-01-01 10:00'),
(2,'2024-01-01 10:03');

INSERT INTO quotas(user_id, daily_limit, used_count) VALUES
(1, 5, 3),
(2, 2, 2),
(3, 10, 0);

-- begin-expected
-- columns: recent_count
-- row: 0
-- end-expected
SELECT COUNT(*) AS recent_count
FROM api_requests
WHERE user_id = 1
  AND created_at >= '2024-01-01 10:00'
  AND created_at < '2024-01-01 10:01';

-- begin-expected
-- columns: user_id,has_quota_left
-- row: 1|t
-- row: 2|f
-- row: 3|t
-- end-expected
SELECT user_id, used_count < daily_limit AS has_quota_left
FROM quotas
ORDER BY user_id;

UPDATE quotas
SET used_count = used_count + 1
WHERE user_id = 1
  AND used_count < daily_limit;

-- begin-expected
-- columns: user_id,used_count
-- row: 1|4
-- end-expected
SELECT user_id, used_count
FROM quotas
WHERE user_id = 1;

