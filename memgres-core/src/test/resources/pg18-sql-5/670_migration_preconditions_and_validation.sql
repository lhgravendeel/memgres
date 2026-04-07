DROP SCHEMA IF EXISTS test_670 CASCADE;
CREATE SCHEMA test_670;
SET search_path TO test_670;

CREATE TABLE users (
    user_id integer PRIMARY KEY,
    email text,
    normalized_email text
);

INSERT INTO users(user_id, email, normalized_email) VALUES
(1, 'Alice@example.com', NULL),
(2, 'bob@example.com', NULL),
(3, 'bob@example.com', NULL),
(4, NULL, NULL);

-- begin-expected
-- columns: duplicate_email,cnt
-- row: bob@example.com|2
-- end-expected
SELECT lower(email) AS duplicate_email, COUNT(*) AS cnt
FROM users
WHERE email IS NOT NULL
GROUP BY lower(email)
HAVING COUNT(*) > 1
ORDER BY duplicate_email;

-- begin-expected
-- columns: rows_missing_backfill
-- row: 3
-- end-expected
SELECT COUNT(*) AS rows_missing_backfill
FROM users
WHERE email IS NOT NULL
  AND normalized_email IS NULL;

UPDATE users
SET normalized_email = lower(email)
WHERE email IS NOT NULL
  AND normalized_email IS NULL;

-- begin-expected
-- columns: mismatched_rows
-- row: 0
-- end-expected
SELECT COUNT(*) AS mismatched_rows
FROM users
WHERE email IS NOT NULL
  AND normalized_email <> lower(email);

-- begin-expected
-- columns: rows_still_null
-- row: 1
-- end-expected
SELECT COUNT(*) AS rows_still_null
FROM users
WHERE normalized_email IS NULL;

