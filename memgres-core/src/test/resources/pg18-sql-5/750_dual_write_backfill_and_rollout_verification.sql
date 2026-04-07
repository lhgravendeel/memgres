DROP SCHEMA IF EXISTS test_750 CASCADE;
CREATE SCHEMA test_750;
SET search_path TO test_750;

CREATE TABLE old_users (
    user_id integer PRIMARY KEY,
    email text NOT NULL
);

CREATE TABLE new_users (
    user_id integer PRIMARY KEY,
    email text NOT NULL,
    normalized_email text
);

INSERT INTO old_users(user_id, email) VALUES
(1, 'Alice@example.com'),
(2, 'bob@example.com'),
(3, 'carol@example.com');

INSERT INTO new_users(user_id, email, normalized_email) VALUES
(1, 'Alice@example.com', 'alice@example.com'),
(2, 'bob@example.com', 'bob@example.com'),
(3, 'carol@example.com', NULL);

-- begin-expected
-- columns: missing_in_new
-- row: 0
-- end-expected
SELECT COUNT(*) AS missing_in_new
FROM old_users o
LEFT JOIN new_users n USING (user_id)
WHERE n.user_id IS NULL;

-- begin-expected
-- columns: rows_needing_backfill
-- row: 1
-- end-expected
SELECT COUNT(*) AS rows_needing_backfill
FROM new_users
WHERE normalized_email IS NULL;

UPDATE new_users
SET normalized_email = lower(email)
WHERE normalized_email IS NULL;

-- begin-expected
-- columns: mismatched_normalization
-- row: 0
-- end-expected
SELECT COUNT(*) AS mismatched_normalization
FROM new_users
WHERE normalized_email <> lower(email);

-- begin-expected
-- columns: old_count,new_count
-- row: 3|3
-- end-expected
SELECT
  (SELECT COUNT(*) FROM old_users) AS old_count,
  (SELECT COUNT(*) FROM new_users) AS new_count;

