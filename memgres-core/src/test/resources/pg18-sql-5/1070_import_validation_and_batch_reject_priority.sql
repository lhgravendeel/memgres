DROP SCHEMA IF EXISTS test_1070 CASCADE;
CREATE SCHEMA test_1070;
SET search_path TO test_1070;

CREATE TABLE target_users (
    user_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email text NOT NULL UNIQUE
);

CREATE TABLE staging_users (
    row_num integer PRIMARY KEY,
    email text,
    full_name text
);

CREATE TABLE rejected_rows (
    row_num integer PRIMARY KEY,
    reject_reason text NOT NULL
);

INSERT INTO target_users(email) VALUES
('existing@example.com');

INSERT INTO staging_users VALUES
(1, NULL, 'Missing Email'),
(2, 'existing@example.com', 'Dup Against Target'),
(3, 'new@example.com', 'Accepted'),
(4, 'new@example.com', 'Dup In Batch'),
(5, 'ok@example.com', 'Accepted 2');

WITH batch_dupes AS (
    SELECT email
    FROM staging_users
    WHERE email IS NOT NULL
    GROUP BY email
    HAVING COUNT(*) > 1
)
INSERT INTO rejected_rows(row_num, reject_reason)
SELECT s.row_num,
       CASE
         WHEN s.email IS NULL THEN 'missing_email'
         WHEN EXISTS (SELECT 1 FROM target_users t WHERE t.email = s.email) THEN 'duplicate_target'
         WHEN EXISTS (SELECT 1 FROM batch_dupes d WHERE d.email = s.email) THEN 'duplicate_batch'
       END
FROM staging_users s
WHERE s.email IS NULL
   OR EXISTS (SELECT 1 FROM target_users t WHERE t.email = s.email)
   OR EXISTS (SELECT 1 FROM batch_dupes d WHERE d.email = s.email);

INSERT INTO target_users(email)
SELECT s.email
FROM staging_users s
LEFT JOIN rejected_rows r USING (row_num)
WHERE r.row_num IS NULL
ORDER BY s.row_num;

-- begin-expected
-- columns: row_num,reject_reason
-- row: 1|missing_email
-- row: 2|duplicate_target
-- row: 3|duplicate_batch
-- row: 4|duplicate_batch
-- end-expected
SELECT row_num, reject_reason
FROM rejected_rows
ORDER BY row_num;

-- begin-expected
-- columns: email
-- row: existing@example.com
-- row: ok@example.com
-- end-expected
SELECT email
FROM target_users
ORDER BY email;

