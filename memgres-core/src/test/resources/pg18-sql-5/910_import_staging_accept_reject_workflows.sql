DROP SCHEMA IF EXISTS test_910 CASCADE;
CREATE SCHEMA test_910;
SET search_path TO test_910;

CREATE TABLE staging_users (
    row_num integer PRIMARY KEY,
    email text,
    age integer
);

CREATE TABLE accepted_users (
    user_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email text NOT NULL,
    age integer NOT NULL
);

CREATE TABLE rejected_users (
    row_num integer PRIMARY KEY,
    reason text NOT NULL
);

INSERT INTO staging_users VALUES
(1, 'a@example.com', 20),
(2, NULL, 30),
(3, 'b@example.com', -1),
(4, 'c@example.com', 40);

INSERT INTO rejected_users(row_num, reason)
SELECT row_num,
       CASE
         WHEN email IS NULL THEN 'missing_email'
         WHEN age < 0 THEN 'invalid_age'
       END AS reason
FROM staging_users
WHERE email IS NULL OR age < 0;

INSERT INTO accepted_users(email, age)
SELECT email, age
FROM staging_users
WHERE email IS NOT NULL
  AND age >= 0
ORDER BY row_num;

-- begin-expected
-- columns: row_num,reason
-- row: 2|missing_email
-- row: 3|invalid_age
-- end-expected
SELECT row_num, reason
FROM rejected_users
ORDER BY row_num;

-- begin-expected
-- columns: email,age
-- row: a@example.com|20
-- row: c@example.com|40
-- end-expected
SELECT email, age
FROM accepted_users
ORDER BY user_id;

-- begin-expected
-- columns: accepted_count,rejected_count
-- row: 2|2
-- end-expected
SELECT
  (SELECT COUNT(*) FROM accepted_users) AS accepted_count,
  (SELECT COUNT(*) FROM rejected_users) AS rejected_count;

