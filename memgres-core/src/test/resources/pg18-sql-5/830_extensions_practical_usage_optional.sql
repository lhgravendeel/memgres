DROP SCHEMA IF EXISTS test_830 CASCADE;
CREATE SCHEMA test_830;
SET search_path TO test_830;

CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE users (
    user_id integer PRIMARY KEY,
    email text NOT NULL
);

INSERT INTO users VALUES
(1, 'Alice@example.com'),
(2, 'bob@example.com');

-- begin-expected
-- columns: user_id
-- row: 1
-- end-expected
SELECT user_id
FROM users
WHERE lower(email) = lower('alice@example.com');

-- begin-expected-error
-- message-like: function similarity
-- end-expected-error
SELECT similarity('postgres', 'postgress') > 0 AS similarity_positive;
