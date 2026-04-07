DROP SCHEMA IF EXISTS test_540 CASCADE;
CREATE SCHEMA test_540;
SET search_path TO test_540;

CREATE TABLE users (
    user_id integer PRIMARY KEY,
    email text NOT NULL
);

CREATE TABLE sessions (
    session_id integer PRIMARY KEY,
    user_id integer NOT NULL REFERENCES users(user_id),
    token_hash text NOT NULL,
    created_at timestamp NOT NULL,
    expires_at timestamp NOT NULL,
    revoked_at timestamp
);

CREATE TABLE auth_identities (
    identity_id integer PRIMARY KEY,
    user_id integer NOT NULL REFERENCES users(user_id),
    provider text NOT NULL,
    provider_user_id text NOT NULL,
    UNIQUE (provider, provider_user_id)
);

INSERT INTO users VALUES
    (1, 'alice@example.com'),
    (2, 'bob@example.com');

INSERT INTO sessions VALUES
    (1, 1, 'tok-a', TIMESTAMP '2024-01-10 08:00:00', TIMESTAMP '2024-01-10 12:00:00', NULL),
    (2, 1, 'tok-b', TIMESTAMP '2024-01-09 08:00:00', TIMESTAMP '2024-01-09 12:00:00', NULL),
    (3, 2, 'tok-c', TIMESTAMP '2024-01-10 08:30:00', TIMESTAMP '2024-01-10 12:30:00', TIMESTAMP '2024-01-10 09:00:00');

INSERT INTO auth_identities VALUES
    (1, 1, 'github', 'gh_100'),
    (2, 2, 'google', 'gg_200');

-- begin-expected
-- columns: session_id|user_id
-- row: 1|1
-- end-expected
SELECT session_id, user_id
FROM sessions
WHERE token_hash = 'tok-a'
  AND revoked_at IS NULL
  AND expires_at > TIMESTAMP '2024-01-10 10:00:00';

-- begin-expected
-- columns: exists_valid_session
-- row: f
-- end-expected
SELECT EXISTS (
    SELECT 1
    FROM sessions
    WHERE token_hash = 'tok-b'
      AND revoked_at IS NULL
      AND expires_at > TIMESTAMP '2024-01-10 10:00:00'
) AS exists_valid_session;

-- begin-expected
-- columns: user_id|latest_valid_session_id
-- row: 1|1
-- row: 2|
-- end-expected
SELECT u.user_id,
       (
           SELECT s.session_id
           FROM sessions s
           WHERE s.user_id = u.user_id
             AND s.revoked_at IS NULL
             AND s.expires_at > TIMESTAMP '2024-01-10 10:00:00'
           ORDER BY s.created_at DESC, s.session_id DESC
           LIMIT 1
       ) AS latest_valid_session_id
FROM users u
ORDER BY u.user_id;

-- begin-expected
-- columns: user_id|email
-- row: 1|alice@example.com
-- end-expected
SELECT u.user_id, u.email
FROM users u
JOIN auth_identities ai ON ai.user_id = u.user_id
WHERE ai.provider = 'github'
  AND ai.provider_user_id = 'gh_100';

DROP SCHEMA test_540 CASCADE;
