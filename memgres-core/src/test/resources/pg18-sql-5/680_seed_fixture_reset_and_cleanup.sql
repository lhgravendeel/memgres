DROP SCHEMA IF EXISTS test_680 CASCADE;
CREATE SCHEMA test_680;
SET search_path TO test_680;

CREATE TABLE roles (
    role_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    role_name text NOT NULL UNIQUE
);

CREATE TABLE app_users (
    user_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username text NOT NULL UNIQUE,
    role_id integer NOT NULL REFERENCES roles(role_id)
);

INSERT INTO roles(role_name) VALUES ('admin'), ('member');
INSERT INTO app_users(username, role_id)
SELECT v.username, r.role_id
FROM (VALUES ('alice', 'admin'), ('bob', 'member')) AS v(username, role_name)
JOIN roles r ON r.role_name = v.role_name;

-- begin-expected
-- columns: username,role_name
-- row: alice|admin
-- row: bob|member
-- end-expected
SELECT u.username, r.role_name
FROM app_users u
JOIN roles r ON r.role_id = u.role_id
ORDER BY u.username;

DELETE FROM app_users;

-- begin-expected
-- columns: remaining_users
-- row: 0
-- end-expected
SELECT COUNT(*) AS remaining_users
FROM app_users;

INSERT INTO app_users(username, role_id)
SELECT v.username, r.role_id
FROM (VALUES ('carol', 'member')) AS v(username, role_name)
JOIN roles r ON r.role_name = v.role_name;

-- begin-expected
-- columns: user_id,username
-- row: 3|carol
-- end-expected
SELECT user_id, username
FROM app_users
ORDER BY user_id;

