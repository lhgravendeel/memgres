DROP SCHEMA IF EXISTS test_235 CASCADE;
CREATE SCHEMA test_235;
SET search_path TO test_235;

-- Optional file: requires privilege to create roles and set roles.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_owner_demo') THEN
        CREATE ROLE rls_owner_demo LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_north_demo') THEN
        CREATE ROLE rls_north_demo LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_south_demo') THEN
        CREATE ROLE rls_south_demo LOGIN;
    END IF;
END;
$$;

CREATE TABLE accounts (
    id integer PRIMARY KEY,
    region text NOT NULL,
    owner_name text NOT NULL,
    balance integer NOT NULL
);

INSERT INTO accounts VALUES
    (1, 'north', 'anna', 100),
    (2, 'south', 'bob', 200),
    (3, 'north', 'cara', 300);

ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE accounts FORCE ROW LEVEL SECURITY;

GRANT USAGE ON SCHEMA test_235 TO rls_north_demo, rls_south_demo;
GRANT SELECT, INSERT ON accounts TO rls_north_demo, rls_south_demo;

CREATE POLICY north_select ON accounts
    FOR SELECT
    TO rls_north_demo
    USING (region = 'north');

CREATE POLICY north_insert ON accounts
    FOR INSERT
    TO rls_north_demo
    WITH CHECK (region = 'north');

CREATE POLICY south_select ON accounts
    FOR SELECT
    TO rls_south_demo
    USING (region = 'south');

SET ROLE rls_north_demo;
-- begin-expected
-- columns: id|region|owner_name|balance
-- row: 1|north|anna|100
-- row: 3|north|cara|300
-- end-expected
SELECT id, region, owner_name, balance
FROM accounts
ORDER BY id;
RESET ROLE;

SET ROLE rls_south_demo;
-- begin-expected
-- columns: id|region|owner_name|balance
-- row: 2|south|bob|200
-- end-expected
SELECT id, region, owner_name, balance
FROM accounts
ORDER BY id;
RESET ROLE;

SET ROLE rls_north_demo;
INSERT INTO accounts VALUES (4, 'north', 'dina', 400);
RESET ROLE;

SET ROLE rls_north_demo;
-- begin-expected-error
-- message-like: row-level security policy
-- end-expected-error
INSERT INTO accounts VALUES (5, 'south', 'eric', 500);
RESET ROLE;

SET ROLE rls_north_demo;
-- begin-expected
-- columns: id|region|owner_name|balance
-- row: 1|north|anna|100
-- row: 3|north|cara|300
-- row: 4|north|dina|400
-- end-expected
SELECT id, region, owner_name, balance
FROM accounts
ORDER BY id;
RESET ROLE;

DROP SCHEMA test_235 CASCADE;
DROP ROLE IF EXISTS rls_owner_demo;
DROP ROLE IF EXISTS rls_north_demo;
DROP ROLE IF EXISTS rls_south_demo;
