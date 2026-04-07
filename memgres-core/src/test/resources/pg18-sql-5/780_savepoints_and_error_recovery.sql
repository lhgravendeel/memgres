DROP SCHEMA IF EXISTS test_780 CASCADE;
CREATE SCHEMA test_780;
SET search_path TO test_780;

CREATE TABLE accounts (
    account_id integer PRIMARY KEY,
    email text NOT NULL UNIQUE
);

BEGIN;

INSERT INTO accounts VALUES (1, 'a@example.com');

SAVEPOINT s1;
INSERT INTO accounts VALUES (2, 'b@example.com');
ROLLBACK TO SAVEPOINT s1;

INSERT INTO accounts VALUES (3, 'c@example.com');

COMMIT;

-- begin-expected
-- columns: account_id,email
-- row: 1|a@example.com
-- row: 3|c@example.com
-- end-expected
SELECT account_id, email
FROM accounts
ORDER BY account_id;

BEGIN;
SAVEPOINT s2;
INSERT INTO accounts VALUES (4, 'd@example.com');
ROLLBACK TO SAVEPOINT s2;
INSERT INTO accounts VALUES (5, 'e@example.com');
COMMIT;

-- begin-expected
-- columns: account_id
-- row: 1
-- row: 3
-- row: 5
-- end-expected
SELECT account_id
FROM accounts
ORDER BY account_id;

