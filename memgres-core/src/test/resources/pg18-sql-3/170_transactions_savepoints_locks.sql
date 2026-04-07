DROP SCHEMA IF EXISTS test_170 CASCADE;
CREATE SCHEMA test_170;
SET search_path TO test_170;

CREATE TABLE account (
    id integer PRIMARY KEY,
    balance integer NOT NULL
);

INSERT INTO account VALUES
    (1, 100),
    (2, 50);

BEGIN;
UPDATE account SET balance = balance - 10 WHERE id = 1;
UPDATE account SET balance = balance + 10 WHERE id = 2;
COMMIT;

BEGIN;
SAVEPOINT s1;
UPDATE account SET balance = balance - 999 WHERE id = 1;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
COMMIT;

-- begin-expected
-- columns: id|balance
-- row: 1|90
-- row: 2|60
-- end-expected
SELECT id, balance
FROM account
ORDER BY id;

BEGIN;
SELECT id, balance
FROM account
WHERE id = 1
FOR UPDATE;
ROLLBACK;

-- begin-expected
-- columns: id|balance
-- row: 1|90
-- end-expected
SELECT id, balance
FROM account
WHERE id = 1;

DROP SCHEMA test_170 CASCADE;
