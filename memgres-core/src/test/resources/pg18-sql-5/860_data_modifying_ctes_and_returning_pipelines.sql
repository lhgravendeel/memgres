DROP SCHEMA IF EXISTS test_860 CASCADE;
CREATE SCHEMA test_860;
SET search_path TO test_860;

CREATE TABLE accounts (
    account_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email text NOT NULL,
    status text NOT NULL
);

CREATE TABLE account_audit (
    audit_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id integer NOT NULL,
    action text NOT NULL,
    old_status text,
    new_status text NOT NULL
);

INSERT INTO accounts(email, status) VALUES
('a@example.com', 'pending'),
('b@example.com', 'pending'),
('c@example.com', 'active');

WITH upd AS (
    UPDATE accounts
    SET status = 'active'
    WHERE status = 'pending'
    RETURNING account_id, 'pending'::text AS old_status, status AS new_status
), ins AS (
    INSERT INTO account_audit(account_id, action, old_status, new_status)
    SELECT account_id, 'activate', old_status, new_status
    FROM upd
    RETURNING account_id
)
SELECT COUNT(*) AS changed_rows
FROM ins;

-- begin-expected
-- columns: changed_rows
-- row: 0
-- end-expected
WITH upd AS (
    UPDATE accounts
    SET status = status
    WHERE false
    RETURNING account_id
)
SELECT COUNT(*) AS changed_rows
FROM upd;

-- begin-expected
-- columns: account_id,status
-- row: 1|active
-- row: 2|active
-- row: 3|active
-- end-expected
SELECT account_id, status
FROM accounts
ORDER BY account_id;

-- begin-expected
-- columns: account_id,action,old_status,new_status
-- row: 1|activate|pending|active
-- row: 2|activate|pending|active
-- end-expected
SELECT account_id, action, old_status, new_status
FROM account_audit
ORDER BY account_id;

