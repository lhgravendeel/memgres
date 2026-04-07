DROP SCHEMA IF EXISTS test_660 CASCADE;
CREATE SCHEMA test_660;
SET search_path TO test_660;

CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

CREATE TABLE accounts (
    account_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email text NOT NULL UNIQUE,
    status mood NOT NULL DEFAULT 'ok',
    created_at timestamp NOT NULL DEFAULT TIMESTAMP '2024-01-01 00:00:00'
);

CREATE INDEX idx_accounts_created_at ON accounts(created_at);

CREATE VIEW active_accounts AS
SELECT account_id, email
FROM accounts
WHERE status = 'happy';

INSERT INTO accounts(email, status) VALUES
('a@example.com', 'happy'),
('b@example.com', 'ok');

-- begin-expected
-- columns: table_name
-- row: accounts
-- end-expected
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'test_660'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- begin-expected
-- columns: column_name,data_type,is_nullable
-- row: account_id|integer|NO
-- row: created_at|timestamp without time zone|NO
-- row: email|text|NO
-- row: status|USER-DEFINED|NO
-- end-expected
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'test_660'
  AND table_name = 'accounts'
ORDER BY column_name;

-- begin-expected
-- columns: indexname
-- row: accounts_email_key
-- row: accounts_pkey
-- row: idx_accounts_created_at
-- end-expected
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'test_660'
  AND tablename = 'accounts'
ORDER BY indexname;

-- begin-expected
-- columns: enumlabel
-- row: sad
-- row: ok
-- row: happy
-- end-expected
SELECT e.enumlabel
FROM pg_type t
JOIN pg_enum e ON e.enumtypid = t.oid
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = 'test_660'
  AND t.typname = 'mood'
ORDER BY e.enumsortorder;

-- begin-expected
-- columns: viewname
-- row: active_accounts
-- end-expected
SELECT viewname
FROM pg_views
WHERE schemaname = 'test_660'
ORDER BY viewname;

