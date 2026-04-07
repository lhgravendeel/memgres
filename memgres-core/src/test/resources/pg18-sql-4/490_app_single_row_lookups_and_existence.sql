DROP SCHEMA IF EXISTS test_490 CASCADE;
CREATE SCHEMA test_490;
SET search_path TO test_490;

CREATE TABLE accounts (
    account_id integer PRIMARY KEY,
    tenant_id integer NOT NULL,
    email text NOT NULL,
    slug text NOT NULL,
    external_id text,
    active boolean NOT NULL,
    deleted_at timestamp,
    created_at timestamp NOT NULL,
    UNIQUE (tenant_id, slug),
    UNIQUE (tenant_id, email)
);

INSERT INTO accounts(account_id, tenant_id, email, slug, external_id, active, deleted_at, created_at) VALUES
    (1, 10, 'alice@example.com', 'alice', 'ext-1', true,  NULL, TIMESTAMP '2024-01-01 09:00:00'),
    (2, 10, 'bob@example.com',   'bob',   'ext-2', false, NULL, TIMESTAMP '2024-01-02 09:00:00'),
    (3, 20, 'alice@example.com', 'alice', 'ext-3', true,  NULL, TIMESTAMP '2024-01-03 09:00:00'),
    (4, 10, 'carol@example.com', 'carol', NULL,    true,  TIMESTAMP '2024-01-10 10:00:00', TIMESTAMP '2024-01-04 09:00:00');

-- begin-expected
-- columns: account_id|email|active
-- row: 1|alice@example.com|t
-- end-expected
SELECT account_id, email, active
FROM accounts
WHERE account_id = 1;

-- begin-expected
-- columns: account_id|tenant_id|slug
-- row: 3|20|alice
-- end-expected
SELECT account_id, tenant_id, slug
FROM accounts
WHERE tenant_id = 20 AND slug = 'alice';

-- begin-expected
-- columns: account_id|email
-- row: 1|alice@example.com
-- end-expected
SELECT account_id, email
FROM accounts
WHERE tenant_id = 10
  AND lower(email) = lower('ALICE@EXAMPLE.COM')
  AND deleted_at IS NULL;

-- begin-expected
-- columns: account_id|chosen_key
-- row: 2|ext-2
-- end-expected
SELECT account_id,
       COALESCE(external_id, slug) AS chosen_key
FROM accounts
WHERE COALESCE(external_id, slug) = 'ext-2';

-- begin-expected
-- columns: found
-- row: 1
-- end-expected
SELECT 1 AS found
FROM accounts
WHERE tenant_id = 10
  AND slug = 'bob'
LIMIT 1;

-- begin-expected
-- columns: exists_live_carol
-- row: f
-- end-expected
SELECT EXISTS (
    SELECT 1
    FROM accounts
    WHERE tenant_id = 10
      AND slug = 'carol'
      AND deleted_at IS NULL
) AS exists_live_carol;

-- begin-expected
-- columns: account_id|resolved_by
-- row: 1|email
-- end-expected
SELECT account_id,
       CASE
           WHEN email = 'alice@example.com' THEN 'email'
           WHEN slug = 'alice'
           THEN 'slug'
           ELSE 'none'
       END AS resolved_by
FROM accounts
WHERE tenant_id = 10
  AND (email = 'alice@example.com' OR slug = 'alice')
ORDER BY CASE WHEN email = 'alice@example.com' THEN 1 ELSE 2 END,
         account_id
LIMIT 1;

DROP SCHEMA test_490 CASCADE;
