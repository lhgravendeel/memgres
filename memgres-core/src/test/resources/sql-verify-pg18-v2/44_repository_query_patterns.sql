\pset pager off
\pset format unaligned
\pset tuples_only off
\pset null <NULL>
\set VERBOSITY verbose
\set SHOW_CONTEXT always
\set ON_ERROR_STOP off

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;
SET client_min_messages = notice;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';

SELECT current_schema() AS current_schema,
       current_setting('TimeZone') AS timezone,
       current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle;

CREATE TABLE repo_account(
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  email text NOT NULL UNIQUE,
  active boolean,
  deleted_at timestamptz,
  version int NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE repo_order(
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_id bigint NOT NULL REFERENCES repo_account(id),
  status text NOT NULL,
  total numeric(12,2) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO repo_account(email, active, deleted_at) VALUES
('a@example.com', true, NULL),
('b@example.com', false, NULL),
('c@example.com', NULL, CURRENT_TIMESTAMP);

INSERT INTO repo_order(account_id, status, total) VALUES
(1, 'open', 10.00),
(1, 'open', 20.00),
(1, 'done', 30.00),
(2, 'open', 40.00);

-- common repository query shapes
SELECT * FROM repo_account WHERE id = 1;
SELECT * FROM repo_account WHERE email = 'a@example.com';
SELECT * FROM repo_account WHERE deleted_at IS NULL ORDER BY id;
SELECT * FROM repo_account WHERE active IS TRUE ORDER BY id;
SELECT EXISTS (SELECT 1 FROM repo_account WHERE email = 'missing@example.com');
SELECT count(*) FROM repo_order WHERE account_id = 1;
SELECT account_id, count(*), sum(total) FROM repo_order GROUP BY account_id ORDER BY account_id;

SELECT o.*
FROM repo_order o
WHERE o.account_id = 1
ORDER BY o.created_at DESC, o.id DESC
LIMIT 2 OFFSET 0;

SELECT o.*
FROM repo_order o
WHERE (o.created_at, o.id) < (CURRENT_TIMESTAMP, 999999)
ORDER BY o.created_at DESC, o.id DESC
LIMIT 10;

UPDATE repo_account
SET version = version + 1
WHERE id = 1 AND version = 0
RETURNING *;

UPDATE repo_account
SET version = version + 1
WHERE id = 1 AND version = 0
RETURNING *;

INSERT INTO repo_account(email, active)
VALUES ('d@example.com', true)
ON CONFLICT (email) DO NOTHING
RETURNING *;

SELECT ra.id, ra.email, ro.id AS order_id, ro.status, ro.total
FROM repo_account ra
LEFT JOIN repo_order ro ON ro.account_id = ra.id
WHERE ra.deleted_at IS NULL
ORDER BY ra.id, ro.id;

-- zero rows / null distinctions
SELECT (SELECT total FROM repo_order WHERE id = -1);
SELECT sum(total) FROM repo_order WHERE id = -1;
SELECT * FROM repo_account WHERE id = -1;
DELETE FROM repo_order WHERE id = -1 RETURNING *;

-- bad repository-shaped cases
SELECT * FROM repo_account WHERE email = 1;
SELECT * FROM repo_order ORDER BY created_at FETCH FIRST -1 ROWS ONLY;
UPDATE repo_account SET version = version + 1 WHERE id = 1 AND version = 'x';
SELECT (SELECT total FROM repo_order WHERE account_id = 1);

DROP SCHEMA compat CASCADE;
