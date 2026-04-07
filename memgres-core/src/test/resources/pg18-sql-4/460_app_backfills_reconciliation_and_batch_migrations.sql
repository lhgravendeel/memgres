-- 460_app_backfills_reconciliation_and_batch_migrations.sql
-- Backfill, reconciliation, and batch migration query patterns.

DROP SCHEMA IF EXISTS test_460 CASCADE;
CREATE SCHEMA test_460;
SET search_path TO test_460;

CREATE TABLE accounts (
    id integer PRIMARY KEY,
    slug text NOT NULL UNIQUE
);

CREATE TABLE invoices (
    id integer PRIMARY KEY,
    account_id integer NOT NULL REFERENCES accounts(id),
    amount_cents integer NOT NULL,
    paid boolean NOT NULL,
    legacy_slug text NULL,
    new_slug text NULL,
    migrated_at timestamp NULL
);

CREATE TABLE imported_invoices (
    external_id integer PRIMARY KEY,
    account_slug text NOT NULL,
    amount_cents integer NOT NULL
);

INSERT INTO accounts(id, slug) VALUES
(1, 'acme'),
(2, 'globex');

INSERT INTO invoices(id, account_id, amount_cents, paid, legacy_slug, new_slug, migrated_at) VALUES
(10, 1, 1000, true,  'inv-acme-10', NULL, NULL),
(11, 1, 1500, false, 'inv-acme-11', NULL, NULL),
(12, 2, 2000, true,  'inv-globex-12', 'inv-globex-12', TIMESTAMP '2025-01-01 00:00:00');

INSERT INTO imported_invoices(external_id, account_slug, amount_cents) VALUES
(10, 'acme',   1000),
(11, 'acme',   1700),
(13, 'globex', 2500);

-- Find rows needing backfill in a batch window.
-- begin-expected
-- columns: id,legacy_slug
-- row: 10|inv-acme-10
-- row: 11|inv-acme-11
-- end-expected
SELECT id, legacy_slug
FROM invoices
WHERE new_slug IS NULL
ORDER BY id
LIMIT 2;

-- Perform backfill.
UPDATE invoices
SET new_slug = legacy_slug,
    migrated_at = TIMESTAMP '2025-02-01 00:00:00'
WHERE new_slug IS NULL
  AND id BETWEEN 10 AND 11;

-- begin-expected
-- columns: id,new_slug,migrated_at
-- row: 10|inv-acme-10|2025-02-01 00:00:00
-- row: 11|inv-acme-11|2025-02-01 00:00:00
-- row: 12|inv-globex-12|2025-01-01 00:00:00
-- end-expected
SELECT id, new_slug, migrated_at
FROM invoices
ORDER BY id;

-- Reconciliation: imported rows missing locally.
-- begin-expected
-- columns: external_id,account_slug,amount_cents
-- row: 13|globex|2500
-- end-expected
SELECT ii.external_id, ii.account_slug, ii.amount_cents
FROM imported_invoices ii
LEFT JOIN invoices i ON i.id = ii.external_id
WHERE i.id IS NULL
ORDER BY ii.external_id;

-- Reconciliation: local/import amount mismatches.
-- begin-expected
-- columns: invoice_id,local_amount,imported_amount
-- row: 11|1500|1700
-- end-expected
SELECT
    i.id AS invoice_id,
    i.amount_cents AS local_amount,
    ii.amount_cents AS imported_amount
FROM invoices i
JOIN imported_invoices ii ON ii.external_id = i.id
WHERE i.amount_cents <> ii.amount_cents
ORDER BY i.id;

-- Batch migration using keyset pagination shape.
-- begin-expected
-- columns: id
-- row: 11
-- row: 12
-- end-expected
SELECT id
FROM invoices
WHERE id > 10
ORDER BY id
LIMIT 2;

-- Update from imported data to repair mismatches.
UPDATE invoices i
SET amount_cents = ii.amount_cents
FROM imported_invoices ii
WHERE ii.external_id = i.id
  AND ii.amount_cents <> i.amount_cents;

-- begin-expected
-- columns: id,amount_cents
-- row: 10|1000
-- row: 11|1700
-- row: 12|2000
-- end-expected
SELECT id, amount_cents
FROM invoices
ORDER BY id;
