DROP SCHEMA IF EXISTS test_220 CASCADE;
CREATE SCHEMA test_220;
SET search_path TO test_220;

CREATE TABLE inventory (
    sku text PRIMARY KEY,
    qty integer NOT NULL,
    updated_by text NOT NULL
);

INSERT INTO inventory VALUES
    ('A', 10, 'seed'),
    ('B', 5, 'seed');

INSERT INTO inventory(sku, qty, updated_by)
VALUES ('A', 99, 'ignored')
ON CONFLICT (sku) DO NOTHING;

INSERT INTO inventory(sku, qty, updated_by)
VALUES ('B', 7, 'upsert')
ON CONFLICT (sku) DO UPDATE
SET qty = inventory.qty + EXCLUDED.qty,
    updated_by = EXCLUDED.updated_by;

CREATE TABLE incoming_inventory (
    sku text,
    qty integer,
    updated_by text
);

INSERT INTO incoming_inventory VALUES
    ('A', 3, 'merge'),
    ('C', 8, 'merge');

MERGE INTO inventory AS t
USING incoming_inventory AS s
ON t.sku = s.sku
WHEN MATCHED THEN
    UPDATE SET qty = t.qty + s.qty, updated_by = s.updated_by
WHEN NOT MATCHED THEN
    INSERT (sku, qty, updated_by)
    VALUES (s.sku, s.qty, s.updated_by);

-- begin-expected
-- columns: sku|qty|updated_by
-- row: A|13|merge
-- row: B|12|upsert
-- row: C|8|merge
-- end-expected
SELECT sku, qty, updated_by
FROM inventory
ORDER BY sku;

DROP SCHEMA test_220 CASCADE;
