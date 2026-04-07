DROP SCHEMA IF EXISTS test_1020 CASCADE;
CREATE SCHEMA test_1020;
SET search_path TO test_1020;

CREATE TABLE inventory (
    sku text PRIMARY KEY,
    qty integer NOT NULL,
    active boolean NOT NULL
);

CREATE TABLE inventory_stage (
    sku text PRIMARY KEY,
    qty integer NOT NULL,
    active boolean NOT NULL
);

INSERT INTO inventory VALUES
('A', 10, true),
('B', 20, true),
('C', 30, true);

INSERT INTO inventory_stage VALUES
('A', 15, true),
('B', 0, false),
('D', 40, true);

MERGE INTO inventory AS tgt
USING inventory_stage AS src
ON tgt.sku = src.sku
WHEN MATCHED AND src.active = false THEN
    DELETE
WHEN MATCHED THEN
    UPDATE SET qty = src.qty, active = src.active
WHEN NOT MATCHED AND src.active = true THEN
    INSERT (sku, qty, active)
    VALUES (src.sku, src.qty, src.active);

-- begin-expected
-- columns: sku,qty,active
-- row: A|15|t
-- row: C|30|t
-- row: D|40|t
-- end-expected
SELECT sku, qty, active
FROM inventory
ORDER BY sku;

-- begin-expected
-- columns: missing_from_stage
-- row: 1
-- end-expected
SELECT COUNT(*) AS missing_from_stage
FROM inventory i
LEFT JOIN inventory_stage s USING (sku)
WHERE s.sku IS NULL;

