DROP SCHEMA IF EXISTS test_1030 CASCADE;
CREATE SCHEMA test_1030;
SET search_path TO test_1030;

CREATE TABLE prices (
    sku text NOT NULL,
    valid_period daterange NOT NULL,
    price integer NOT NULL,
    PRIMARY KEY (sku, valid_period)
);

INSERT INTO prices VALUES
('A', daterange('2024-01-01', '2024-02-01', '[)'), 10),
('A', daterange('2024-02-01', NULL, '[)'), 12),
('B', daterange('2024-01-15', NULL, '[)'), 20);

-- begin-expected
-- columns: sku,price
-- row: A|10
-- row: B|20
-- end-expected
SELECT sku, price
FROM prices
WHERE valid_period @> DATE '2024-01-20'
ORDER BY sku;

-- begin-expected
-- columns: sku,price
-- row: A|12
-- row: B|20
-- end-expected
SELECT sku, price
FROM prices
WHERE valid_period @> DATE '2024-02-20'
ORDER BY sku;

-- begin-expected
-- columns: overlaps_future
-- row: 2
-- end-expected
SELECT COUNT(*) AS overlaps_future
FROM prices
WHERE valid_period && daterange('2024-03-01', '2024-03-15', '[)');

-- begin-expected
-- columns: sku,upper_inf
-- row: A|t
-- row: B|t
-- end-expected
SELECT sku, upper_inf(valid_period) AS upper_inf
FROM prices
WHERE upper_inf(valid_period)
ORDER BY sku;

