DROP SCHEMA IF EXISTS test_040 CASCADE;
CREATE SCHEMA test_040;
SET search_path TO test_040;

CREATE TABLE items (
    item_id integer PRIMARY KEY,
    name text,
    qty integer,
    price numeric(10,2),
    active boolean
);

INSERT INTO items VALUES
    (1, 'apple', 10, 1.20, true),
    (2, 'banana', 0, 0.80, false),
    (3, 'carrot', NULL, 0.50, true),
    (4, 'date', 25, 2.50, true);

-- begin-expected
-- columns: item_id|label|qty_state|price_band
-- row: 1|APPLE|has_qty|cheap
-- row: 3|CARROT|unknown_qty|cheap
-- row: 4|DATE|has_qty|premium
-- end-expected
SELECT
    item_id,
    upper(name) AS label,
    CASE
        WHEN qty IS NULL THEN 'unknown_qty'
        WHEN qty = 0 THEN 'empty'
        ELSE 'has_qty'
    END AS qty_state,
    CASE
        WHEN price >= 2.00 THEN 'premium'
        ELSE 'cheap'
    END AS price_band
FROM items
WHERE active = true
ORDER BY item_id;

-- begin-expected
-- columns: item_id|name
-- row: 1|apple
-- row: 4|date
-- end-expected
SELECT item_id, name
FROM items
WHERE qty BETWEEN 1 AND 30
  AND price IN (1.20, 2.50)
ORDER BY 1;

-- begin-expected
-- columns: name|qty_safe
-- row: banana|0
-- row: carrot|-1
-- end-expected
SELECT name, COALESCE(qty, -1) AS qty_safe
FROM items
WHERE qty IS NULL OR qty = 0
ORDER BY name;

-- begin-expected
-- columns: item_id|cmp
-- row: 1|same
-- row: 2|different
-- row: 3|different
-- row: 4|different
-- end-expected
SELECT
    item_id,
    CASE WHEN qty IS NOT DISTINCT FROM 10 THEN 'same' ELSE 'different' END AS cmp
FROM items
ORDER BY item_id;

DROP SCHEMA test_040 CASCADE;
