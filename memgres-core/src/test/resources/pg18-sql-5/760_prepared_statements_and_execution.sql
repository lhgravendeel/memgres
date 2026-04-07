DROP SCHEMA IF EXISTS test_760 CASCADE;
CREATE SCHEMA test_760;
SET search_path TO test_760;

CREATE TABLE items (
    item_id integer PRIMARY KEY,
    item_name text NOT NULL,
    category text NOT NULL
);

INSERT INTO items VALUES
(1, 'apple', 'fruit'),
(2, 'banana', 'fruit'),
(3, 'carrot', 'veg');

PREPARE get_item(integer) AS
SELECT item_name, category
FROM items
WHERE item_id = $1;

-- begin-expected
-- columns: item_name,category
-- row: apple|fruit
-- end-expected
EXECUTE get_item(1);

-- begin-expected
-- columns: item_name,category
-- row: carrot|veg
-- end-expected
EXECUTE get_item(3);

PREPARE count_by_category(text) AS
SELECT COUNT(*) AS cnt
FROM items
WHERE category = $1;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
EXECUTE count_by_category('fruit');

DEALLOCATE get_item;
DEALLOCATE count_by_category;

