DROP SCHEMA IF EXISTS test_870 CASCADE;
CREATE SCHEMA test_870;
SET search_path TO test_870;

CREATE TABLE products (
    product_id integer PRIMARY KEY,
    product_name text NOT NULL,
    price integer NOT NULL,
    category text NOT NULL
);

INSERT INTO products VALUES
(1, 'apple', 10, 'fruit'),
(2, 'banana', 20, 'fruit'),
(3, 'carrot', 30, 'veg');

UPDATE products p
SET price = v.new_price
FROM (VALUES
    (1, 11),
    (3, 35)
) AS v(product_id, new_price)
WHERE p.product_id = v.product_id;

-- begin-expected
-- columns: product_id,price
-- row: 1|11
-- row: 2|20
-- row: 3|35
-- end-expected
SELECT product_id, price
FROM products
ORDER BY product_id;

DELETE FROM products p
USING (VALUES (2)) AS doomed(product_id)
WHERE p.product_id = doomed.product_id;

-- begin-expected
-- columns: product_id,product_name
-- row: 1|apple
-- row: 3|carrot
-- end-expected
SELECT product_id, product_name
FROM products
ORDER BY product_id;

INSERT INTO products(product_id, product_name, price, category)
SELECT *
FROM (VALUES
    (4, 'dates', 40, 'fruit'),
    (5, 'eggplant', 50, 'veg')
) AS v(product_id, product_name, price, category);

-- begin-expected
-- columns: product_id,category
-- row: 1|fruit
-- row: 3|veg
-- row: 4|fruit
-- row: 5|veg
-- end-expected
SELECT product_id, category
FROM products
ORDER BY product_id;

