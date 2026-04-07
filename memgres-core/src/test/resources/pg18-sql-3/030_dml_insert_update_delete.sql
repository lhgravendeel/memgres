DROP SCHEMA IF EXISTS test_030 CASCADE;
CREATE SCHEMA test_030;
SET search_path TO test_030;

CREATE TABLE products (
    product_id integer PRIMARY KEY,
    name text NOT NULL,
    price numeric(10,2) NOT NULL,
    stock integer NOT NULL DEFAULT 0,
    category text NOT NULL DEFAULT 'misc'
);

CREATE TABLE incoming_products (
    product_id integer,
    name text,
    price numeric(10,2),
    stock integer,
    category text
);

INSERT INTO products(product_id, name, price, stock, category) VALUES
    (1, 'pen', 1.50, 10, 'office'),
    (2, 'notebook', 3.00, 5, 'office');


INSERT INTO incoming_products VALUES
    (3, 'pencil', 0.50, 20, 'office'),
    (4, 'eraser', 0.75, 15, 'office');

INSERT INTO products(product_id, name, price, stock, category)
SELECT product_id, name, price, stock, category
FROM incoming_products;

-- begin-expected
-- columns: product_id|name|price|stock
-- row: 2|notebook|3.00|7
-- end-expected
UPDATE products
SET stock = stock + 2
WHERE product_id = 2
RETURNING product_id, name, price, stock;

-- begin-expected
-- columns: product_id|name|stock
-- row: 3|pencil|10
-- row: 4|eraser|5
-- end-expected
UPDATE products p
SET stock = p.stock - 10
FROM incoming_products i
WHERE p.product_id = i.product_id
RETURNING p.product_id, p.name, p.stock;

-- begin-expected
-- columns: product_id|name
-- row: 1|pen
-- end-expected
DELETE FROM products
WHERE product_id = 1
RETURNING product_id, name;

-- begin-expected
-- columns: product_id|name|price|stock|category
-- row: 2|notebook|3.00|7|office
-- row: 3|pencil|0.50|10|office
-- row: 4|eraser|0.75|5|office
-- end-expected
SELECT product_id, name, price, stock, category
FROM products
WHERE product_id IS NOT NULL
ORDER BY product_id;

DROP SCHEMA test_030 CASCADE;
