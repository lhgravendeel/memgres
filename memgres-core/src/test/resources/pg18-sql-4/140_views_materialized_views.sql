DROP SCHEMA IF EXISTS test_140 CASCADE;
CREATE SCHEMA test_140;
SET search_path TO test_140;

CREATE TABLE orders (
    order_id integer PRIMARY KEY,
    customer text NOT NULL,
    amount integer NOT NULL
);

INSERT INTO orders VALUES
    (1, 'alice', 10),
    (2, 'alice', 15),
    (3, 'bob', 7);

CREATE VIEW order_totals AS
SELECT customer, SUM(amount) AS total_amount
FROM orders
GROUP BY customer;

CREATE MATERIALIZED VIEW order_totals_mv AS
SELECT customer, SUM(amount) AS total_amount
FROM orders
GROUP BY customer;

-- begin-expected
-- columns: customer|total_amount
-- row: alice|25
-- row: bob|7
-- end-expected
SELECT customer, total_amount
FROM order_totals
ORDER BY customer;

INSERT INTO orders VALUES (4, 'bob', 8);

-- begin-expected
-- columns: customer|total_amount
-- row: alice|25
-- row: bob|7
-- end-expected
SELECT customer, total_amount
FROM order_totals_mv
ORDER BY customer;

REFRESH MATERIALIZED VIEW order_totals_mv;

-- begin-expected
-- columns: customer|total_amount
-- row: alice|25
-- row: bob|15
-- end-expected
SELECT customer, total_amount
FROM order_totals_mv
ORDER BY customer;

DROP SCHEMA test_140 CASCADE;
