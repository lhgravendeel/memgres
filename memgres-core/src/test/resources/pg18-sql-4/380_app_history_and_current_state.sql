DROP SCHEMA IF EXISTS test_380 CASCADE;
CREATE SCHEMA test_380;
SET search_path TO test_380;

CREATE TABLE orders (
    order_id integer PRIMARY KEY,
    customer_name text NOT NULL
);

CREATE TABLE order_status_history (
    history_id integer PRIMARY KEY,
    order_id integer NOT NULL REFERENCES orders(order_id),
    status text NOT NULL,
    changed_at timestamp NOT NULL
);

INSERT INTO orders(order_id, customer_name) VALUES
    (1, 'Acme'),
    (2, 'Beta Corp'),
    (3, 'Cyan LLC');

INSERT INTO order_status_history(history_id, order_id, status, changed_at) VALUES
    (1, 1, 'pending',    TIMESTAMP '2024-01-01 09:00:00'),
    (2, 1, 'paid',       TIMESTAMP '2024-01-01 10:00:00'),
    (3, 1, 'shipped',    TIMESTAMP '2024-01-02 08:00:00'),
    (4, 2, 'pending',    TIMESTAMP '2024-01-01 11:00:00'),
    (5, 2, 'cancelled',  TIMESTAMP '2024-01-01 12:00:00'),
    (6, 3, 'pending',    TIMESTAMP '2024-01-03 09:00:00'),
    (7, 3, 'paid',       TIMESTAMP '2024-01-03 10:00:00');

-- begin-expected
-- columns: order_id|customer_name|current_status|changed_at
-- row: 1|Acme|shipped|2024-01-02 08:00:00
-- row: 2|Beta Corp|cancelled|2024-01-01 12:00:00
-- row: 3|Cyan LLC|paid|2024-01-03 10:00:00
-- end-expected
SELECT o.order_id, o.customer_name, h.status AS current_status, h.changed_at
FROM orders AS o
JOIN LATERAL (
    SELECT status, changed_at
    FROM order_status_history AS osh
    WHERE osh.order_id = o.order_id
    ORDER BY changed_at DESC, history_id DESC
    LIMIT 1
) AS h ON TRUE
ORDER BY o.order_id;

-- begin-expected
-- columns: status|order_count
-- row: cancelled|1
-- row: paid|1
-- row: shipped|1
-- end-expected
SELECT current_status AS status, count(*) AS order_count
FROM (
    SELECT o.order_id,
           first_value(osh.status) OVER (
               PARTITION BY o.order_id
               ORDER BY osh.changed_at DESC, osh.history_id DESC
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           ) AS current_status,
           row_number() OVER (
               PARTITION BY o.order_id
               ORDER BY osh.changed_at DESC, osh.history_id DESC
           ) AS rn
    FROM orders AS o
    JOIN order_status_history AS osh ON osh.order_id = o.order_id
) AS ranked
WHERE rn = 1
GROUP BY current_status
ORDER BY current_status;

-- begin-expected
-- columns: order_id|has_been_paid
-- row: 1|t
-- row: 2|f
-- row: 3|t
-- end-expected
SELECT o.order_id,
       EXISTS (
           SELECT 1
           FROM order_status_history AS osh
           WHERE osh.order_id = o.order_id
             AND osh.status = 'paid'
       ) AS has_been_paid
FROM orders AS o
ORDER BY o.order_id;

DROP SCHEMA test_380 CASCADE;
