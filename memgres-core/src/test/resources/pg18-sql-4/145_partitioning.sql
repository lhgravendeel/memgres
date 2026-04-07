DROP SCHEMA IF EXISTS test_145 CASCADE;
CREATE SCHEMA test_145;
SET search_path TO test_145;

CREATE TABLE sales (
    sale_id integer NOT NULL,
    sale_date date NOT NULL,
    region text NOT NULL,
    amount integer NOT NULL,
    PRIMARY KEY (sale_id, sale_date)
) PARTITION BY RANGE (sale_date);

CREATE TABLE sales_2024 PARTITION OF sales
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE sales_2025 PARTITION OF sales
FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

INSERT INTO sales(sale_id, sale_date, region, amount) VALUES
    (1, '2024-03-15', 'north', 100),
    (2, '2024-12-31', 'south', 200),
    (3, '2025-01-01', 'north', 300),
    (4, '2025-06-20', 'west', 400);

-- begin-expected
-- columns: sale_id|partition_name
-- row: 1|sales_2024
-- row: 2|sales_2024
-- row: 3|sales_2025
-- row: 4|sales_2025
-- end-expected
SELECT sale_id, tableoid::regclass::text AS partition_name
FROM sales
ORDER BY sale_id;

-- begin-expected
-- columns: year_bucket|cnt|total_amount
-- row: 2024|2|300
-- row: 2025|2|700
-- end-expected
SELECT EXTRACT(YEAR FROM sale_date)::integer AS year_bucket,
       count(*) AS cnt,
       sum(amount) AS total_amount
FROM sales
GROUP BY EXTRACT(YEAR FROM sale_date)
ORDER BY year_bucket;

CREATE TABLE sales_2026 (
    sale_id integer NOT NULL,
    sale_date date NOT NULL,
    region text NOT NULL,
    amount integer NOT NULL,
    PRIMARY KEY (sale_id, sale_date)
);

ALTER TABLE sales ATTACH PARTITION sales_2026
FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

INSERT INTO sales VALUES (5, '2026-02-02', 'east', 500);

-- begin-expected
-- columns: sale_id|partition_name
-- row: 5|sales_2026
-- end-expected
SELECT sale_id, tableoid::regclass::text AS partition_name
FROM sales
WHERE sale_id = 5;

ALTER TABLE sales DETACH PARTITION sales_2026;

-- begin-expected
-- columns: sale_id|sale_date|region|amount
-- row: 5|2026-02-02|east|500
-- end-expected
SELECT sale_id, sale_date, region, amount
FROM sales_2026;

DROP SCHEMA test_145 CASCADE;
