DROP SCHEMA IF EXISTS test_060 CASCADE;
CREATE SCHEMA test_060;
SET search_path TO test_060;

CREATE TABLE sales (
    region text,
    product text,
    amount integer
);

INSERT INTO sales VALUES
    ('EU', 'A', 10),
    ('EU', 'A', 5),
    ('EU', 'B', 7),
    ('US', 'A', 3),
    ('US', 'B', 11),
    ('US', 'B', NULL);

-- begin-expected
-- columns: region|cnt_all|cnt_amount|sum_amount|avg_amount|min_amount|max_amount
-- row: EU|3|3|22|7.3333333333333333|5|10
-- row: US|3|2|14|7.0000000000000000|3|11
-- end-expected
SELECT
    region,
    COUNT(*) AS cnt_all,
    COUNT(amount) AS cnt_amount,
    SUM(amount) AS sum_amount,
    AVG(amount) AS avg_amount,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount
FROM sales
GROUP BY region
ORDER BY region;

-- begin-expected
-- columns: region|big_sales
-- row: EU|1
-- row: US|1
-- end-expected
SELECT
    region,
    COUNT(*) FILTER (WHERE amount >= 10) AS big_sales
FROM sales
GROUP BY region
ORDER BY region;

-- begin-expected
-- columns: region|product|total
-- row: EU|A|15
-- row: EU|B|7
-- row: US|B|11
-- end-expected
SELECT region, product, SUM(amount) AS total
FROM sales
GROUP BY region, product
HAVING SUM(amount) > 6
ORDER BY region, product;

-- begin-expected
-- columns: region|product|total
-- row: EU|A|15
-- row: EU|B|7
-- row: EU||22
-- row: US|A|3
-- row: US|B|11
-- row: US||14
-- row: ||36
-- end-expected
SELECT region, product, SUM(amount) AS total
FROM sales
GROUP BY ROLLUP(region, product)
ORDER BY region NULLS LAST, product NULLS LAST;

DROP SCHEMA test_060 CASCADE;
