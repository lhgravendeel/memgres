-- ============================================================
-- 04: Aggregate Functions and GROUP BY
-- ============================================================

-- Setup
CREATE TABLE sales (id serial PRIMARY KEY, product text NOT NULL, category text NOT NULL, amount numeric(10,2) NOT NULL, quantity int NOT NULL, sold_at date NOT NULL DEFAULT CURRENT_DATE);
INSERT INTO sales (product, category, amount, quantity, sold_at) VALUES
  ('Widget A', 'hardware', 25.00, 10, '2024-01-15'),
  ('Widget B', 'hardware', 35.00, 5, '2024-01-20'),
  ('Service X', 'services', 100.00, 1, '2024-02-01'),
  ('Service Y', 'services', 200.00, 2, '2024-02-15'),
  ('Widget A', 'hardware', 25.00, 8, '2024-03-01'),
  ('Gadget C', 'electronics', 50.00, 20, '2024-03-10'),
  ('Gadget D', 'electronics', 75.00, 15, '2024-03-15'),
  ('Service X', 'services', 100.00, 3, '2024-04-01'),
  ('Widget A', 'hardware', 25.00, 12, '2024-04-15'),
  ('Gadget C', 'electronics', 50.00, 7, '2024-05-01');

-- === Basic aggregates ===
SELECT COUNT(*) FROM sales;
SELECT COUNT(DISTINCT product) FROM sales;
SELECT COUNT(DISTINCT category) FROM sales;
SELECT SUM(amount) FROM sales;
SELECT SUM(amount * quantity) AS total_revenue FROM sales;
SELECT AVG(amount) FROM sales;
SELECT AVG(amount)::numeric(10,2) FROM sales;
SELECT MIN(amount) FROM sales;
SELECT MAX(amount) FROM sales;
SELECT MIN(sold_at) FROM sales;
SELECT MAX(sold_at) FROM sales;

-- === GROUP BY ===
SELECT category, COUNT(*) FROM sales GROUP BY category ORDER BY category;
SELECT category, SUM(amount) FROM sales GROUP BY category ORDER BY SUM(amount) DESC;
SELECT category, AVG(amount)::numeric(10,2) AS avg_price FROM sales GROUP BY category ORDER BY category;
SELECT category, MIN(amount), MAX(amount) FROM sales GROUP BY category ORDER BY category;
SELECT product, COUNT(*), SUM(quantity) FROM sales GROUP BY product ORDER BY product;
SELECT category, product, SUM(quantity) FROM sales GROUP BY category, product ORDER BY category, product;
SELECT EXTRACT(month FROM sold_at) AS month, SUM(amount) FROM sales GROUP BY month ORDER BY month;

-- === GROUP BY with HAVING ===
SELECT category, COUNT(*) FROM sales GROUP BY category HAVING COUNT(*) > 2 ORDER BY category;
SELECT product, SUM(quantity) FROM sales GROUP BY product HAVING SUM(quantity) > 10 ORDER BY product;
SELECT category, AVG(amount) FROM sales GROUP BY category HAVING AVG(amount) > 50 ORDER BY category;

-- === GROUP BY with ordinals and aliases ===
SELECT category, COUNT(*) AS cnt FROM sales GROUP BY 1 ORDER BY 2 DESC;
SELECT category AS cat, SUM(amount) AS total FROM sales GROUP BY cat ORDER BY total DESC;

-- === String aggregate ===
SELECT category, STRING_AGG(DISTINCT product, ', ' ORDER BY product) FROM sales GROUP BY category ORDER BY category;
SELECT STRING_AGG(product, ' | ') FROM sales;

-- === Array aggregate ===
SELECT category, ARRAY_AGG(DISTINCT product ORDER BY product) FROM sales GROUP BY category ORDER BY category;
SELECT ARRAY_AGG(amount ORDER BY amount) FROM sales;

-- === Bool aggregates ===
CREATE TABLE flags (id serial PRIMARY KEY, label text, flag boolean);
INSERT INTO flags (label, flag) VALUES ('a', true), ('b', true), ('c', false), ('d', true), ('e', NULL);
SELECT BOOL_AND(flag) FROM flags;
SELECT BOOL_OR(flag) FROM flags;
SELECT BOOL_AND(flag) FROM flags WHERE flag IS NOT NULL;
SELECT label, BOOL_AND(flag) FROM flags GROUP BY label ORDER BY label;
DROP TABLE flags;

-- === COUNT with FILTER ===
SELECT COUNT(*) FILTER (WHERE category = 'hardware') AS hw_count,
       COUNT(*) FILTER (WHERE category = 'services') AS svc_count,
       COUNT(*) FILTER (WHERE category = 'electronics') AS elec_count
FROM sales;

-- === SUM with FILTER ===
SELECT SUM(amount) FILTER (WHERE category = 'hardware') AS hw_total,
       SUM(amount) FILTER (WHERE category = 'services') AS svc_total
FROM sales;

-- === Aggregate with ORDER BY inside ===
SELECT ARRAY_AGG(product ORDER BY amount DESC) FROM sales;
SELECT STRING_AGG(product, ', ' ORDER BY sold_at) FROM sales;
SELECT JSON_AGG(product ORDER BY product) FROM sales;

-- === Aggregate on empty set ===
SELECT COUNT(*) FROM sales WHERE 1 = 0;
SELECT SUM(amount) FROM sales WHERE 1 = 0;
SELECT AVG(amount) FROM sales WHERE 1 = 0;
SELECT MIN(amount) FROM sales WHERE 1 = 0;
SELECT MAX(amount) FROM sales WHERE 1 = 0;
SELECT ARRAY_AGG(product) FROM sales WHERE 1 = 0;
SELECT STRING_AGG(product, ',') FROM sales WHERE 1 = 0;

-- === INVALID: Aggregate errors ===
-- Non-aggregated column without GROUP BY
SELECT category, COUNT(*) FROM sales;
-- HAVING without GROUP BY (valid in PG if entire table is one group)
SELECT COUNT(*) FROM sales HAVING COUNT(*) > 1;
-- Aggregate in WHERE
SELECT * FROM sales WHERE SUM(amount) > 100;
-- Nested aggregate
SELECT MAX(COUNT(*)) FROM sales GROUP BY category;

-- Cleanup
DROP TABLE sales;
