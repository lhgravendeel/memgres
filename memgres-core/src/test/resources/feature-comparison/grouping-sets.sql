-- ============================================================================
-- Feature Comparison: GROUPING SETS, ROLLUP, CUBE, GROUPING()
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests GROUP BY extensions: GROUPING SETS for arbitrary grouping
-- combinations, ROLLUP for hierarchical subtotals, CUBE for all
-- combinations, and the GROUPING() function to detect aggregated columns.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS gs_test CASCADE;
CREATE SCHEMA gs_test;
SET search_path = gs_test, public;

CREATE TABLE gs_sales (
  region text,
  product text,
  amount integer
);
INSERT INTO gs_sales VALUES
  ('east', 'widget', 100),
  ('east', 'widget', 150),
  ('east', 'gadget', 200),
  ('west', 'widget', 300),
  ('west', 'gadget', 250),
  ('west', 'gadget', 350);

-- ============================================================================
-- SECTION A: GROUPING SETS
-- ============================================================================

-- ============================================================================
-- 1. Basic GROUPING SETS: region subtotals + grand total
-- ============================================================================

-- begin-expected
-- columns: region, total
-- row: east, 450
-- row: west, 900
-- row: NULL, 1350
-- end-expected
SELECT region, sum(amount) AS total
FROM gs_sales
GROUP BY GROUPING SETS ((region), ())
ORDER BY region NULLS LAST;

-- ============================================================================
-- 2. GROUPING SETS with two grouping columns
-- ============================================================================

-- begin-expected
-- columns: region, product, total
-- row: east, gadget, 200
-- row: east, widget, 250
-- row: west, gadget, 600
-- row: west, widget, 300
-- row: NULL, NULL, 1350
-- end-expected
SELECT region, product, sum(amount) AS total
FROM gs_sales
GROUP BY GROUPING SETS ((region, product), ())
ORDER BY region NULLS LAST, product NULLS LAST;

-- ============================================================================
-- 3. GROUPING SETS with multiple independent groupings
-- ============================================================================

-- note: Groups by region, by product, and grand total — separately
-- begin-expected
-- columns: cnt
-- row: 5
-- end-expected
SELECT count(*)::integer AS cnt FROM (
  SELECT region, product, sum(amount)
  FROM gs_sales
  GROUP BY GROUPING SETS ((region), (product), ())
) sub;

-- ============================================================================
-- 4. GROUPING SETS: empty set only (grand total)
-- ============================================================================

-- begin-expected
-- columns: total
-- row: 1350
-- end-expected
SELECT sum(amount) AS total
FROM gs_sales
GROUP BY GROUPING SETS (());

-- ============================================================================
-- 5. GROUPING SETS with count and avg
-- ============================================================================

-- begin-expected
-- columns: region, cnt, avg_amount
-- row: east, 3, 150
-- row: west, 3, 300
-- row: NULL, 6, 225
-- end-expected
SELECT region,
  count(*)::integer AS cnt,
  avg(amount)::integer AS avg_amount
FROM gs_sales
GROUP BY GROUPING SETS ((region), ())
ORDER BY region NULLS LAST;

-- ============================================================================
-- SECTION B: ROLLUP
-- ============================================================================

-- ============================================================================
-- 6. Basic ROLLUP: hierarchical subtotals
-- ============================================================================

-- note: ROLLUP(region, product) = GROUPING SETS ((region,product), (region), ())
-- begin-expected
-- columns: region, product, total
-- row: east, gadget, 200
-- row: east, widget, 250
-- row: east, NULL, 450
-- row: west, gadget, 600
-- row: west, widget, 300
-- row: west, NULL, 900
-- row: NULL, NULL, 1350
-- end-expected
SELECT region, product, sum(amount) AS total
FROM gs_sales
GROUP BY ROLLUP (region, product)
ORDER BY region NULLS LAST, product NULLS LAST;

-- ============================================================================
-- 7. Single-column ROLLUP (subtotals + grand total)
-- ============================================================================

-- begin-expected
-- columns: region, total
-- row: east, 450
-- row: west, 900
-- row: NULL, 1350
-- end-expected
SELECT region, sum(amount) AS total
FROM gs_sales
GROUP BY ROLLUP (region)
ORDER BY region NULLS LAST;

-- ============================================================================
-- 8. ROLLUP with HAVING
-- ============================================================================

-- begin-expected
-- columns: region, total
-- row: west, 900
-- row: NULL, 1350
-- end-expected
SELECT region, sum(amount) AS total
FROM gs_sales
GROUP BY ROLLUP (region)
HAVING sum(amount) >= 900
ORDER BY region NULLS LAST;

-- ============================================================================
-- SECTION C: CUBE
-- ============================================================================

-- ============================================================================
-- 9. CUBE: all combination subtotals
-- ============================================================================

-- note: CUBE(region, product) = GROUPING SETS ((region,product), (region), (product), ())
-- begin-expected
-- columns: cnt
-- row: 9
-- end-expected
SELECT count(*)::integer AS cnt FROM (
  SELECT region, product, sum(amount)
  FROM gs_sales
  GROUP BY CUBE (region, product)
) sub;

-- ============================================================================
-- 10. CUBE: single column (same as ROLLUP for one col)
-- ============================================================================

-- begin-expected
-- columns: product, total
-- row: gadget, 800
-- row: widget, 550
-- row: NULL, 1350
-- end-expected
SELECT product, sum(amount) AS total
FROM gs_sales
GROUP BY CUBE (product)
ORDER BY product NULLS LAST;

-- ============================================================================
-- 11. CUBE vs ROLLUP: different row counts
-- ============================================================================

-- note: ROLLUP(a,b) = 3 levels; CUBE(a,b) = 4 levels (adds (b) grouping)
-- begin-expected
-- columns: rollup_cnt, cube_cnt
-- row: 7, 9
-- end-expected
SELECT
  (SELECT count(*) FROM (
    SELECT 1 FROM gs_sales GROUP BY ROLLUP (region, product)
  ) r)::integer AS rollup_cnt,
  (SELECT count(*) FROM (
    SELECT 1 FROM gs_sales GROUP BY CUBE (region, product)
  ) c)::integer AS cube_cnt;

-- ============================================================================
-- SECTION D: GROUPING() function
-- ============================================================================

-- ============================================================================
-- 12. GROUPING(): detect aggregated vs grouped rows
-- ============================================================================

-- note: GROUPING(col) = 0 when col is in the current group, 1 when aggregated
-- begin-expected
-- columns: region, total, is_grand_total
-- row: east, 450, 0
-- row: west, 900, 0
-- row: NULL, 1350, 1
-- end-expected
SELECT region, sum(amount) AS total,
  grouping(region)::integer AS is_grand_total
FROM gs_sales
GROUP BY ROLLUP (region)
ORDER BY region NULLS LAST;

-- ============================================================================
-- 13. GROUPING() with multiple columns
-- ============================================================================

-- begin-expected
-- columns: region, product, g_region, g_product
-- row: east, gadget, 0, 0
-- row: east, widget, 0, 0
-- row: east, NULL, 0, 1
-- row: west, gadget, 0, 0
-- row: west, widget, 0, 0
-- row: west, NULL, 0, 1
-- row: NULL, NULL, 1, 1
-- end-expected
SELECT region, product,
  grouping(region)::integer AS g_region,
  grouping(product)::integer AS g_product
FROM gs_sales
GROUP BY ROLLUP (region, product)
ORDER BY region NULLS LAST, product NULLS LAST;

-- ============================================================================
-- 14. Using GROUPING() in HAVING to filter subtotal rows
-- ============================================================================

-- note: Filter to only subtotal rows (where at least one column is aggregated)
-- begin-expected
-- columns: region, product, total
-- row: east, NULL, 450
-- row: west, NULL, 900
-- row: NULL, NULL, 1350
-- end-expected
SELECT region, product, sum(amount) AS total
FROM gs_sales
GROUP BY ROLLUP (region, product)
HAVING grouping(product) = 1
ORDER BY region NULLS LAST;

-- ============================================================================
-- 15. GROUPING() in CUBE
-- ============================================================================

-- begin-expected
-- columns: g_region, g_product, total
-- row: 0, 0, 200
-- row: 0, 0, 250
-- row: 0, 0, 300
-- row: 0, 0, 600
-- row: 0, 1, 450
-- row: 0, 1, 900
-- row: 1, 0, 550
-- row: 1, 0, 800
-- row: 1, 1, 1350
-- end-expected
SELECT
  grouping(region)::integer AS g_region,
  grouping(product)::integer AS g_product,
  sum(amount) AS total
FROM gs_sales
GROUP BY CUBE (region, product)
ORDER BY g_region, g_product, total;

-- ============================================================================
-- SECTION E: Mixed GROUP BY with GROUPING SETS
-- ============================================================================

-- ============================================================================
-- 16. Regular GROUP BY combined with GROUPING SETS
-- ============================================================================

-- note: GROUP BY region, GROUPING SETS ((product), ())
-- means: for each region, show product subtotals and region total
-- begin-expected
-- columns: cnt
-- row: 6
-- end-expected
SELECT count(*)::integer AS cnt FROM (
  SELECT region, product, sum(amount)
  FROM gs_sales
  GROUP BY region, GROUPING SETS ((product), ())
) sub;

-- ============================================================================
-- 17. GROUPING SETS with FILTER
-- ============================================================================

-- begin-expected
-- columns: region, widget_total
-- row: east, 250
-- row: west, 300
-- row: NULL, 550
-- end-expected
SELECT region,
  sum(amount) FILTER (WHERE product = 'widget') AS widget_total
FROM gs_sales
GROUP BY ROLLUP (region)
ORDER BY region NULLS LAST;

-- ============================================================================
-- 18. GROUPING SETS with ORDER BY on aggregate
-- ============================================================================

-- begin-expected
-- columns: region, total
-- row: east, 450
-- row: west, 900
-- row: NULL, 1350
-- end-expected
SELECT region, sum(amount) AS total
FROM gs_sales
GROUP BY ROLLUP (region)
ORDER BY sum(amount);

-- ============================================================================
-- 19. GROUPING SETS with DISTINCT aggregate
-- ============================================================================

-- begin-expected
-- columns: region, distinct_products
-- row: east, 2
-- row: west, 2
-- row: NULL, 2
-- end-expected
SELECT region,
  count(DISTINCT product)::integer AS distinct_products
FROM gs_sales
GROUP BY ROLLUP (region)
ORDER BY region NULLS LAST;

-- ============================================================================
-- 20. Empty ROLLUP() rejected
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
SELECT sum(amount) FROM gs_sales GROUP BY ROLLUP ();

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA gs_test CASCADE;
SET search_path = public;
