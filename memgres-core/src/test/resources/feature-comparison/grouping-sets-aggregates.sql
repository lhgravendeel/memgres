-- ============================================================================
-- Feature Comparison: GROUPING() bitmask, grouping-set select-list expressions,
--                     value-semantic DISTINCT aggregates, hypothetical cume_dist
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests the GROUPING() multi-argument bitmask, evaluation of select-list
-- expressions over grouped columns per grouping set (ROLLUP / composite
-- ROLLUP), DISTINCT aggregates deduplicating numerics by value rather than
-- representation (1.0 vs 1.00), and hypothetical-set cume_dist including the
-- hypothetical row in numerator and denominator.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS gsagg_test CASCADE;
CREATE SCHEMA gsagg_test;
SET search_path = gsagg_test, public;

CREATE TABLE gsa_sales (
  region text,
  product text,
  amount integer
);
INSERT INTO gsa_sales VALUES
  ('east', 'widget', 10),
  ('east', 'gadget', 20),
  ('west', 'widget', 40),
  ('west', 'gadget', 80);

CREATE TABLE gsa_pairs (a integer, b integer);
INSERT INTO gsa_pairs VALUES (1, 10), (1, 20), (2, 30);

CREATE TABLE gsa_nums (x numeric);
INSERT INTO gsa_nums VALUES (1.0), (1.00), (2);

CREATE TABLE gsa_empty (x integer);

-- ============================================================================
-- SECTION A: GROUPING() multi-argument bitmask
-- ============================================================================

-- ============================================================================
-- 1. grouping(a, b) over ROLLUP: 0 for detail, 1 for a-subtotals, 3 for total
-- ============================================================================

-- note: bit i (from the left, most significant first) is 1 when argument i
-- note: is NOT grouped in the current grouping set
-- begin-expected
-- columns: region, product, g, total
-- row: east, gadget, 0, 20
-- row: east, widget, 0, 10
-- row: east, NULL, 1, 30
-- row: west, gadget, 0, 80
-- row: west, widget, 0, 40
-- row: west, NULL, 1, 120
-- row: NULL, NULL, 3, 150
-- end-expected
SELECT region, product, grouping(region, product) AS g, sum(amount) AS total
FROM gsa_sales
GROUP BY ROLLUP (region, product)
ORDER BY region NULLS LAST, product NULLS LAST;

-- ============================================================================
-- 2. grouping(a, b) over CUBE: all four masks 0, 1, 2, 3
-- ============================================================================

-- begin-expected
-- columns: g, cnt
-- row: 0, 1
-- row: 0, 1
-- row: 0, 1
-- row: 0, 1
-- row: 1, 2
-- row: 1, 2
-- row: 2, 2
-- row: 2, 2
-- row: 3, 4
-- end-expected
SELECT grouping(region, product) AS g, count(*) AS cnt
FROM gsa_sales
GROUP BY CUBE (region, product)
ORDER BY g, cnt;

-- ============================================================================
-- 3. grouping() with three arguments over ROLLUP: masks 0, 1, 3, 7
-- ============================================================================

-- begin-expected
-- columns: g, cnt
-- row: 0, 1
-- row: 0, 1
-- row: 0, 1
-- row: 0, 1
-- row: 1, 1
-- row: 1, 1
-- row: 1, 1
-- row: 1, 1
-- row: 3, 2
-- row: 3, 2
-- row: 7, 4
-- end-expected
SELECT grouping(region, product, amount) AS g, count(*) AS cnt
FROM gsa_sales
GROUP BY ROLLUP (region, product, amount)
ORDER BY g, cnt;

-- ============================================================================
-- 4. grouping() bitmask over explicit GROUPING SETS
-- ============================================================================

-- note: set (region): product not grouped -> g = 01 = 1
-- note: set (product): region not grouped -> g = 10 = 2
-- begin-expected
-- columns: gr, gp, g, cnt
-- row: 0, 1, 1, 2
-- row: 0, 1, 1, 2
-- row: 1, 0, 2, 2
-- row: 1, 0, 2, 2
-- end-expected
SELECT grouping(region) AS gr, grouping(product) AS gp,
  grouping(region, product) AS g, count(*) AS cnt
FROM gsa_sales
GROUP BY GROUPING SETS ((region), (product))
ORDER BY g, cnt;

-- ============================================================================
-- 5. GROUPING() disambiguation idiom with CASE
-- ============================================================================

-- begin-expected
-- columns: label, total
-- row: east/widget, 10
-- row: east/gadget, 20
-- row: east subtotal, 30
-- row: west/widget, 40
-- row: west/gadget, 80
-- row: west subtotal, 120
-- row: grand total, 150
-- end-expected
SELECT CASE WHEN grouping(region, product) = 3 THEN 'grand total'
            WHEN grouping(region, product) = 1 THEN region || ' subtotal'
            ELSE region || '/' || product END AS label,
  sum(amount) AS total
FROM gsa_sales
GROUP BY ROLLUP (region, product)
ORDER BY sum(amount);

-- ============================================================================
-- SECTION B: Select-list expressions over grouping sets
-- ============================================================================

-- ============================================================================
-- 6. Expression over a grouped column evaluates per set, NULL in grand total
-- ============================================================================

-- begin-expected
-- columns: ap, c
-- row: 11, 2
-- row: 12, 1
-- row: NULL, 3
-- end-expected
SELECT a + 10 AS ap, count(*) AS c
FROM gsa_pairs
GROUP BY ROLLUP (a)
ORDER BY ap NULLS LAST;

-- ============================================================================
-- 7. ROLLUP(a, b): subtotal rows keep the values of columns still grouped
-- ============================================================================

-- begin-expected
-- columns: a, b, c
-- row: 1, 10, 1
-- row: 1, 20, 1
-- row: 1, NULL, 2
-- row: 2, 30, 1
-- row: 2, NULL, 1
-- row: NULL, NULL, 3
-- end-expected
SELECT a, b, count(*) AS c
FROM gsa_pairs
GROUP BY ROLLUP (a, b)
ORDER BY a NULLS LAST, b NULLS LAST;

-- ============================================================================
-- 8. Composite ROLLUP((a, b)): detail rows keep real values for a and b
-- ============================================================================

-- begin-expected
-- columns: a, b, c
-- row: 1, 10, 1
-- row: 1, 20, 1
-- row: 2, 30, 1
-- row: NULL, NULL, 3
-- end-expected
SELECT a, b, count(*) AS c
FROM gsa_pairs
GROUP BY ROLLUP ((a, b))
ORDER BY a NULLS LAST, b NULLS LAST;

-- ============================================================================
-- 9. Composite ROLLUP((a, b)): member columns report grouping() = 0 in detail
-- ============================================================================

-- begin-expected
-- columns: a, b, ga, gb
-- row: 1, 10, 0, 0
-- row: 1, 20, 0, 0
-- row: 2, 30, 0, 0
-- row: NULL, NULL, 1, 1
-- end-expected
SELECT a, b, grouping(a) AS ga, grouping(b) AS gb
FROM gsa_pairs
GROUP BY ROLLUP ((a, b))
ORDER BY a NULLS LAST, b NULLS LAST;

-- ============================================================================
-- SECTION C: DISTINCT aggregates deduplicate numerics by value
-- ============================================================================

-- ============================================================================
-- 10. numeric 1.0 and 1.00 are one distinct value
-- ============================================================================

-- begin-expected
-- columns: c, s
-- row: 1, 1.0
-- end-expected
SELECT count(DISTINCT x) AS c, sum(DISTINCT x) AS s
FROM (VALUES (1.0::numeric), (1.00::numeric)) v(x);

-- ============================================================================
-- 11. count / sum DISTINCT over a table with mixed-scale numerics
-- ============================================================================

-- begin-expected
-- columns: c, s
-- row: 2, 3.0
-- end-expected
SELECT count(DISTINCT x) AS c, sum(DISTINCT x) AS s
FROM gsa_nums;

-- ============================================================================
-- 12. avg DISTINCT over mixed-scale numerics
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 1.50
-- end-expected
SELECT round(avg(DISTINCT x), 2) AS a
FROM gsa_nums;

-- ============================================================================
-- 13. array_agg DISTINCT over mixed-scale numerics
-- ============================================================================

-- begin-expected
-- columns: aa
-- row: {1.0,2}
-- end-expected
SELECT array_agg(DISTINCT x) AS aa
FROM gsa_nums;

-- ============================================================================
-- 14. Values that differ only beyond trailing zeros stay distinct
-- ============================================================================

-- begin-expected
-- columns: c, s
-- row: 2, 2.01
-- end-expected
SELECT count(DISTINCT x) AS c, sum(DISTINCT x) AS s
FROM (VALUES (1.0::numeric), (1.01::numeric), (1.010::numeric)) v(x);

-- ============================================================================
-- SECTION D: Hypothetical-set cume_dist
-- ============================================================================

-- ============================================================================
-- 15. cume_dist includes the hypothetical row: (countLE + 1) / (N + 1)
-- ============================================================================

-- begin-expected
-- columns: cd
-- row: 0.75
-- end-expected
SELECT cume_dist(2) WITHIN GROUP (ORDER BY x) AS cd
FROM (VALUES (1), (2), (3)) v(x);

-- ============================================================================
-- 16. cume_dist with duplicate values
-- ============================================================================

-- begin-expected
-- columns: cd
-- row: 0.8
-- end-expected
SELECT cume_dist(2) WITHIN GROUP (ORDER BY x) AS cd
FROM (VALUES (1), (2), (2), (3)) v(x);

-- ============================================================================
-- 17. cume_dist over an empty group is (0 + 1) / (0 + 1) = 1
-- ============================================================================

-- begin-expected
-- columns: cd
-- row: 1
-- end-expected
SELECT cume_dist(2) WITHIN GROUP (ORDER BY x) AS cd
FROM gsa_empty;

-- ============================================================================
-- 18. rank / dense_rank / percent_rank hypothetical variants unchanged
-- ============================================================================

-- begin-expected
-- columns: r, dr, pr
-- row: 2, 2, 0.3333333333333333
-- end-expected
SELECT rank(2) WITHIN GROUP (ORDER BY x) AS r,
  dense_rank(2) WITHIN GROUP (ORDER BY x) AS dr,
  percent_rank(2) WITHIN GROUP (ORDER BY x) AS pr
FROM (VALUES (1), (2), (3)) v(x);

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA gsagg_test CASCADE;
SET search_path = public;
