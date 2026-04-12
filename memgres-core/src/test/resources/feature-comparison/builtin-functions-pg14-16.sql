-- ============================================================================
-- Feature Comparison: Missing Built-in Functions PG 14-16 (C1, C2)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Functions: trim_array, string_to_table, array_sample, array_shuffle,
--            range_agg, range_intersect_agg, OVERLAPS syntax.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS bf_test CASCADE;
CREATE SCHEMA bf_test;
SET search_path = bf_test, public;

-- ============================================================================
-- trim_array (PG 14)
-- ============================================================================

-- ============================================================================
-- 1. Basic trim_array: remove last n elements
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {1,2,3}
-- end-expected
SELECT trim_array(ARRAY[1,2,3,4,5], 2) AS result;

-- ============================================================================
-- 2. trim_array: remove 0 elements (no-op)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {1,2,3}
-- end-expected
SELECT trim_array(ARRAY[1,2,3], 0) AS result;

-- ============================================================================
-- 3. trim_array: remove all elements
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {}
-- end-expected
SELECT trim_array(ARRAY[1,2,3], 3) AS result;

-- ============================================================================
-- 4. trim_array: text array
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {a,b}
-- end-expected
SELECT trim_array(ARRAY['a','b','c','d'], 2) AS result;

-- ============================================================================
-- 5. trim_array: n > length (should error)
-- ============================================================================

-- begin-expected-error
-- sqlstate: 2202E
-- message-like: must be between 0 and
-- end-expected-error
SELECT trim_array(ARRAY[1,2], 5);

-- ============================================================================
-- 6. trim_array: negative n (should error)
-- ============================================================================

-- begin-expected-error
-- sqlstate: 2202E
-- message-like: must be between 0 and
-- end-expected-error
SELECT trim_array(ARRAY[1,2], -1);

-- ============================================================================
-- string_to_table (PG 14)
-- ============================================================================

-- ============================================================================
-- 7. Basic string_to_table: split by delimiter
-- ============================================================================

-- begin-expected
-- columns: val
-- row: hello
-- row: world
-- row: foo
-- end-expected
SELECT val FROM string_to_table('hello,world,foo', ',') AS val;

-- ============================================================================
-- 8. string_to_table: multi-char delimiter
-- ============================================================================

-- begin-expected
-- columns: val
-- row: a
-- row: b
-- row: c
-- end-expected
SELECT val FROM string_to_table('a::b::c', '::') AS val;

-- ============================================================================
-- 9. string_to_table: NULL delimiter (split to characters)
-- ============================================================================

-- begin-expected
-- columns: val
-- row: a
-- row: b
-- row: c
-- end-expected
SELECT val FROM string_to_table('abc', NULL) AS val;

-- ============================================================================
-- 10. string_to_table: with null_string parameter
-- ============================================================================

-- begin-expected
-- columns: is_null
-- row: false
-- row: true
-- row: false
-- end-expected
SELECT val IS NULL AS is_null
FROM string_to_table('a,NULL,b', ',', 'NULL') AS val;

-- ============================================================================
-- 11. string_to_table: empty string
-- ============================================================================

-- begin-expected
-- columns: val
-- end-expected
SELECT val FROM string_to_table('', ',') AS val;

-- ============================================================================
-- 12. string_to_table: consecutive delimiters (empty strings)
-- ============================================================================

-- begin-expected
-- columns: val
-- row: a
-- row:
-- row: b
-- end-expected
SELECT val FROM string_to_table('a,,b', ',') AS val;

-- ============================================================================
-- array_sample / array_shuffle (PG 16)
-- ============================================================================

-- ============================================================================
-- 13. array_sample: returns correct number of elements
-- ============================================================================

-- begin-expected
-- columns: len
-- row: 3
-- end-expected
SELECT array_length(array_sample(ARRAY[1,2,3,4,5,6,7,8,9,10], 3), 1) AS len;

-- ============================================================================
-- 14. array_sample: n = 0
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {}
-- end-expected
SELECT array_sample(ARRAY[1,2,3], 0) AS result;

-- ============================================================================
-- 15. array_sample: n = array length
-- ============================================================================

-- begin-expected
-- columns: len
-- row: 3
-- end-expected
SELECT array_length(array_sample(ARRAY[1,2,3], 3), 1) AS len;

-- ============================================================================
-- 16. array_sample: elements come from source array
-- ============================================================================

-- begin-expected
-- columns: all_valid
-- row: true
-- end-expected
SELECT bool_and(elem = ANY(ARRAY[10,20,30,40,50])) AS all_valid
FROM unnest(array_sample(ARRAY[10,20,30,40,50], 3)) AS elem;

-- ============================================================================
-- 17. array_shuffle: same length, same elements
-- ============================================================================

-- begin-expected
-- columns: same_length, same_elements
-- row: true, true
-- end-expected
SELECT
  array_length(array_shuffle(ARRAY[1,2,3,4,5]), 1) = 5 AS same_length,
  (SELECT bool_and(elem = ANY(ARRAY[1,2,3,4,5]))
   FROM unnest(array_shuffle(ARRAY[1,2,3,4,5])) AS elem) AS same_elements;

-- ============================================================================
-- 18. array_shuffle: empty array
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {}
-- end-expected
SELECT array_shuffle('{}'::integer[]) AS result;

-- ============================================================================
-- 19. array_sample: text array
-- ============================================================================

-- begin-expected
-- columns: len
-- row: 2
-- end-expected
SELECT array_length(array_sample(ARRAY['a','b','c','d'], 2), 1) AS len;

-- ============================================================================
-- range_agg / range_intersect_agg (PG 14)
-- ============================================================================

-- ============================================================================
-- 20. range_agg: basic aggregation of ranges
-- ============================================================================

CREATE TABLE bf_ranges (r int4range);
INSERT INTO bf_ranges VALUES ('[1,5)'), ('[3,8)'), ('[10,15)');

-- begin-expected
-- columns: result
-- row: {[1,8),[10,15)}
-- end-expected
SELECT range_agg(r) AS result FROM bf_ranges;

-- ============================================================================
-- 21. range_agg: non-overlapping ranges stay separate
-- ============================================================================

TRUNCATE bf_ranges;
INSERT INTO bf_ranges VALUES ('[1,3)'), ('[5,7)'), ('[9,11)');

-- begin-expected
-- columns: result
-- row: {[1,3),[5,7),[9,11)}
-- end-expected
SELECT range_agg(r) AS result FROM bf_ranges;

-- ============================================================================
-- 22. range_agg: with GROUP BY
-- ============================================================================

CREATE TABLE bf_grouped (category text, r int4range);
INSERT INTO bf_grouped VALUES
  ('A', '[1,5)'), ('A', '[3,8)'),
  ('B', '[10,15)'), ('B', '[20,25)');

-- begin-expected
-- columns: category, ranges
-- row: A, {[1,8)}
-- row: B, {[10,15),[20,25)}
-- end-expected
SELECT category, range_agg(r) AS ranges
FROM bf_grouped
GROUP BY category
ORDER BY category;

DROP TABLE bf_grouped;

-- ============================================================================
-- 23. range_agg: NULL handling
-- ============================================================================

TRUNCATE bf_ranges;
INSERT INTO bf_ranges VALUES ('[1,5)'), (NULL), ('[3,8)');

-- begin-expected
-- columns: result
-- row: {[1,8)}
-- end-expected
SELECT range_agg(r) AS result FROM bf_ranges;

-- ============================================================================
-- 24. range_agg: empty result
-- ============================================================================

TRUNCATE bf_ranges;

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT range_agg(r) IS NULL AS result FROM bf_ranges;

-- ============================================================================
-- 25. range_intersect_agg: basic intersection
-- ============================================================================

TRUNCATE bf_ranges;
INSERT INTO bf_ranges VALUES ('[1,10)'), ('[3,8)'), ('[5,15)');

-- begin-expected
-- columns: result
-- row: [5,8)
-- end-expected
SELECT range_intersect_agg(r) AS result FROM bf_ranges;

-- ============================================================================
-- 26. range_intersect_agg: no overlap → empty
-- ============================================================================

TRUNCATE bf_ranges;
INSERT INTO bf_ranges VALUES ('[1,3)'), ('[5,7)');

-- begin-expected
-- columns: result
-- row: empty
-- end-expected
SELECT range_intersect_agg(r) AS result FROM bf_ranges;

DROP TABLE bf_ranges;

-- ============================================================================
-- SQL OVERLAPS syntax (C2)
-- ============================================================================

-- ============================================================================
-- 27. Date OVERLAPS: overlapping periods
-- ============================================================================

-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT (DATE '2026-01-01', DATE '2026-01-10') OVERLAPS (DATE '2026-01-05', DATE '2026-01-15') AS result;

-- ============================================================================
-- 28. Date OVERLAPS: non-overlapping periods
-- ============================================================================

-- begin-expected
-- columns: result
-- row: f
-- end-expected
SELECT (DATE '2026-01-01', DATE '2026-01-05') OVERLAPS (DATE '2026-01-06', DATE '2026-01-10') AS result;

-- ============================================================================
-- 29. Timestamp OVERLAPS
-- ============================================================================

-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT (TIMESTAMP '2026-01-01 08:00', TIMESTAMP '2026-01-01 17:00')
       OVERLAPS
       (TIMESTAMP '2026-01-01 12:00', TIMESTAMP '2026-01-01 20:00') AS result;

-- ============================================================================
-- 30. OVERLAPS with interval
-- ============================================================================

-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT (DATE '2026-01-01', INTERVAL '5 days') OVERLAPS (DATE '2026-01-03', INTERVAL '5 days') AS result;

-- ============================================================================
-- 31. OVERLAPS: edge case — touching but not overlapping
-- ============================================================================

-- note: Touching endpoints are NOT considered overlapping per SQL standard
-- begin-expected
-- columns: result
-- row: f
-- end-expected
SELECT (DATE '2026-01-01', DATE '2026-01-05') OVERLAPS (DATE '2026-01-05', DATE '2026-01-10') AS result;

-- ============================================================================
-- 32. OVERLAPS: same start, zero-length period
-- ============================================================================

-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT (DATE '2026-01-01', DATE '2026-01-01') OVERLAPS (DATE '2026-01-01', DATE '2026-01-05') AS result;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA bf_test CASCADE;
SET search_path = public;
