-- ============================================================================
-- Feature Comparison: Extended Array Functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests array functions not covered elsewhere: array_fill, array_ndims,
-- array_positions, array_replace, array_remove, array_to_string,
-- string_to_array, cardinality, and array slicing.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS arr_test CASCADE;
CREATE SCHEMA arr_test;
SET search_path = arr_test, public;

-- ============================================================================
-- SECTION A: Array construction functions
-- ============================================================================

-- ============================================================================
-- 1. array_fill: 1D
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {0,0,0}
-- end-expected
SELECT array_fill(0, ARRAY[3]) AS result;

-- ============================================================================
-- 2. array_fill: 2D
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {{7,7},{7,7}}
-- end-expected
SELECT array_fill(7, ARRAY[2, 2]) AS result;

-- ============================================================================
-- 3. array_fill with custom lower bounds
-- ============================================================================

-- begin-expected
-- columns: result
-- row: [2:4]={5,5,5}
-- end-expected
SELECT array_fill(5, ARRAY[3], ARRAY[2]) AS result;

-- ============================================================================
-- SECTION B: Array inspection functions
-- ============================================================================

-- ============================================================================
-- 4. array_ndims
-- ============================================================================

-- begin-expected
-- columns: d1, d2
-- row: 1, 2
-- end-expected
SELECT
  array_ndims(ARRAY[1,2,3]) AS d1,
  array_ndims(ARRAY[[1,2],[3,4]]) AS d2;

-- ============================================================================
-- 5. cardinality: total number of elements
-- ============================================================================

-- begin-expected
-- columns: c1, c2
-- row: 3, 4
-- end-expected
SELECT
  cardinality(ARRAY[1,2,3]) AS c1,
  cardinality(ARRAY[[1,2],[3,4]]) AS c2;

-- ============================================================================
-- 6. array_length: length of specific dimension
-- ============================================================================

-- begin-expected
-- columns: len1, len2_d1, len2_d2
-- row: 3, 2, 2
-- end-expected
SELECT
  array_length(ARRAY[1,2,3], 1) AS len1,
  array_length(ARRAY[[1,2],[3,4]], 1) AS len2_d1,
  array_length(ARRAY[[1,2],[3,4]], 2) AS len2_d2;

-- ============================================================================
-- 7. array_lower and array_upper
-- ============================================================================

-- begin-expected
-- columns: lo, hi
-- row: 1, 4
-- end-expected
SELECT
  array_lower(ARRAY[10,20,30,40], 1) AS lo,
  array_upper(ARRAY[10,20,30,40], 1) AS hi;

-- ============================================================================
-- SECTION C: Array search functions
-- ============================================================================

-- ============================================================================
-- 8. array_position: find first occurrence
-- ============================================================================

-- begin-expected
-- columns: pos
-- row: 3
-- end-expected
SELECT array_position(ARRAY['a','b','c','d'], 'c') AS pos;

-- ============================================================================
-- 9. array_position: not found returns NULL
-- ============================================================================

-- begin-expected
-- columns: pos
-- row: NULL
-- end-expected
SELECT array_position(ARRAY[1,2,3], 99) AS pos;

-- ============================================================================
-- 10. array_position: with starting position
-- ============================================================================

-- begin-expected
-- columns: pos
-- row: 4
-- end-expected
SELECT array_position(ARRAY[1,2,3,1,2,3], 1, 2) AS pos;

-- ============================================================================
-- 11. array_positions: find all occurrences
-- ============================================================================

-- begin-expected
-- columns: positions
-- row: {1,4}
-- end-expected
SELECT array_positions(ARRAY[1,2,3,1,2,3], 1) AS positions;

-- ============================================================================
-- 12. array_positions: no matches
-- ============================================================================

-- begin-expected
-- columns: positions
-- row: {}
-- end-expected
SELECT array_positions(ARRAY[1,2,3], 99) AS positions;

-- ============================================================================
-- SECTION D: Array modification functions
-- ============================================================================

-- ============================================================================
-- 13. array_remove: remove all occurrences
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {1,3}
-- end-expected
SELECT array_remove(ARRAY[1,2,3,2], 2) AS result;

-- ============================================================================
-- 14. array_remove: element not present
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {1,2,3}
-- end-expected
SELECT array_remove(ARRAY[1,2,3], 99) AS result;

-- ============================================================================
-- 15. array_replace: replace all occurrences
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {1,99,3,99}
-- end-expected
SELECT array_replace(ARRAY[1,2,3,2], 2, 99) AS result;

-- ============================================================================
-- 16. array_replace: no matches
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {1,2,3}
-- end-expected
SELECT array_replace(ARRAY[1,2,3], 99, 0) AS result;

-- ============================================================================
-- SECTION E: Array conversion functions
-- ============================================================================

-- ============================================================================
-- 17. array_to_string: join with delimiter
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 1,2,3
-- end-expected
SELECT array_to_string(ARRAY[1,2,3], ',') AS result;

-- ============================================================================
-- 18. array_to_string with null replacement
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 1,N/A,3
-- end-expected
SELECT array_to_string(ARRAY[1,NULL,3], ',', 'N/A') AS result;

-- ============================================================================
-- 19. string_to_array: split string
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {a,b,c}
-- end-expected
SELECT string_to_array('a,b,c', ',') AS result;

-- ============================================================================
-- 20. string_to_array with NULL replacement
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {a,NULL,c}
-- end-expected
SELECT string_to_array('a,,c', ',', '') AS result;

-- ============================================================================
-- 21. string_to_array: NULL delimiter (split each char)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {a,b,c}
-- end-expected
SELECT string_to_array('abc', NULL) AS result;

-- ============================================================================
-- SECTION F: Array slicing
-- ============================================================================

-- ============================================================================
-- 22. Array slice: subarray
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {2,3}
-- end-expected
SELECT (ARRAY[1,2,3,4,5])[2:3] AS result;

-- ============================================================================
-- 23. Array slice: open-ended (from start)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {1,2,3}
-- end-expected
SELECT (ARRAY[1,2,3,4,5])[:3] AS result;

-- ============================================================================
-- 24. Array slice: open-ended (to end)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {3,4,5}
-- end-expected
SELECT (ARRAY[1,2,3,4,5])[3:] AS result;

-- ============================================================================
-- SECTION G: Array aggregation
-- ============================================================================

-- ============================================================================
-- 25. array_agg: basic
-- ============================================================================

CREATE TABLE arr_data (id integer, val text);
INSERT INTO arr_data VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- begin-expected
-- columns: result
-- row: {a,b,c}
-- end-expected
SELECT array_agg(val ORDER BY id) AS result FROM arr_data;

-- ============================================================================
-- 26. array_agg with DISTINCT
-- ============================================================================

INSERT INTO arr_data VALUES (4, 'a'), (5, 'b');

-- begin-expected
-- columns: result
-- row: {a,b,c}
-- end-expected
SELECT array_agg(DISTINCT val ORDER BY val) AS result FROM arr_data;

-- ============================================================================
-- 27. unnest + array_agg roundtrip
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {1,1,3,4,5}
-- end-expected
SELECT array_agg(x ORDER BY x) AS result
FROM unnest(ARRAY[3,1,4,1,5]) AS x;

-- ============================================================================
-- 28. Array comparison
-- ============================================================================

-- begin-expected
-- columns: eq, lt, gt
-- row: true, true, false
-- end-expected
SELECT
  ARRAY[1,2,3] = ARRAY[1,2,3] AS eq,
  ARRAY[1,2,3] < ARRAY[1,2,4] AS lt,
  ARRAY[1,2,3] > ARRAY[1,2,4] AS gt;

-- ============================================================================
-- 29. Array with NULL elements
-- ============================================================================

-- begin-expected
-- columns: has_null
-- row: true
-- end-expected
SELECT ARRAY[1, NULL, 3] IS NOT NULL AS has_null;

-- begin-expected
-- columns: elem_is_null
-- row: true
-- end-expected
SELECT (ARRAY[1, NULL, 3])[2] IS NULL AS elem_is_null;

-- ============================================================================
-- 30. Empty array operations
-- ============================================================================

-- begin-expected
-- columns: len, card
-- row: NULL, 0
-- end-expected
SELECT
  array_length(ARRAY[]::integer[], 1) AS len,
  cardinality(ARRAY[]::integer[]) AS card;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA arr_test CASCADE;
SET search_path = public;
