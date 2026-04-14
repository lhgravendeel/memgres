-- ============================================================================
-- Feature Comparison: Multirange Operators and Functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests multirange-specific operations including the || (union) operator,
-- - (difference), * (intersection), and multirange functions.
-- Extends multirange-types.sql which covers type basics.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS mrop_test CASCADE;
CREATE SCHEMA mrop_test;
SET search_path = mrop_test, public;

-- ============================================================================
-- SECTION A: Multirange Union (||)
-- ============================================================================

-- ============================================================================
-- 1. int4multirange union: non-overlapping
-- ============================================================================

-- note: PG 18 does not support the || operator for multiranges.
-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT '{[1,4)}'::int4multirange || '{[6,9)}'::int4multirange AS result;

-- ============================================================================
-- 2. int4multirange union: overlapping ranges merge
-- ============================================================================

-- note: PG 18 does not support the || operator for multiranges.
-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT '{[1,5)}'::int4multirange || '{[3,8)}'::int4multirange AS result;

-- ============================================================================
-- 3. int4multirange union: adjacent ranges merge
-- ============================================================================

-- note: PG 18 does not support the || operator for multiranges.
-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT '{[1,3)}'::int4multirange || '{[3,6)}'::int4multirange AS result;

-- ============================================================================
-- 4. int4multirange union: range + multirange
-- ============================================================================

-- note: PG 18 does not support the || operator for multiranges.
-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT '{[1,4)}'::int4multirange || '[6,9)'::int4range AS result;

-- ============================================================================
-- 5. int4multirange union: empty multirange
-- ============================================================================

-- note: PG 18 does not support the || operator for multiranges.
-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT '{}'::int4multirange || '{[1,5)}'::int4multirange AS result;

-- ============================================================================
-- 6. int8multirange union
-- ============================================================================

-- note: PG 18 does not support the || operator for multiranges.
-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT '{[1,5)}'::int8multirange || '{[3,10)}'::int8multirange AS result;

-- ============================================================================
-- 7. nummultirange union
-- ============================================================================

-- note: PG 18 does not support the || operator for multiranges.
-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT '{[1.5,4.0)}'::nummultirange || '{[3.0,7.5)}'::nummultirange AS result;

-- ============================================================================
-- 8. datemultirange union
-- ============================================================================

-- note: PG 18 does not support the || operator for multiranges.
-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT
  '{[2024-01-01,2024-03-01)}'::datemultirange ||
  '{[2024-06-01,2024-09-01)}'::datemultirange AS result;

-- ============================================================================
-- 9. tsmultirange union
-- ============================================================================

-- note: PG 18 does not support the || operator for multiranges.
-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT
  '{["2024-01-01","2024-01-02")}'::tsmultirange ||
  '{["2024-01-02","2024-01-03")}'::tsmultirange AS result;

-- ============================================================================
-- SECTION B: Multirange Intersection (*)
-- ============================================================================

-- ============================================================================
-- 10. int4multirange intersection
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[3,5)}
-- end-expected
SELECT '{[1,5)}'::int4multirange * '{[3,8)}'::int4multirange AS result;

-- ============================================================================
-- 11. int4multirange intersection: no overlap
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {}
-- end-expected
SELECT '{[1,3)}'::int4multirange * '{[5,8)}'::int4multirange AS result;

-- ============================================================================
-- 12. int4multirange intersection: multiple ranges
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[2,3),[5,7)}
-- end-expected
SELECT '{[1,3),[5,7)}'::int4multirange * '{[2,10)}'::int4multirange AS result;

-- ============================================================================
-- SECTION C: Multirange Difference (-)
-- ============================================================================

-- ============================================================================
-- 13. int4multirange difference
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[1,3)}
-- end-expected
SELECT '{[1,5)}'::int4multirange - '{[3,8)}'::int4multirange AS result;

-- ============================================================================
-- 14. int4multirange difference: complete removal
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {}
-- end-expected
SELECT '{[3,5)}'::int4multirange - '{[1,8)}'::int4multirange AS result;

-- ============================================================================
-- 15. int4multirange difference: hole in middle
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[1,3),[7,10)}
-- end-expected
SELECT '{[1,10)}'::int4multirange - '{[3,7)}'::int4multirange AS result;

-- ============================================================================
-- SECTION D: Containment Operators
-- ============================================================================

-- ============================================================================
-- 16. multirange @> value
-- ============================================================================

-- begin-expected
-- columns: inside, outside
-- row: true, false
-- end-expected
SELECT
  '{[1,5),[10,15)}'::int4multirange @> 3 AS inside,
  '{[1,5),[10,15)}'::int4multirange @> 7 AS outside;

-- ============================================================================
-- 17. multirange @> range
-- ============================================================================

-- begin-expected
-- columns: contains, not_contains
-- row: true, false
-- end-expected
SELECT
  '{[1,10)}'::int4multirange @> '[3,5)'::int4range AS contains,
  '{[1,5)}'::int4multirange @> '[3,8)'::int4range AS not_contains;

-- ============================================================================
-- 18. multirange @> multirange
-- ============================================================================

-- begin-expected
-- columns: contains
-- row: true
-- end-expected
SELECT '{[1,20)}'::int4multirange @> '{[3,5),[10,15)}'::int4multirange AS contains;

-- ============================================================================
-- 19. value <@ multirange
-- ============================================================================

-- begin-expected
-- columns: in_range
-- row: true
-- end-expected
SELECT 12 <@ '{[1,5),[10,15)}'::int4multirange AS in_range;

-- ============================================================================
-- SECTION E: Overlap and adjacency
-- ============================================================================

-- ============================================================================
-- 20. multirange && multirange (overlap)
-- ============================================================================

-- begin-expected
-- columns: overlaps, no_overlap
-- row: true, false
-- end-expected
SELECT
  '{[1,5)}'::int4multirange && '{[3,8)}'::int4multirange AS overlaps,
  '{[1,3)}'::int4multirange && '{[5,8)}'::int4multirange AS no_overlap;

-- ============================================================================
-- 21. multirange -|- multirange (adjacent)
-- ============================================================================

-- begin-expected
-- columns: adjacent, not_adjacent
-- row: true, false
-- end-expected
SELECT
  '{[1,3)}'::int4multirange -|- '{[3,5)}'::int4multirange AS adjacent,
  '{[1,3)}'::int4multirange -|- '{[5,8)}'::int4multirange AS not_adjacent;

-- ============================================================================
-- SECTION F: Multirange functions
-- ============================================================================

-- ============================================================================
-- 22. lower() and upper()
-- ============================================================================

-- begin-expected
-- columns: lo, hi
-- row: 1, 15
-- end-expected
SELECT
  lower('{[1,5),[10,15)}'::int4multirange) AS lo,
  upper('{[1,5),[10,15)}'::int4multirange) AS hi;

-- ============================================================================
-- 23. isempty()
-- ============================================================================

-- begin-expected
-- columns: empty, not_empty
-- row: true, false
-- end-expected
SELECT
  isempty('{}'::int4multirange) AS empty,
  isempty('{[1,5)}'::int4multirange) AS not_empty;

-- ============================================================================
-- 24. range_agg(): aggregate ranges into multirange
-- ============================================================================

CREATE TABLE mrop_ranges (r int4range);
INSERT INTO mrop_ranges VALUES ('[1,3)'), ('[5,8)'), ('[2,4)');

-- begin-expected
-- columns: result
-- row: {[1,4),[5,8)}
-- end-expected
SELECT range_agg(r) AS result FROM mrop_ranges;

-- ============================================================================
-- 25. unnest(): expand multirange to ranges
-- ============================================================================

-- begin-expected
-- columns: r
-- row: [1,5)
-- row: [10,15)
-- end-expected
SELECT unnest('{[1,5),[10,15)}'::int4multirange) AS r;

-- ============================================================================
-- 26. multirange in table column
-- ============================================================================

CREATE TABLE mrop_schedule (
  id integer PRIMARY KEY,
  name text,
  available int4multirange
);
INSERT INTO mrop_schedule VALUES
  (1, 'alice', '{[9,12),[14,17)}'),
  (2, 'bob', '{[10,15)}');

-- begin-expected
-- columns: name
-- row: alice
-- end-expected
SELECT name FROM mrop_schedule
WHERE available @> 11
  AND available @> 15
ORDER BY name;

-- ============================================================================
-- 27. multirange with GROUP BY and aggregation
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM mrop_schedule WHERE available && '{[11,13)}'::int4multirange;

-- ============================================================================
-- 28. Empty multirange operations
-- ============================================================================

-- note: PG 18 does not support the || operator for multiranges.
-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT
  '{[1,5)}'::int4multirange || '{}'::int4multirange AS union_result,
  '{[1,5)}'::int4multirange * '{}'::int4multirange AS inter_result;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA mrop_test CASCADE;
SET search_path = public;
