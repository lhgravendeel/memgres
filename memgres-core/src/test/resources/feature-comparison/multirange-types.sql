-- ============================================================================
-- Feature Comparison: Multirange Types (B4)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG 14+ multirange types: int4multirange, int8multirange, nummultirange,
-- datemultirange, tsmultirange, tstzmultirange.
-- Tests: constructors, operators, functions, casts, DML, edge cases.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS mr_type_test CASCADE;
CREATE SCHEMA mr_type_test;
SET search_path = mr_type_test, public;

-- ============================================================================
-- 1. int4multirange constructor
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[1,5),[10,20)}
-- end-expected
SELECT '{[1,5),[10,20)}'::int4multirange AS result;

-- ============================================================================
-- 2. int8multirange constructor
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[1,5),[10,20)}
-- end-expected
SELECT '{[1,5),[10,20)}'::int8multirange AS result;

-- ============================================================================
-- 3. nummultirange constructor
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[1.5,3.5),[10.0,20.0)}
-- end-expected
SELECT '{[1.5,3.5),[10.0,20.0)}'::nummultirange AS result;

-- ============================================================================
-- 4. datemultirange constructor
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[2026-01-01,2026-02-01),[2026-06-01,2026-07-01)}
-- end-expected
SELECT '{[2026-01-01,2026-02-01),[2026-06-01,2026-07-01)}'::datemultirange AS result;

-- ============================================================================
-- 5. tsmultirange constructor
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {["2026-01-01 00:00:00","2026-01-02 00:00:00"),["2026-06-01 00:00:00","2026-06-02 00:00:00")}
-- end-expected
SELECT '{["2026-01-01 00:00:00","2026-01-02 00:00:00"),["2026-06-01 00:00:00","2026-06-02 00:00:00")}'::tsmultirange AS result;

-- ============================================================================
-- 6. tstzmultirange constructor
-- ============================================================================

-- begin-expected
-- columns: queryable
-- row: true
-- end-expected
SELECT '{["2026-01-01 00:00:00+00","2026-01-02 00:00:00+00")}'::tstzmultirange IS NOT NULL AS queryable;

-- ============================================================================
-- 7. Empty multirange
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {}
-- end-expected
SELECT '{}'::int4multirange AS result;

-- ============================================================================
-- 8. Multirange from function constructor
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[1,5),[10,20)}
-- end-expected
SELECT int4multirange(int4range(1,5), int4range(10,20)) AS result;

-- ============================================================================
-- 9. Multirange @> contains element
-- ============================================================================

-- begin-expected
-- columns: contains_3, contains_7
-- row: true, false
-- end-expected
SELECT
  '{[1,5),[10,20)}'::int4multirange @> 3 AS contains_3,
  '{[1,5),[10,20)}'::int4multirange @> 7 AS contains_7;

-- ============================================================================
-- 10. Multirange @> contains range
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT '{[1,5),[10,20)}'::int4multirange @> int4range(11,15) AS result;

-- ============================================================================
-- 11. Multirange && overlaps
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT '{[1,5),[10,20)}'::int4multirange && '{[3,7)}'::int4multirange AS result;

-- begin-expected
-- columns: result
-- row: false
-- end-expected
SELECT '{[1,5)}'::int4multirange && '{[6,10)}'::int4multirange AS result;

-- ============================================================================
-- 12. Multirange * intersection
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[3,5)}
-- end-expected
SELECT '{[1,5),[10,20)}'::int4multirange * '{[3,7)}'::int4multirange AS result;

-- ============================================================================
-- 13. Multirange + union
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[1,7),[10,20)}
-- end-expected
SELECT '{[1,5),[10,20)}'::int4multirange + '{[3,7)}'::int4multirange AS result;

-- ============================================================================
-- 14. Multirange - difference
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[1,3),[10,20)}
-- end-expected
SELECT '{[1,5),[10,20)}'::int4multirange - '{[3,7)}'::int4multirange AS result;

-- ============================================================================
-- 15. range_agg → multirange
-- ============================================================================

CREATE TABLE mr_ranges (r int4range);
INSERT INTO mr_ranges VALUES ('[1,5)'), ('[3,8)'), ('[10,15)');

-- begin-expected
-- columns: result
-- row: {[1,8),[10,15)}
-- end-expected
SELECT range_agg(r) AS result FROM mr_ranges;

DROP TABLE mr_ranges;

-- ============================================================================
-- 16. unnest(multirange) → set of ranges
-- ============================================================================

-- begin-expected
-- columns: r
-- row: [1,5)
-- row: [10,20)
-- end-expected
SELECT unnest('{[1,5),[10,20)}'::int4multirange) AS r;

-- ============================================================================
-- 17. isempty() on multirange
-- ============================================================================

-- begin-expected
-- columns: r1, r2
-- row: true, false
-- end-expected
SELECT
  isempty('{}'::int4multirange) AS r1,
  isempty('{[1,5)}'::int4multirange) AS r2;

-- ============================================================================
-- 18. lower() / upper() on multirange
-- ============================================================================

-- begin-expected
-- columns: lo, hi
-- row: 1, 20
-- end-expected
SELECT
  lower('{[1,5),[10,20)}'::int4multirange) AS lo,
  upper('{[1,5),[10,20)}'::int4multirange) AS hi;

-- ============================================================================
-- 19. Multirange in table column
-- ============================================================================

CREATE TABLE mr_schedule (
  id integer PRIMARY KEY,
  available int4multirange
);

INSERT INTO mr_schedule VALUES
  (1, '{[9,12),[14,18)}'),
  (2, '{[8,17)}'),
  (3, '{}'::int4multirange);

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM mr_schedule WHERE available @> 10 ORDER BY id;

-- ============================================================================
-- 20. Multirange in WHERE with overlap
-- ============================================================================

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM mr_schedule
WHERE available && '{[11,15)}'::int4multirange
ORDER BY id;

-- ============================================================================
-- 21. UPDATE with multirange column
-- ============================================================================

UPDATE mr_schedule SET available = '{[10,12),[15,20)}' WHERE id = 3;

-- begin-expected
-- columns: available
-- row: {[10,12),[15,20)}
-- end-expected
SELECT available FROM mr_schedule WHERE id = 3;

-- ============================================================================
-- 22. Cast range to multirange
-- ============================================================================

-- begin-expected
-- columns: result
-- row: {[1,10)}
-- end-expected
SELECT int4range(1,10)::int4multirange AS result;

-- ============================================================================
-- 23. Cast multirange to range (single sub-range)
-- ============================================================================

-- note: Only works if multirange contains exactly one sub-range
-- begin-expected
-- columns: result
-- row: [1,10)
-- end-expected
SELECT '{[1,10)}'::int4multirange::int4range AS result;

-- ============================================================================
-- 24. Multirange equality and comparison
-- ============================================================================

-- begin-expected
-- columns: eq, neq
-- row: true, true
-- end-expected
SELECT
  '{[1,5),[10,20)}'::int4multirange = '{[1,5),[10,20)}'::int4multirange AS eq,
  '{[1,5)}'::int4multirange <> '{[1,10)}'::int4multirange AS neq;

-- ============================================================================
-- 25. Multirange with NULL
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT (NULL::int4multirange) IS NULL AS result;

-- ============================================================================
-- 26. datemultirange practical example: booking availability
-- ============================================================================

CREATE TABLE mr_bookings (
  room_id integer,
  booked datemultirange
);

INSERT INTO mr_bookings VALUES
  (101, '{[2026-01-01,2026-01-05),[2026-01-10,2026-01-15)}'),
  (102, '{[2026-01-03,2026-01-08)}');

-- Is room 101 available on Jan 7?
-- begin-expected
-- columns: available
-- row: true
-- end-expected
SELECT NOT (booked @> '2026-01-07'::date) AS available
FROM mr_bookings WHERE room_id = 101;

-- Do any bookings overlap Jan 4-6?
-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM mr_bookings
WHERE booked && '{[2026-01-04,2026-01-06)}'::datemultirange;

DROP TABLE mr_bookings;

-- ============================================================================
-- 27. nummultirange operations
-- ============================================================================

-- begin-expected
-- columns: contains, intersects
-- row: true, true
-- end-expected
SELECT
  '{[1.0,5.0),[10.0,20.0)}'::nummultirange @> 3.5::numeric AS contains,
  '{[1.0,5.0)}'::nummultirange && '{[4.0,8.0)}'::nummultirange AS intersects;

-- ============================================================================
-- 28. Multirange in GiST index
-- ============================================================================

CREATE TABLE mr_indexed (id integer PRIMARY KEY, r int4multirange);
INSERT INTO mr_indexed
  SELECT i, int4multirange(int4range(i*10, i*10+5))
  FROM generate_series(1,100) i;

CREATE INDEX idx_mr_gist ON mr_indexed USING gist (r);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::integer AS cnt FROM mr_indexed WHERE r @> 55;

DROP TABLE mr_indexed;

-- ============================================================================
-- 29. pg_typeof for multirange types
-- ============================================================================

-- begin-expected
-- columns: t1, t2, t3, t4
-- row: int4multirange, int8multirange, nummultirange, datemultirange
-- end-expected
SELECT
  pg_typeof('{[1,5)}'::int4multirange)::text AS t1,
  pg_typeof('{[1,5)}'::int8multirange)::text AS t2,
  pg_typeof('{[1.0,5.0)}'::nummultirange)::text AS t3,
  pg_typeof('{[2026-01-01,2026-02-01)}'::datemultirange)::text AS t4;

-- ============================================================================
-- 30. Multirange adjacent operator -|-
-- ============================================================================

-- begin-expected
-- columns: adj, not_adj
-- row: true, false
-- end-expected
SELECT
  '{[1,5)}'::int4multirange -|- '{[5,10)}'::int4multirange AS adj,
  '{[1,5)}'::int4multirange -|- '{[6,10)}'::int4multirange AS not_adj;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA mr_type_test CASCADE;
SET search_path = public;
