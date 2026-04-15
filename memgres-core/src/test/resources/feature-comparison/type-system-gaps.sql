-- ============================================================================
-- Feature Comparison: Type System Gaps
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests handling of types that are missing or partial in Memgres:
-- timetz, oid, xid, cid, tid, pg_lsn, jsonpath, macaddr8, regtype,
-- regclass, regproc, and array type handling.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS types_test CASCADE;
CREATE SCHEMA types_test;
SET search_path = types_test, public;

-- ============================================================================
-- SECTION A: timetz (time with time zone)
-- ============================================================================

-- ============================================================================
-- 1. timetz literal
-- ============================================================================

-- begin-expected
-- columns: t
-- row: 12:30:00+00
-- end-expected
SELECT '12:30:00+00'::timetz AS t;

-- ============================================================================
-- 2. timetz with offset
-- ============================================================================

-- begin-expected
-- columns: t
-- row: 14:30:00+05:30
-- end-expected
SELECT '14:30:00+05:30'::timetz AS t;

-- ============================================================================
-- 3. timetz in table
-- ============================================================================

CREATE TABLE types_timetz (id integer, t timetz);
INSERT INTO types_timetz VALUES (1, '09:00:00+00'), (2, '17:30:00-05');

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM types_timetz;

-- begin-expected
-- columns: id, t
-- row: 1, 09:00:00+00
-- end-expected
SELECT id, t FROM types_timetz WHERE id = 1;

-- ============================================================================
-- 4. timetz arithmetic
-- ============================================================================

-- begin-expected
-- columns: later
-- row: 14:30:00+00
-- end-expected
SELECT ('12:30:00+00'::timetz + interval '2 hours') AS later;

-- ============================================================================
-- SECTION B: OID type
-- ============================================================================

-- ============================================================================
-- 5. OID literal and cast
-- ============================================================================

-- begin-expected
-- columns: o
-- row: 42
-- end-expected
SELECT 42::oid AS o;

-- ============================================================================
-- 6. OID comparison
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT 42::oid = 42::oid AS result;

-- ============================================================================
-- 7. OID in pg_class query
-- ============================================================================

-- begin-expected
-- columns: has_oid
-- row: true
-- end-expected
SELECT oid IS NOT NULL AS has_oid FROM pg_class WHERE relname = 'pg_class' LIMIT 1;

-- ============================================================================
-- SECTION C: Reg* types (regclass, regtype, regproc)
-- ============================================================================

-- ============================================================================
-- 8. regclass: table name to OID
-- ============================================================================

-- begin-expected
-- columns: is_valid
-- row: true
-- end-expected
SELECT 'pg_class'::regclass IS NOT NULL AS is_valid;

-- ============================================================================
-- 9. regclass: OID to name
-- ============================================================================

-- begin-expected
-- columns: name
-- row: pg_class
-- end-expected
SELECT 'pg_class'::regclass::text AS name;

-- ============================================================================
-- 10. regtype: type name to OID and back
-- ============================================================================

-- begin-expected
-- columns: t
-- row: integer
-- end-expected
SELECT 'integer'::regtype::text AS t;

-- begin-expected
-- columns: t
-- row: text
-- end-expected
SELECT 'text'::regtype::text AS t;

-- ============================================================================
-- 11. regproc: function name to OID and back
-- ============================================================================

-- begin-expected
-- columns: f
-- row: now
-- end-expected
SELECT 'now'::regproc::text AS f;

-- ============================================================================
-- 12. regclass with schema-qualified name
-- ============================================================================

-- begin-expected
-- columns: is_valid
-- row: true
-- end-expected
SELECT 'pg_catalog.pg_class'::regclass IS NOT NULL AS is_valid;

-- ============================================================================
-- 13. regclass error on nonexistent table
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: does not exist
-- end-expected-error
SELECT 'nonexistent_table_xyz999'::regclass;

-- ============================================================================
-- SECTION D: Array type handling
-- ============================================================================

-- ============================================================================
-- 14. Integer array
-- ============================================================================

-- begin-expected
-- columns: a
-- row: {1,2,3}
-- end-expected
SELECT ARRAY[1, 2, 3] AS a;

-- ============================================================================
-- 15. Text array
-- ============================================================================

-- begin-expected
-- columns: a
-- row: {hello,world}
-- end-expected
SELECT ARRAY['hello', 'world'] AS a;

-- ============================================================================
-- 16. Boolean array
-- ============================================================================

-- begin-expected
-- columns: a
-- row: {t,f,t}
-- end-expected
SELECT ARRAY[true, false, true] AS a;

-- ============================================================================
-- 17. Numeric array
-- ============================================================================

-- begin-expected
-- columns: a
-- row: {1.1,2.2,3.3}
-- end-expected
SELECT ARRAY[1.1, 2.2, 3.3]::numeric[] AS a;

-- ============================================================================
-- 18. Bigint array
-- ============================================================================

-- begin-expected
-- columns: a
-- row: {1000000000000,2000000000000}
-- end-expected
SELECT ARRAY[1000000000000::bigint, 2000000000000::bigint] AS a;

-- ============================================================================
-- 19. UUID array
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT array_length(
  ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid],
  1
) AS cnt;

-- ============================================================================
-- 20. Date array
-- ============================================================================

-- begin-expected
-- columns: a
-- row: {2024-01-01,2024-06-15}
-- end-expected
SELECT ARRAY['2024-01-01'::date, '2024-06-15'::date] AS a;

-- ============================================================================
-- 21. Timestamp array
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT array_length(
  ARRAY['2024-01-01 12:00:00'::timestamp, '2024-06-15 18:30:00'::timestamp],
  1
) AS cnt;

-- ============================================================================
-- 22. Array operations: append, prepend, concat
-- ============================================================================

-- begin-expected
-- columns: appended
-- row: {1,2,3,4}
-- end-expected
SELECT array_append(ARRAY[1,2,3], 4) AS appended;

-- begin-expected
-- columns: prepended
-- row: {0,1,2,3}
-- end-expected
SELECT array_prepend(0, ARRAY[1,2,3]) AS prepended;

-- begin-expected
-- columns: catted
-- row: {1,2,3,4,5}
-- end-expected
SELECT ARRAY[1,2,3] || ARRAY[4,5] AS catted;

-- ============================================================================
-- 23. Array containment operators
-- ============================================================================

-- begin-expected
-- columns: contains, is_contained
-- row: true, true
-- end-expected
SELECT
  ARRAY[1,2,3,4] @> ARRAY[2,3] AS contains,
  ARRAY[2,3] <@ ARRAY[1,2,3,4] AS is_contained;

-- ============================================================================
-- 24. Array overlap operator
-- ============================================================================

-- begin-expected
-- columns: overlaps, no_overlap
-- row: true, false
-- end-expected
SELECT
  ARRAY[1,2,3] && ARRAY[3,4,5] AS overlaps,
  ARRAY[1,2] && ARRAY[3,4] AS no_overlap;

-- ============================================================================
-- 25. Multi-dimensional arrays
-- ============================================================================

-- begin-expected
-- columns: a
-- row: {{1,2},{3,4}}
-- end-expected
SELECT ARRAY[[1,2],[3,4]] AS a;

-- begin-expected
-- columns: elem
-- row: 3
-- end-expected
SELECT (ARRAY[[1,2],[3,4]])[2][1] AS elem;

-- ============================================================================
-- SECTION E: jsonpath
-- ============================================================================

-- ============================================================================
-- 26. jsonpath literal
-- ============================================================================

-- begin-expected
-- columns: jp
-- row: $."store"."book"[*]."author"
-- end-expected
SELECT '$.store.book[*].author'::jsonpath AS jp;

-- ============================================================================
-- 27. jsonb @@ jsonpath
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT '{"a": 1}'::jsonb @@ '$.a == 1' AS result;

-- begin-expected
-- columns: result
-- row: false
-- end-expected
SELECT '{"a": 1}'::jsonb @@ '$.a == 2' AS result;

-- ============================================================================
-- 28. jsonb_path_query
-- ============================================================================

-- begin-expected
-- columns: val
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT jsonb_path_query('[1, 2, 3]'::jsonb, '$[*]') AS val;

-- ============================================================================
-- 29. jsonb_path_exists
-- ============================================================================

-- begin-expected
-- columns: exists_result
-- row: true
-- end-expected
SELECT jsonb_path_exists('{"name": "Alice", "age": 30}'::jsonb, '$.name') AS exists_result;

-- begin-expected
-- columns: exists_result
-- row: false
-- end-expected
SELECT jsonb_path_exists('{"name": "Alice"}'::jsonb, '$.email') AS exists_result;

-- ============================================================================
-- SECTION F: macaddr8
-- ============================================================================

-- ============================================================================
-- 30. macaddr8 literal
-- ============================================================================

-- begin-expected
-- columns: m
-- row: 08:00:2b:01:02:03:04:05
-- end-expected
SELECT '08:00:2b:01:02:03:04:05'::macaddr8 AS m;

-- ============================================================================
-- 31. macaddr8 comparison
-- ============================================================================

-- begin-expected
-- columns: eq
-- row: true
-- end-expected
SELECT '08:00:2b:01:02:03:04:05'::macaddr8 = '08:00:2b:01:02:03:04:05'::macaddr8 AS eq;

-- ============================================================================
-- 32. macaddr to macaddr8 conversion
-- ============================================================================

-- begin-expected
-- columns: m8
-- row: 08:00:2b:ff:fe:01:02:03
-- end-expected
SELECT macaddr8('08:00:2b:01:02:03'::macaddr) AS m8;

-- ============================================================================
-- SECTION G: Domain types
-- ============================================================================

-- ============================================================================
-- 33. Create and use domain type
-- ============================================================================

CREATE DOMAIN types_posint AS integer CHECK (VALUE > 0);

-- begin-expected
-- columns: result
-- row: 5
-- end-expected
SELECT 5::types_posint AS result;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint
-- end-expected-error
SELECT (-1)::types_posint;

-- ============================================================================
-- 34. Domain in table column
-- ============================================================================

CREATE TABLE types_domain_tbl (id integer, val types_posint);
INSERT INTO types_domain_tbl VALUES (1, 10);

-- begin-expected
-- columns: id, val
-- row: 1, 10
-- end-expected
SELECT * FROM types_domain_tbl;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO types_domain_tbl VALUES (2, -5);

-- ============================================================================
-- SECTION H: Enum types
-- ============================================================================

-- ============================================================================
-- 35. Create and use enum type
-- ============================================================================

CREATE TYPE types_mood AS ENUM ('sad', 'ok', 'happy');

-- begin-expected
-- columns: m
-- row: happy
-- end-expected
SELECT 'happy'::types_mood AS m;

-- ============================================================================
-- 36. Enum ordering
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT 'happy'::types_mood > 'sad'::types_mood AS result;

-- ============================================================================
-- 37. Enum in table with ORDER BY
-- ============================================================================

CREATE TABLE types_people (name text, mood types_mood);
INSERT INTO types_people VALUES ('alice', 'happy'), ('bob', 'sad'), ('carol', 'ok');

-- begin-expected
-- columns: name, mood
-- row: bob, sad
-- row: carol, ok
-- row: alice, happy
-- end-expected
SELECT name, mood FROM types_people ORDER BY mood;

-- ============================================================================
-- 38. Invalid enum value
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input value for enum
-- end-expected-error
SELECT 'angry'::types_mood;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA types_test CASCADE;
SET search_path = public;
