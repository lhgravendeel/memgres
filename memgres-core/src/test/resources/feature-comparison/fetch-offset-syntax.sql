-- ============================================================================
-- Feature Comparison: SQL-Standard FETCH/OFFSET and WITH TIES
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests SQL-standard FETCH FIRST N ROWS ONLY, OFFSET N ROWS,
-- FETCH FIRST N ROWS WITH TIES, and FETCH FIRST N PERCENT.
-- These are alternatives to LIMIT/OFFSET with additional features.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS fetch_test CASCADE;
CREATE SCHEMA fetch_test;
SET search_path = fetch_test, public;

CREATE TABLE fetch_data (id integer PRIMARY KEY, score integer, name text);
INSERT INTO fetch_data VALUES
  (1, 90, 'alice'),
  (2, 85, 'bob'),
  (3, 90, 'carol'),
  (4, 80, 'dave'),
  (5, 75, 'eve'),
  (6, 90, 'frank'),
  (7, 85, 'grace'),
  (8, 70, 'heidi'),
  (9, 95, 'ivan'),
  (10, 80, 'judy');

-- ============================================================================
-- SECTION A: FETCH FIRST N ROWS ONLY
-- ============================================================================

-- ============================================================================
-- 1. FETCH FIRST 3 ROWS ONLY (equivalent to LIMIT 3)
-- ============================================================================

-- begin-expected
-- columns: id, name
-- row: 1, alice
-- row: 2, bob
-- row: 3, carol
-- end-expected
SELECT id, name FROM fetch_data
ORDER BY id
FETCH FIRST 3 ROWS ONLY;

-- ============================================================================
-- 2. FETCH FIRST 1 ROW ONLY (singular form)
-- ============================================================================

-- begin-expected
-- columns: id, name
-- row: 1, alice
-- end-expected
SELECT id, name FROM fetch_data
ORDER BY id
FETCH FIRST 1 ROW ONLY;

-- ============================================================================
-- 3. FETCH FIRST ROW ONLY (implicit 1)
-- ============================================================================

-- begin-expected
-- columns: id, name
-- row: 1, alice
-- end-expected
SELECT id, name FROM fetch_data
ORDER BY id
FETCH FIRST ROW ONLY;

-- ============================================================================
-- SECTION B: OFFSET N ROWS
-- ============================================================================

-- ============================================================================
-- 4. OFFSET with FETCH
-- ============================================================================

-- begin-expected
-- columns: id, name
-- row: 4, dave
-- row: 5, eve
-- row: 6, frank
-- end-expected
SELECT id, name FROM fetch_data
ORDER BY id
OFFSET 3 ROWS
FETCH FIRST 3 ROWS ONLY;

-- ============================================================================
-- 5. OFFSET 0 ROWS (no skip)
-- ============================================================================

-- begin-expected
-- columns: id, name
-- row: 1, alice
-- row: 2, bob
-- end-expected
SELECT id, name FROM fetch_data
ORDER BY id
OFFSET 0 ROWS
FETCH FIRST 2 ROWS ONLY;

-- ============================================================================
-- 6. OFFSET beyond available rows
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM (
  SELECT id FROM fetch_data
  ORDER BY id
  OFFSET 100 ROWS
  FETCH FIRST 5 ROWS ONLY
) sub;

-- ============================================================================
-- 7. OFFSET without FETCH (returns remaining rows)
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 7
-- end-expected
SELECT count(*)::integer AS cnt FROM (
  SELECT id FROM fetch_data
  ORDER BY id
  OFFSET 3 ROWS
) sub;

-- ============================================================================
-- SECTION C: FETCH FIRST N ROWS WITH TIES
-- ============================================================================

-- ============================================================================
-- 8. WITH TIES: includes tied rows beyond N
-- ============================================================================

-- note: WITH TIES ties on the full ORDER BY expression (score DESC, id).
-- The 2nd row is alice (90, id=1). carol (90, id=3) has a different id, so not tied.
-- begin-expected
-- columns: name, score
-- row: ivan, 95
-- row: alice, 90
-- end-expected
SELECT name, score FROM fetch_data
ORDER BY score DESC, id
FETCH FIRST 2 ROWS WITH TIES;

-- ============================================================================
-- 9. WITH TIES: no ties (same as ONLY)
-- ============================================================================

-- begin-expected
-- columns: name, score
-- row: ivan, 95
-- end-expected
SELECT name, score FROM fetch_data
ORDER BY score DESC, id
FETCH FIRST 1 ROW WITH TIES;

-- ============================================================================
-- 10. WITH TIES: all rows are tied
-- ============================================================================

CREATE TABLE fetch_same (id integer, val integer);
INSERT INTO fetch_same VALUES (1, 10), (2, 10), (3, 10), (4, 10);

-- begin-expected
-- columns: cnt
-- row: 4
-- end-expected
SELECT count(*)::integer AS cnt FROM (
  SELECT id FROM fetch_same
  ORDER BY val
  FETCH FIRST 1 ROW WITH TIES
) sub;

DROP TABLE fetch_same;

-- ============================================================================
-- 11. WITH TIES with OFFSET
-- ============================================================================

-- note: WITH TIES ties on the full ORDER BY (score DESC, id). After offset 1,
-- first row is alice (90, id=1). carol (90, id=3) has different id, not tied.
-- begin-expected
-- columns: name, score
-- row: alice, 90
-- end-expected
SELECT name, score FROM fetch_data
ORDER BY score DESC, id
OFFSET 1 ROW
FETCH FIRST 1 ROW WITH TIES;

-- ============================================================================
-- SECTION D: FETCH FIRST N PERCENT
-- ============================================================================

-- ============================================================================
-- 12. FETCH FIRST 50 PERCENT ROWS ONLY
-- ============================================================================

-- note: PostgreSQL does not support PERCENT syntax in FETCH FIRST.
-- begin-expected-error
-- message-like: PERCENT
-- end-expected-error
SELECT count(*)::integer AS cnt FROM (
  SELECT id FROM fetch_data
  ORDER BY id
  FETCH FIRST 50 PERCENT ROWS ONLY
) sub;

-- ============================================================================
-- 13. FETCH FIRST 10 PERCENT (rounds up)
-- ============================================================================

-- note: PostgreSQL does not support PERCENT syntax in FETCH FIRST.
-- begin-expected-error
-- message-like: PERCENT
-- end-expected-error
SELECT count(*)::integer AS cnt FROM (
  SELECT id FROM fetch_data
  ORDER BY id
  FETCH FIRST 10 PERCENT ROWS ONLY
) sub;

-- ============================================================================
-- 14. FETCH FIRST 100 PERCENT (all rows)
-- ============================================================================

-- note: PostgreSQL does not support PERCENT syntax in FETCH FIRST.
-- begin-expected-error
-- message-like: PERCENT
-- end-expected-error
SELECT count(*)::integer AS cnt FROM (
  SELECT id FROM fetch_data
  ORDER BY id
  FETCH FIRST 100 PERCENT ROWS ONLY
) sub;

-- ============================================================================
-- SECTION E: Interaction with subqueries and CTEs
-- ============================================================================

-- ============================================================================
-- 15. FETCH in subquery
-- ============================================================================

-- begin-expected
-- columns: avg_score
-- row: 92
-- end-expected
SELECT avg(score)::integer AS avg_score FROM (
  SELECT score FROM fetch_data
  ORDER BY score DESC
  FETCH FIRST 3 ROWS ONLY
) top3;

-- ============================================================================
-- 16. FETCH in CTE
-- ============================================================================

-- begin-expected
-- columns: id, name
-- row: 1, alice
-- row: 9, ivan
-- end-expected
WITH top2 AS (
  SELECT id, name, score FROM fetch_data
  ORDER BY score DESC, id
  FETCH FIRST 2 ROWS ONLY
)
SELECT id, name FROM top2 ORDER BY id;

-- ============================================================================
-- 17. FETCH FIRST with UNION
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 4
-- end-expected
SELECT count(*)::integer AS cnt FROM (
  (SELECT id FROM fetch_data ORDER BY id FETCH FIRST 2 ROWS ONLY)
  UNION ALL
  (SELECT id FROM fetch_data ORDER BY id DESC FETCH FIRST 2 ROWS ONLY)
) sub;

-- ============================================================================
-- 18. Mixing LIMIT and FETCH styles (LIMIT is PG extension, FETCH is standard)
-- ============================================================================

-- begin-expected
-- columns: id, name
-- row: 1, alice
-- row: 2, bob
-- row: 3, carol
-- end-expected
SELECT id, name FROM fetch_data
ORDER BY id
LIMIT 3;

-- begin-expected
-- columns: id, name
-- row: 1, alice
-- row: 2, bob
-- row: 3, carol
-- end-expected
SELECT id, name FROM fetch_data
ORDER BY id
FETCH FIRST 3 ROWS ONLY;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA fetch_test CASCADE;
SET search_path = public;
