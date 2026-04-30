-- ============================================================================
-- Feature Comparison: Round 15 — Set ops + WITH ORDINALITY
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r15_so CASCADE;
CREATE SCHEMA r15_so;
SET search_path = r15_so, public;

-- ============================================================================
-- SECTION A: INTERSECT ALL / EXCEPT ALL
-- ============================================================================

-- 1. INTERSECT ALL — keeps min multiplicity
-- begin-expected
-- columns: x
-- row: 1
-- row: 1
-- row: 2
-- end-expected
SELECT x FROM (VALUES (1),(1),(1),(2)) v(x)
INTERSECT ALL
SELECT x FROM (VALUES (1),(1),(2),(3)) w(x)
ORDER BY x;

-- 2. INTERSECT (no ALL) — dedup
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*)::int AS c FROM (
  SELECT x FROM (VALUES (1),(1),(2)) v(x)
  INTERSECT
  SELECT x FROM (VALUES (1),(2),(3)) w(x)
) sub;

-- 3. EXCEPT ALL — counts diff
-- begin-expected
-- columns: x
-- row: 1
-- row: 1
-- end-expected
SELECT x FROM (VALUES (1),(1),(1),(2)) v(x)
EXCEPT ALL
SELECT x FROM (VALUES (1),(2)) w(x)
ORDER BY x;

-- 4. EXCEPT (no ALL) — dedup
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM (
  SELECT x FROM (VALUES (1),(1),(1),(2)) v(x)
  EXCEPT
  SELECT x FROM (VALUES (2)) w(x)
) sub;

-- ============================================================================
-- SECTION B: DISTINCT ON + GROUP BY — should error
-- ============================================================================

CREATE TABLE r15_doonly (a int, b int);
INSERT INTO r15_doonly VALUES (1,1),(1,2),(2,3);

-- 5. DISTINCT ON + GROUP BY — error
-- begin-expected
-- columns: a,count
-- row: 1 | 2
-- row: 2 | 1
-- end-expected
SELECT DISTINCT ON (a) a, count(*) FROM r15_doonly GROUP BY a;

-- ============================================================================
-- SECTION C: ROWS FROM (…) WITH ORDINALITY
-- ============================================================================

-- 6. Ordinal column
-- begin-expected
-- columns: val, ord
-- row: 10, 1
-- row: 11, 2
-- row: 12, 3
-- end-expected
SELECT * FROM ROWS FROM (generate_series(10,12)) WITH ORDINALITY AS t(val, ord);

-- 7. Standalone SRF WITH ORDINALITY
-- begin-expected
-- columns: val, ord
-- row: 100, 1
-- row: 101, 2
-- row: 102, 3
-- end-expected
SELECT * FROM generate_series(100,102) WITH ORDINALITY AS t(val, ord);

-- 8. ROWS FROM with two SRFs (different lengths)
-- begin-expected
-- columns: c
-- row: 3
-- end-expected
SELECT count(*)::int AS c FROM ROWS FROM
  (generate_series(1,3), generate_series(10,11)) AS t(a, b);

-- ============================================================================
-- SECTION D: Parenthesized set ops
-- ============================================================================

-- 9. Parenthesized UNION ALL
-- begin-expected
-- columns: c
-- row: 4
-- end-expected
SELECT count(*)::int AS c FROM (
  (SELECT 1 UNION ALL SELECT 2)
  UNION ALL
  (SELECT 3 UNION ALL SELECT 4)
) sub;

-- 10. ORDER BY + LIMIT after UNION ALL
-- begin-expected
-- columns: x
-- row: 1
-- row: 2
-- end-expected
SELECT x FROM (VALUES (3),(1),(2)) v(x)
UNION ALL SELECT x FROM (VALUES (2)) w(x)
ORDER BY x LIMIT 2;
