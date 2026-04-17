-- ============================================================================
-- Feature Comparison: Round 14 — CREATE CAST, OVERLAPS, BETWEEN SYMMETRIC,
--                             IN/NULL 3VL, IS DISTINCT FROM, SIMILAR TO
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r14_cp CASCADE;
CREATE SCHEMA r14_cp;
SET search_path = r14_cp, public;

-- ============================================================================
-- SECTION A: OVERLAPS
-- ============================================================================

-- 1. Overlapping date ranges
-- begin-expected
-- columns: o
-- row: t
-- end-expected
SELECT ((DATE '2020-01-01', DATE '2020-06-01')
        OVERLAPS (DATE '2020-03-01', DATE '2020-09-01'))::text AS o;

-- 2. Non-overlapping ranges
-- begin-expected
-- columns: o
-- row: f
-- end-expected
SELECT ((DATE '2020-01-01', DATE '2020-02-01')
        OVERLAPS (DATE '2020-06-01', DATE '2020-09-01'))::text AS o;

-- 3. Touching intervals DO NOT overlap (PG semantics)
-- begin-expected
-- columns: o
-- row: f
-- end-expected
SELECT ((DATE '2020-01-01', DATE '2020-02-01')
        OVERLAPS (DATE '2020-02-01', DATE '2020-03-01'))::text AS o;

-- 4. Duration form: (timestamp, interval) OVERLAPS (timestamp, timestamp)
-- begin-expected
-- columns: o
-- row: t
-- end-expected
SELECT ((DATE '2020-01-01', INTERVAL '30 days')
        OVERLAPS (DATE '2020-01-15', DATE '2020-02-15'))::text AS o;

-- ============================================================================
-- SECTION B: BETWEEN SYMMETRIC
-- ============================================================================

-- 5. Plain BETWEEN with reversed bounds → false
-- begin-expected
-- columns: b
-- row: f
-- end-expected
SELECT (5 BETWEEN 10 AND 1)::text AS b;

-- 6. BETWEEN SYMMETRIC with reversed bounds → true
-- begin-expected
-- columns: b
-- row: t
-- end-expected
SELECT (5 BETWEEN SYMMETRIC 10 AND 1)::text AS b;

-- 7. NOT BETWEEN SYMMETRIC
-- begin-expected
-- columns: b
-- row: t
-- end-expected
SELECT (20 NOT BETWEEN SYMMETRIC 10 AND 1)::text AS b;

-- ============================================================================
-- SECTION C: IN NULL 3VL
-- ============================================================================

-- 8. `5 IN (1, NULL)` → NULL (rendered empty)
-- begin-expected
-- columns: r
-- row:
-- end-expected
SELECT (5 IN (1, NULL))::text AS r;

-- 9. `5 NOT IN (1, NULL)` → NULL
-- begin-expected
-- columns: r
-- row:
-- end-expected
SELECT (5 NOT IN (1, NULL))::text AS r;

-- 10. `1 IN (1, NULL)` → TRUE
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (1 IN (1, NULL))::text AS r;

-- ============================================================================
-- SECTION D: IS DISTINCT FROM
-- ============================================================================

-- 11. NULL vs NULL with IS DISTINCT FROM is false
-- begin-expected
-- columns: r
-- row: f
-- end-expected
SELECT (NULL IS DISTINCT FROM NULL)::text AS r;

-- 12. NULL IS NOT DISTINCT FROM NULL is true
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (NULL IS NOT DISTINCT FROM NULL)::text AS r;

-- ============================================================================
-- SECTION E: CREATE CAST
-- ============================================================================

CREATE DOMAIN r14_dom_pos AS int CHECK (VALUE > 0);
CREATE FUNCTION r14_cast_fn(int) RETURNS r14_dom_pos
  AS 'SELECT $1::r14_dom_pos' LANGUAGE SQL;
CREATE CAST (int AS r14_dom_pos) WITH FUNCTION r14_cast_fn(int) AS ASSIGNMENT;

-- 13. User cast visible in pg_cast
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_cast c
  JOIN pg_type t ON c.casttarget = t.oid
  WHERE t.typname = 'r14_dom_pos';

CREATE DOMAIN r14_dom_text AS text;
CREATE CAST (text AS r14_dom_text) WITHOUT FUNCTION AS IMPLICIT;

-- 14. Binary-coercible cast
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_cast c
  JOIN pg_type t ON c.casttarget = t.oid
  WHERE t.typname = 'r14_dom_text';

CREATE DOMAIN r14_dom_d AS int;
CREATE CAST (int AS r14_dom_d) WITHOUT FUNCTION;
DROP CAST (int AS r14_dom_d);

-- 15. DROP CAST removes the entry
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_cast c
  JOIN pg_type t ON c.casttarget = t.oid
  WHERE t.typname = 'r14_dom_d';

-- ============================================================================
-- SECTION F: SIMILAR TO
-- ============================================================================

-- 16. SIMILAR TO with _ single-char wildcard
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('abc' SIMILAR TO 'a_c')::text AS r;

-- 17. SIMILAR TO with % multi-char wildcard
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('abc' SIMILAR TO 'a%')::text AS r;

-- 18. SIMILAR TO with character class
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('abc' SIMILAR TO '[a-z]+')::text AS r;

-- ============================================================================
-- SECTION G: Row-valued comparison
-- ============================================================================

-- 19. Row equality
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ((1,'a') = (1,'a'))::text AS r;

-- 20. Row lexicographic comparison
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ((1,2) < (1,3))::text AS r;

CREATE TABLE r14_rv (a int, b text);
INSERT INTO r14_rv VALUES (1,'a'),(2,'b');

-- 21. Row value in subquery
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ((1,'a') IN (SELECT a, b FROM r14_rv))::text AS r;

-- ============================================================================
-- SECTION H: Interval justify functions
-- ============================================================================

-- 22. justify_hours folds 24h → 1 day
-- begin-expected
-- columns: r
-- row: 1 day 01:00:00
-- end-expected
SELECT justify_hours(interval '25 hours')::text AS r;

-- 23. justify_days folds 30d → 1 mon
-- begin-expected
-- columns: r
-- row: 1 mon
-- end-expected
SELECT justify_days(interval '30 days')::text AS r;

-- ============================================================================
-- SECTION I: current_query and friends
-- ============================================================================

-- 24. current_query returns the running SQL
SELECT current_query();

-- 25. inet_server_addr resolves (may be NULL)
SELECT inet_server_addr();

-- 26. inet_client_addr resolves (may be NULL)
SELECT inet_client_addr();
