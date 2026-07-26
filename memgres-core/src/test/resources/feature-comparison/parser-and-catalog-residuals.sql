-- ============================================================================
-- Feature Comparison: bare VALUES arms, jsonb arrays, named windows, _pg_expandarray
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A bare VALUES list is a query, so it can stand on either side of a set
-- operation. Braces open a JSON object, not a nested array, when the element
-- type is jsonb. A named window can be refined at the call site with a frame,
-- and the call's own FILTER survives that substitution.
-- ============================================================================

-- ============================================================================
-- 1. A bare VALUES list is a valid set-operation arm
-- ============================================================================

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM (SELECT 1 UNION ALL VALUES (2)) s;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM (SELECT 1 INTERSECT VALUES (1)) s;

-- ============================================================================
-- 2. Braces in a jsonb array element open an object
-- ============================================================================

-- begin-expected
-- columns: a
-- row: {"k": 1}
-- end-expected
SELECT (ARRAY['{"k":1}']::jsonb[])[1]::text AS a;

-- ============================================================================
-- 3. information_schema._pg_expandarray pairs elements with their index
-- ============================================================================

-- begin-expected
-- columns: x | n
-- row: 3, 1
-- row: 4, 2
-- row: 5, 3
-- end-expected
SELECT x, n FROM information_schema._pg_expandarray(ARRAY[3,4,5]) ORDER BY n;

-- ============================================================================
-- 4. A named window can be refined at the call site
-- ============================================================================

DROP TABLE IF EXISTS pcr_w CASCADE;
CREATE TABLE pcr_w (id int PRIMARY KEY, g int, v int);
INSERT INTO pcr_w VALUES (1,1,10),(2,1,20),(3,2,30);

-- OVER (w ROWS ...) adds a frame to the named window
-- begin-expected
-- columns: id | s
-- row: 1, 10
-- row: 2, 30
-- row: 3, 30
-- end-expected
SELECT id, sum(v) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s
  FROM pcr_w WINDOW w AS (PARTITION BY g ORDER BY id) ORDER BY id;

-- FILTER survives the named-window substitution
-- begin-expected
-- columns: id | c
-- row: 1, 0
-- row: 2, 1
-- row: 3, 1
-- end-expected
SELECT id, count(*) FILTER (WHERE v > 10) OVER w AS c
  FROM pcr_w WINDOW w AS (PARTITION BY g ORDER BY id) ORDER BY id;

DROP TABLE pcr_w;
