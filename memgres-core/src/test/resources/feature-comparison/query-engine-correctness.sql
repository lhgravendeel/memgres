-- ============================================================================
-- Feature Comparison: query shapes that returned wrong answers
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- An ordinal ORDER BY counts output columns, so a star target must be expanded
-- before the ordinal is resolved. A row constructor compared against a subquery
-- compares whole rows. A bare name matching a FROM item is a whole-row
-- reference. Arrays sort element-wise. A union takes the type of the branch
-- that has one.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS qec_t CASCADE;
DROP TABLE IF EXISTS qec_pts CASCADE;
DROP TABLE IF EXISTS qec_wr CASCADE;
DROP TABLE IF EXISTS qec_ar CASCADE;

CREATE TABLE qec_t (id int, val text);
INSERT INTO qec_t VALUES (2,'b'),(1,'a'),(3,'c');

-- ============================================================================
-- 1. Ordinal ORDER BY with a star target
-- ============================================================================

-- begin-expected
-- columns: id | val
-- row: 3, c
-- row: 2, b
-- row: 1, a
-- end-expected
SELECT * FROM qec_t ORDER BY 1 DESC;

-- begin-expected
-- columns: id | val
-- row: 1, a
-- row: 2, b
-- row: 3, c
-- end-expected
SELECT * FROM qec_t ORDER BY 1, 2;

-- begin-expected
-- columns: id | val
-- row: 1, a
-- row: 2, b
-- row: 3, c
-- end-expected
SELECT * FROM qec_t ORDER BY 2;

-- ============================================================================
-- 2. Row constructors against a subquery
-- ============================================================================

CREATE TABLE qec_pts (xi int, yi int);
INSERT INTO qec_pts VALUES (1,2),(3,4);

-- begin-expected
-- columns: any_hit | in_hit | any_miss | in_miss
-- row: t, t, f, f
-- end-expected
SELECT (1,2) = ANY (SELECT xi, yi FROM qec_pts) AS any_hit,
       (1,2) IN (SELECT xi, yi FROM qec_pts) AS in_hit,
       (9,9) = ANY (SELECT xi, yi FROM qec_pts) AS any_miss,
       (9,9) IN (SELECT xi, yi FROM qec_pts) AS in_miss;

-- ============================================================================
-- 3. Whole-row references and row NULL tests
-- ============================================================================

CREATE TABLE qec_wr (a int, b text);
INSERT INTO qec_wr VALUES (1,'x'),(2,NULL);

-- begin-expected
-- columns: t1
-- row: (1,x)
-- row: (2,)
-- end-expected
SELECT t1 FROM qec_wr t1 ORDER BY a;

-- A row is NOT NULL only when every field is, and NULL only when all fields are
-- begin-expected
-- columns: notnull | isnull
-- row: t, f
-- row: f, f
-- end-expected
SELECT (t1 IS NOT NULL) AS notnull, (t1 IS NULL) AS isnull FROM qec_wr t1 ORDER BY a;

-- ============================================================================
-- 4. Arrays sort element-wise, not as text
-- ============================================================================

CREATE TABLE qec_ar (id int, arr int[]);
INSERT INTO qec_ar VALUES (1,'{1,2}'),(2,'{1}'),(3,'{1,2,3}');

-- begin-expected
-- columns: id
-- row: 2
-- row: 1
-- row: 3
-- end-expected
SELECT id FROM qec_ar ORDER BY arr;

-- begin-expected
-- columns: id
-- row: 3
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM qec_ar ORDER BY arr DESC;

-- ============================================================================
-- 5. A union takes the type of the branch that has one
-- ============================================================================

-- begin-expected
-- columns: t
-- row: integer
-- end-expected
SELECT pg_typeof(x)::text AS t FROM (SELECT NULL UNION ALL SELECT 1) u(x) LIMIT 1;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE qec_ar CASCADE;
DROP TABLE qec_wr CASCADE;
DROP TABLE qec_pts CASCADE;
DROP TABLE qec_t CASCADE;
