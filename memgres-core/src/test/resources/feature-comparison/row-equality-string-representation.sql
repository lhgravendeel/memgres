-- ============================================================================
-- Feature Comparison: Row equality must be value-based, not string-based
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- DISTINCT, UNION/INTERSECT/EXCEPT, and GROUP BY should compare values
-- semantically, not by string representation. String-based comparison
-- causes false collisions with comma-containing strings and fails to
-- recognize numerically equal values with different scale.
-- ============================================================================

-- ============================================================================
-- 1. INTERSECT: equal numerics with different scale
-- ============================================================================

-- begin-expected
-- columns: v
-- row: 1.0
-- end-expected
SELECT 1.0 AS v INTERSECT SELECT 1.00 AS v;

-- ============================================================================
-- 2. EXCEPT: equal numerics cancel out
-- ============================================================================

-- begin-expected
-- columns: v
-- end-expected
SELECT 1.0 AS v EXCEPT SELECT 1.00 AS v;

-- ============================================================================
-- 3. UNION: comma-containing strings must not collide
-- ============================================================================

-- begin-expected
-- columns: c1|c2
-- row: a|b, c
-- row: a, b|c
-- end-expected
SELECT 'a, b' AS c1, 'c' AS c2 UNION SELECT 'a' AS c1, 'b, c' AS c2 ORDER BY c1;

-- ============================================================================
-- 4. DISTINCT: comma-containing strings must not collide
-- ============================================================================

DROP TABLE IF EXISTS rse_d1 CASCADE;
CREATE TABLE rse_d1 (a text, b text);
INSERT INTO rse_d1 VALUES ('a, b', 'c'), ('a', 'b, c');

-- begin-expected
-- columns: a|b
-- row: a|b, c
-- row: a, b|c
-- end-expected
SELECT DISTINCT a, b FROM rse_d1 ORDER BY a;

-- ============================================================================
-- 5. DISTINCT: equal numerics collapse to 1 row
-- ============================================================================

DROP TABLE IF EXISTS rse_d2 CASCADE;
CREATE TABLE rse_d2 (v numeric);
INSERT INTO rse_d2 VALUES (1.0), (1.00), (1.000);

-- begin-expected
-- columns: v
-- row: 1.0
-- end-expected
SELECT DISTINCT v FROM rse_d2;

-- ============================================================================
-- 6. GROUP BY: equal numerics group together
-- ============================================================================

DROP TABLE IF EXISTS rse_g1 CASCADE;
CREATE TABLE rse_g1 (v numeric, x int);
INSERT INTO rse_g1 VALUES (1.0, 10), (1.00, 20), (2.0, 30);

-- begin-expected
-- columns: v|s
-- row: 1.0|30
-- row: 2.0|30
-- end-expected
SELECT v, sum(x)::text AS s FROM rse_g1 GROUP BY v ORDER BY v;

-- ============================================================================
-- 7. GROUP BY: comma-containing strings must not collide
-- ============================================================================

DROP TABLE IF EXISTS rse_g2 CASCADE;
CREATE TABLE rse_g2 (a text, b text, x int);
INSERT INTO rse_g2 VALUES ('a, b', 'c', 1), ('a', 'b, c', 2);

-- begin-expected
-- columns: a|b|s
-- row: a|b, c|2
-- row: a, b|c|1
-- end-expected
SELECT a, b, sum(x)::text AS s FROM rse_g2 GROUP BY a, b ORDER BY a;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS rse_d1 CASCADE;
DROP TABLE IF EXISTS rse_d2 CASCADE;
DROP TABLE IF EXISTS rse_g1 CASCADE;
DROP TABLE IF EXISTS rse_g2 CASCADE;
