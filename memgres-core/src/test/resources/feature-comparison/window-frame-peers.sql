-- ============================================================================
-- Feature Comparison: Default window frame includes ORDER BY peers (RANGE)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG's default frame with ORDER BY is RANGE BETWEEN UNBOUNDED PRECEDING AND
-- CURRENT ROW, which includes all peers (rows with the same ORDER BY value).
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS wfp_data CASCADE;
CREATE TABLE wfp_data (g int, x int);
INSERT INTO wfp_data VALUES (1, 10), (1, 20), (2, 30), (2, 40);

-- ============================================================================
-- 1. SUM with ORDER BY: peers should be included
-- ============================================================================

-- begin-expected
-- columns: g|x|running_sum
-- row: 1|10|30
-- row: 1|20|30
-- row: 2|30|100
-- row: 2|40|100
-- end-expected
SELECT g, x, sum(x) OVER (ORDER BY g) AS running_sum FROM wfp_data ORDER BY g, x;

-- ============================================================================
-- 2. COUNT with ORDER BY: peers should be included
-- ============================================================================

DROP TABLE IF EXISTS wfp_data2 CASCADE;
CREATE TABLE wfp_data2 (g text, v int);
INSERT INTO wfp_data2 VALUES ('a', 1), ('a', 2), ('a', 3), ('b', 4);

-- begin-expected
-- columns: g|cnt
-- row: a|3
-- row: a|3
-- row: a|3
-- row: b|4
-- end-expected
SELECT g, count(*) OVER (ORDER BY g) AS cnt FROM wfp_data2 ORDER BY g, v;

-- ============================================================================
-- 3. LAST_VALUE with ORDER BY: default RANGE frame ends at last peer
-- ============================================================================

DROP TABLE IF EXISTS wfp_data3 CASCADE;
CREATE TABLE wfp_data3 (g int, v int);
INSERT INTO wfp_data3 VALUES (1, 10), (1, 20), (2, 30);

-- begin-expected
-- columns: g|v|lv
-- row: 1|10|20
-- row: 1|20|20
-- row: 2|30|30
-- end-expected
SELECT g, v, last_value(v) OVER (ORDER BY g) AS lv FROM wfp_data3 ORDER BY g, v;

-- ============================================================================
-- 4. AVG with ORDER BY: peers should be included
-- ============================================================================

-- begin-expected
-- columns: g|a
-- row: 1|15.0000000000000000
-- row: 1|15.0000000000000000
-- row: 2|25.0000000000000000
-- row: 2|25.0000000000000000
-- end-expected
SELECT g, avg(x) OVER (ORDER BY g) AS a FROM wfp_data ORDER BY g, x;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS wfp_data CASCADE;
DROP TABLE IF EXISTS wfp_data2 CASCADE;
DROP TABLE IF EXISTS wfp_data3 CASCADE;
