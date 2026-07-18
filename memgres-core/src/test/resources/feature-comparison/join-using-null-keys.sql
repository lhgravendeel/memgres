-- ============================================================================
-- Feature Comparison: FULL/RIGHT JOIN USING merged column = COALESCE(l, r)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- When using JOIN ... USING(col), PG merges the column with COALESCE semantics.
-- For unmatched right rows, the merged column should show the right side's value.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS jnk_l CASCADE;
DROP TABLE IF EXISTS jnk_r CASCADE;
CREATE TABLE jnk_l (id int, lv text);
CREATE TABLE jnk_r (id int, rv text);
INSERT INTO jnk_l VALUES (1, 'left1');
INSERT INTO jnk_r VALUES (1, 'right1'), (2, 'right2');

-- ============================================================================
-- 1. FULL JOIN USING: unmatched right row shows right.id
-- ============================================================================

-- begin-expected
-- columns: id|lv|rv
-- row: 1|left1|right1
-- row: 2||right2
-- end-expected
SELECT id, lv, rv FROM jnk_l FULL JOIN jnk_r USING (id) ORDER BY id;

-- ============================================================================
-- 2. RIGHT JOIN USING: unmatched right row shows right.id
-- ============================================================================

DROP TABLE IF EXISTS jnk_l2 CASCADE;
DROP TABLE IF EXISTS jnk_r2 CASCADE;
CREATE TABLE jnk_l2 (id int, lv text);
CREATE TABLE jnk_r2 (id int, rv text);
INSERT INTO jnk_l2 VALUES (1, 'left1');
INSERT INTO jnk_r2 VALUES (1, 'right1'), (3, 'right3');

-- begin-expected
-- columns: id|lv|rv
-- row: 1|left1|right1
-- row: 3||right3
-- end-expected
SELECT id, lv, rv FROM jnk_l2 RIGHT JOIN jnk_r2 USING (id) ORDER BY id;

-- ============================================================================
-- 3. FULL JOIN USING: unmatched left row shows left.id
-- ============================================================================

DROP TABLE IF EXISTS jnk_l3 CASCADE;
DROP TABLE IF EXISTS jnk_r3 CASCADE;
CREATE TABLE jnk_l3 (id int, lv text);
CREATE TABLE jnk_r3 (id int, rv text);
INSERT INTO jnk_l3 VALUES (1, 'left1'), (5, 'left5');
INSERT INTO jnk_r3 VALUES (1, 'right1');

-- begin-expected
-- columns: id|lv|rv
-- row: 1|left1|right1
-- row: 5|left5|
-- end-expected
SELECT id, lv, rv FROM jnk_l3 FULL JOIN jnk_r3 USING (id) ORDER BY id;

-- ============================================================================
-- 4. FULL JOIN USING with multiple columns
-- ============================================================================

DROP TABLE IF EXISTS jnk_l4 CASCADE;
DROP TABLE IF EXISTS jnk_r4 CASCADE;
CREATE TABLE jnk_l4 (a int, b int, lv text);
CREATE TABLE jnk_r4 (a int, b int, rv text);
INSERT INTO jnk_l4 VALUES (1, 10, 'L');
INSERT INTO jnk_r4 VALUES (2, 20, 'R');

-- begin-expected
-- columns: a|b|lv|rv
-- row: 1|10|L|
-- row: 2|20||R
-- end-expected
SELECT a, b, lv, rv FROM jnk_l4 FULL JOIN jnk_r4 USING (a, b) ORDER BY COALESCE(a, 999);

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS jnk_l CASCADE;
DROP TABLE IF EXISTS jnk_r CASCADE;
DROP TABLE IF EXISTS jnk_l2 CASCADE;
DROP TABLE IF EXISTS jnk_r2 CASCADE;
DROP TABLE IF EXISTS jnk_l3 CASCADE;
DROP TABLE IF EXISTS jnk_r3 CASCADE;
DROP TABLE IF EXISTS jnk_l4 CASCADE;
DROP TABLE IF EXISTS jnk_r4 CASCADE;
