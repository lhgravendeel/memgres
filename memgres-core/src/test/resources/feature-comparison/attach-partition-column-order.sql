-- ============================================================================
-- Feature Comparison: ATTACH PARTITION matches columns by name
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- ATTACH validates a partition's columns by name, and each table keeps its own
-- attribute order. Rows therefore have to be permuted as they cross the
-- boundary: written through the parent they land in the child's order, and read
-- back through the parent they come out in the parent's order.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS apc_parent CASCADE;
DROP TABLE IF EXISTS apc_child CASCADE;
DROP TABLE IF EXISTS apc_p2 CASCADE;
DROP TABLE IF EXISTS apc_c2 CASCADE;

CREATE TABLE apc_parent (id int, region text, val int) PARTITION BY RANGE (id);
-- deliberately a different column order from the parent
CREATE TABLE apc_child (region text, id int, val int);
ALTER TABLE apc_parent ATTACH PARTITION apc_child FOR VALUES FROM (1) TO (100);

INSERT INTO apc_parent VALUES (1, 'b', 10);

-- ============================================================================
-- 1. Values land in the column the name selects, not the position
-- ============================================================================

-- begin-expected
-- columns: id | region | val
-- row: 1, b, 10
-- end-expected
SELECT id, region, val FROM apc_parent;

-- begin-expected
-- columns: id | region | val
-- row: 1, b, 10
-- end-expected
SELECT id, region, val FROM apc_child;

-- ============================================================================
-- 2. SELECT * keeps each table's own attribute order
-- ============================================================================

-- begin-expected
-- columns: region | id | val
-- row: b, 1, 10
-- end-expected
SELECT * FROM apc_child;

-- begin-expected
-- columns: id | region | val
-- row: 1, b, 10
-- end-expected
SELECT * FROM apc_parent;

-- ============================================================================
-- 3. A same-named column with a different type is still rejected
-- ============================================================================

CREATE TABLE apc_p2 (id int, region text) PARTITION BY RANGE (id);
CREATE TABLE apc_c2 (region int, id text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: different type for column
-- end-expected-error
ALTER TABLE apc_p2 ATTACH PARTITION apc_c2 FOR VALUES FROM (1) TO (100);

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE apc_c2 CASCADE;
DROP TABLE apc_p2 CASCADE;
DROP TABLE apc_parent CASCADE;
