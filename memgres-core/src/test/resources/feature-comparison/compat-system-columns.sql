-- ============================================================================
-- Feature Comparison: System Columns (ctid, xmin, xmax, cmin, cmax)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG 18 provides system columns on every heap table:
--   ctid     - physical tuple ID (page, offset), distinct per row
--   xmin     - inserting transaction's ID
--   xmax     - deleting/locking transaction's ID (0 for live rows)
--   cmin     - command ID within the inserting transaction
--   cmax     - command ID within the deleting transaction
--   tableoid - OID of the table (already works in Memgres)
--
-- Memgres gaps:
--   ctid  -> always returns "(0,0)" regardless of actual row
--   xmin  -> column not found (42703)
--   xmax  -> column not found (42703)
--   cmin  -> column not found (42703)
--   cmax  -> column not found (42703)
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

DROP TABLE IF EXISTS syscol_data;
CREATE TABLE syscol_data (id integer PRIMARY KEY, val text);
INSERT INTO syscol_data VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');

-- ============================================================================
-- 1. ctid should return distinct values per row
-- ============================================================================

-- note: In PG, each row has a unique ctid like (0,1), (0,2), (0,3).
-- note: Memgres returns "(0,0)" for all rows.

-- begin-expected
-- columns: all_distinct
-- row: true
-- end-expected
SELECT count(DISTINCT ctid) = count(*) AS all_distinct FROM syscol_data;

-- ============================================================================
-- 2. xmin should be queryable on inserted rows
-- ============================================================================

-- note: In PG, xmin is the transaction ID that inserted the row.
-- note: Memgres throws 42703 "column xmin does not exist".

-- begin-expected
-- columns: has_xmin
-- row: true
-- end-expected
SELECT xmin IS NOT NULL AS has_xmin FROM syscol_data WHERE id = 1;

-- ============================================================================
-- 3. xmax should be zero for live, unlocked rows
-- ============================================================================

-- begin-expected
-- columns: xmax_zero
-- row: true
-- end-expected
SELECT xmax = 0 AS xmax_zero FROM syscol_data WHERE id = 1;

-- ============================================================================
-- 4. cmin should be queryable
-- ============================================================================

-- begin-expected
-- columns: has_cmin
-- row: true
-- end-expected
SELECT cmin >= 0 AS has_cmin FROM syscol_data WHERE id = 1;

-- ============================================================================
-- 5. cmax should be queryable
-- ============================================================================

-- begin-expected
-- columns: has_cmax
-- row: true
-- end-expected
SELECT cmax >= 0 AS has_cmax FROM syscol_data WHERE id = 1;

-- ============================================================================
-- 6. ctid in WHERE clause should select exactly one row
-- ============================================================================

-- note: Use a subquery to get a real ctid, then filter by it.

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM syscol_data
WHERE ctid = (SELECT ctid FROM syscol_data WHERE id = 1);

-- ============================================================================
-- 7. Rows inserted in same statement share same xmin
-- ============================================================================

-- begin-expected
-- columns: same_xmin
-- row: true
-- end-expected
SELECT count(DISTINCT xmin) = 1 AS same_xmin FROM syscol_data;

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP TABLE syscol_data;
