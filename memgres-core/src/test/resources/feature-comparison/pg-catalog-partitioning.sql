-- ============================================================================
-- Feature Comparison: pg_inherits / pg_partitioned_table Catalog (B2)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Partitioning works functionally, but catalog views pg_inherits and
-- pg_partitioned_table must be populated for pg_dump, ORMs, and admin tools.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS pcp_test CASCADE;
CREATE SCHEMA pcp_test;
SET search_path = pcp_test, public;

-- ============================================================================
-- 1. Basic RANGE partitioned table: pg_partitioned_table populated
-- ============================================================================

CREATE TABLE pcp_range (id integer, val text) PARTITION BY RANGE (id);
CREATE TABLE pcp_range_p1 PARTITION OF pcp_range FOR VALUES FROM (1) TO (100);
CREATE TABLE pcp_range_p2 PARTITION OF pcp_range FOR VALUES FROM (100) TO (200);

-- begin-expected
-- columns: has_entry
-- row: true
-- end-expected
SELECT count(*) > 0 AS has_entry
FROM pg_partitioned_table
WHERE partrelid = 'pcp_range'::regclass;

-- ============================================================================
-- 2. pg_partitioned_table columns for RANGE partition
-- ============================================================================

-- begin-expected
-- columns: partstrat, partnatts
-- row: r, 1
-- end-expected
SELECT partstrat, partnatts
FROM pg_partitioned_table
WHERE partrelid = 'pcp_range'::regclass;

-- ============================================================================
-- 3. pg_inherits shows parent-child relationships
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_inherits
WHERE inhparent = 'pcp_range'::regclass;

-- ============================================================================
-- 4. pg_inherits child OIDs match partition tables
-- ============================================================================

-- begin-expected
-- columns: has_p1, has_p2
-- row: true, true
-- end-expected
SELECT
  (SELECT count(*) > 0 FROM pg_inherits WHERE inhparent = 'pcp_range'::regclass AND inhrelid = 'pcp_range_p1'::regclass) AS has_p1,
  (SELECT count(*) > 0 FROM pg_inherits WHERE inhparent = 'pcp_range'::regclass AND inhrelid = 'pcp_range_p2'::regclass) AS has_p2;

-- ============================================================================
-- 5. pg_get_partkeydef() returns partition key definition
-- ============================================================================

-- begin-expected
-- columns: partkeydef
-- row: RANGE (id)
-- end-expected
SELECT pg_get_partkeydef('pcp_range'::regclass) AS partkeydef;

-- ============================================================================
-- 6. LIST partitioned table
-- ============================================================================

CREATE TABLE pcp_list (id integer, status text) PARTITION BY LIST (status);
CREATE TABLE pcp_list_active PARTITION OF pcp_list FOR VALUES IN ('active');
CREATE TABLE pcp_list_inactive PARTITION OF pcp_list FOR VALUES IN ('inactive', 'archived');

-- begin-expected
-- columns: partstrat, partnatts
-- row: l, 1
-- end-expected
SELECT partstrat, partnatts
FROM pg_partitioned_table
WHERE partrelid = 'pcp_list'::regclass;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_inherits
WHERE inhparent = 'pcp_list'::regclass;

-- ============================================================================
-- 7. HASH partitioned table
-- ============================================================================

CREATE TABLE pcp_hash (id integer, val text) PARTITION BY HASH (id);
CREATE TABLE pcp_hash_p0 PARTITION OF pcp_hash FOR VALUES WITH (MODULUS 3, REMAINDER 0);
CREATE TABLE pcp_hash_p1 PARTITION OF pcp_hash FOR VALUES WITH (MODULUS 3, REMAINDER 1);
CREATE TABLE pcp_hash_p2 PARTITION OF pcp_hash FOR VALUES WITH (MODULUS 3, REMAINDER 2);

-- begin-expected
-- columns: partstrat, partnatts
-- row: h, 1
-- end-expected
SELECT partstrat, partnatts
FROM pg_partitioned_table
WHERE partrelid = 'pcp_hash'::regclass;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_inherits
WHERE inhparent = 'pcp_hash'::regclass;

-- ============================================================================
-- 8. Multi-column partition key
-- ============================================================================

CREATE TABLE pcp_multi (a integer, b integer, val text) PARTITION BY RANGE (a, b);
CREATE TABLE pcp_multi_p1 PARTITION OF pcp_multi FOR VALUES FROM (1, 1) TO (10, 10);

-- begin-expected
-- columns: partnatts
-- row: 2
-- end-expected
SELECT partnatts
FROM pg_partitioned_table
WHERE partrelid = 'pcp_multi'::regclass;

-- begin-expected
-- columns: partkeydef
-- row: RANGE (a, b)
-- end-expected
SELECT pg_get_partkeydef('pcp_multi'::regclass) AS partkeydef;

-- ============================================================================
-- 9. Multi-level partitioning (sub-partitions)
-- ============================================================================

CREATE TABLE pcp_multi_level (id integer, region text, val text)
  PARTITION BY LIST (region);

CREATE TABLE pcp_ml_us PARTITION OF pcp_multi_level FOR VALUES IN ('us')
  PARTITION BY RANGE (id);
CREATE TABLE pcp_ml_us_1 PARTITION OF pcp_ml_us FOR VALUES FROM (1) TO (1000);
CREATE TABLE pcp_ml_us_2 PARTITION OF pcp_ml_us FOR VALUES FROM (1000) TO (2000);

CREATE TABLE pcp_ml_eu PARTITION OF pcp_multi_level FOR VALUES IN ('eu');

-- Top-level has 2 children
-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_inherits
WHERE inhparent = 'pcp_multi_level'::regclass;

-- US sub-partition has 2 children
-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_inherits
WHERE inhparent = 'pcp_ml_us'::regclass;

-- US sub-partition is also in pg_partitioned_table
-- begin-expected
-- columns: partstrat
-- row: r
-- end-expected
SELECT partstrat
FROM pg_partitioned_table
WHERE partrelid = 'pcp_ml_us'::regclass;

-- ============================================================================
-- 10. DETACH PARTITION removes from pg_inherits
-- ============================================================================

ALTER TABLE pcp_range DETACH PARTITION pcp_range_p2;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_inherits
WHERE inhparent = 'pcp_range'::regclass;

DROP TABLE pcp_range_p2;

-- ============================================================================
-- 11. ATTACH PARTITION adds to pg_inherits
-- ============================================================================

CREATE TABLE pcp_range_p3 (id integer, val text);
ALTER TABLE pcp_range ATTACH PARTITION pcp_range_p3 FOR VALUES FROM (200) TO (300);

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_inherits
WHERE inhparent = 'pcp_range'::regclass;

-- ============================================================================
-- 12. pg_class relkind for partitioned vs partition
-- ============================================================================

-- begin-expected
-- columns: parent_relkind, child_relkind
-- row: p, r
-- end-expected
SELECT
  (SELECT relkind FROM pg_class WHERE relname = 'pcp_range') AS parent_relkind,
  (SELECT relkind FROM pg_class WHERE relname = 'pcp_range_p1') AS child_relkind;

-- ============================================================================
-- 13. pg_class relispartition flag
-- ============================================================================

-- begin-expected
-- columns: parent_is_partition, child_is_partition
-- row: false, true
-- end-expected
SELECT
  (SELECT relispartition FROM pg_class WHERE relname = 'pcp_range') AS parent_is_partition,
  (SELECT relispartition FROM pg_class WHERE relname = 'pcp_range_p1') AS child_is_partition;

-- ============================================================================
-- 14. Table inheritance (non-partition) in pg_inherits
-- ============================================================================

CREATE TABLE pcp_parent (id integer, val text);
CREATE TABLE pcp_child () INHERITS (pcp_parent);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_inherits
WHERE inhparent = 'pcp_parent'::regclass;

-- NOT in pg_partitioned_table (not a partitioned table)
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_partitioned_table
WHERE partrelid = 'pcp_parent'::regclass;

-- ============================================================================
-- 15. NO INHERIT removes from pg_inherits
-- ============================================================================

ALTER TABLE pcp_child NO INHERIT pcp_parent;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_inherits
WHERE inhparent = 'pcp_parent'::regclass;

-- ============================================================================
-- 16. INHERIT adds to pg_inherits
-- ============================================================================

ALTER TABLE pcp_child INHERIT pcp_parent;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_inherits
WHERE inhparent = 'pcp_parent'::regclass;

-- ============================================================================
-- 17. pg_inherits inhseqno
-- ============================================================================

-- begin-expected
-- columns: inhseqno
-- row: 1
-- end-expected
SELECT inhseqno
FROM pg_inherits
WHERE inhparent = 'pcp_parent'::regclass AND inhrelid = 'pcp_child'::regclass;

-- ============================================================================
-- 18. DEFAULT partition
-- ============================================================================

CREATE TABLE pcp_def_parent (id integer, val text) PARTITION BY LIST (val);
CREATE TABLE pcp_def_a PARTITION OF pcp_def_parent FOR VALUES IN ('a');
CREATE TABLE pcp_def_default PARTITION OF pcp_def_parent DEFAULT;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_inherits
WHERE inhparent = 'pcp_def_parent'::regclass;

-- Data routes correctly
INSERT INTO pcp_def_parent VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::integer AS cnt FROM pcp_def_a;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM pcp_def_default;

-- ============================================================================
-- 19. pg_dump-style partition introspection query
-- ============================================================================

-- note: This is the kind of query pg_dump uses to discover partitions
-- begin-expected
-- columns: has_results
-- row: true
-- end-expected
SELECT count(*) > 0 AS has_results FROM (
  SELECT c.relname AS child, p.relname AS parent
  FROM pg_inherits i
  JOIN pg_class c ON i.inhrelid = c.oid
  JOIN pg_class p ON i.inhparent = p.oid
  WHERE p.relnamespace = 'pcp_test'::regnamespace
) sub;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA pcp_test CASCADE;
SET search_path = public;
