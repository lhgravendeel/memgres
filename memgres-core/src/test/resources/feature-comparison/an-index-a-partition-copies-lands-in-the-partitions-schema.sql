-- ============================================================================
-- Feature Comparison: the index copy a partition is given lands in the
-- partition's own schema
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A partitioned table hands every partition a copy of each of its indexes.
-- When the partition lives in another schema, the copy belongs to the
-- partition's schema and is named after the partition -- in both directions:
-- an index declared before the partition existed, and one declared after.
-- ============================================================================

DROP SCHEMA IF EXISTS zzt4b_s CASCADE;
DROP SCHEMA IF EXISTS zzt4b_t CASCADE;
DROP SCHEMA IF EXISTS zzt4b_u CASCADE;
CREATE SCHEMA zzt4b_s;
CREATE SCHEMA zzt4b_t;
CREATE SCHEMA zzt4b_u;

-- ============================================================================
-- 1. An index declared before the partition in the other schema was created
-- ============================================================================

CREATE TABLE zzt4b_s.p (i int, k text) PARTITION BY RANGE (i);
CREATE INDEX ON zzt4b_s.p (k);
CREATE TABLE zzt4b_t.c PARTITION OF zzt4b_s.p FOR VALUES FROM (0) TO (10);

-- stmt: the copy is in the partition's schema, named for the partition
-- begin-expected
-- columns: schemaname | tablename | indexname
-- row: zzt4b_s | p | p_k_idx
-- row: zzt4b_t | c | c_k_idx
-- end-expected
SELECT schemaname, tablename, indexname FROM pg_indexes
 WHERE schemaname IN ('zzt4b_s','zzt4b_t') ORDER BY 1, 2, 3;

-- ============================================================================
-- 2. And one declared after it already existed
-- ============================================================================

CREATE UNIQUE INDEX ON zzt4b_s.p (i);

-- begin-expected
-- columns: schemaname | tablename | indexname
-- row: zzt4b_s | p | p_i_idx
-- row: zzt4b_s | p | p_k_idx
-- row: zzt4b_t | c | c_i_idx
-- row: zzt4b_t | c | c_k_idx
-- end-expected
SELECT schemaname, tablename, indexname FROM pg_indexes
 WHERE schemaname IN ('zzt4b_s','zzt4b_t') ORDER BY 1, 2, 3;

-- stmt: each copy is recorded as a child of the index it was copied from
-- begin-expected
-- columns: par | cnsp | chi
-- row: p_i_idx | zzt4b_t | c_i_idx
-- row: p_k_idx | zzt4b_t | c_k_idx
-- end-expected
SELECT pc.relname AS par, cn.nspname AS cnsp, cc.relname AS chi
  FROM pg_inherits i
  JOIN pg_class pc ON pc.oid = i.inhparent
  JOIN pg_class cc ON cc.oid = i.inhrelid
  JOIN pg_namespace cn ON cn.oid = cc.relnamespace
 WHERE cc.relkind = 'i' AND cn.nspname IN ('zzt4b_s','zzt4b_t')
 ORDER BY 1, 2, 3;

INSERT INTO zzt4b_s.p VALUES (1,'x');

-- stmt: the copy in the partition's schema is the index the duplicate is reported against
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "c_i_idx"
-- end-expected-error
INSERT INTO zzt4b_s.p VALUES (1,'y');

-- stmt: and writing straight into the partition reports the same index
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "c_i_idx"
-- end-expected-error
INSERT INTO zzt4b_t.c VALUES (1,'z');

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM zzt4b_s.p;

-- ============================================================================
-- 3. Every level of a three-schema hierarchy gets its own copy
-- ============================================================================

CREATE TABLE zzt4b_u.mid PARTITION OF zzt4b_s.p FOR VALUES FROM (10) TO (100) PARTITION BY RANGE (i);
CREATE TABLE zzt4b_t.leaf PARTITION OF zzt4b_u.mid FOR VALUES FROM (10) TO (20);

-- begin-expected
-- columns: schemaname | tablename | indexname
-- row: zzt4b_s | p | p_i_idx
-- row: zzt4b_s | p | p_k_idx
-- row: zzt4b_t | c | c_i_idx
-- row: zzt4b_t | c | c_k_idx
-- row: zzt4b_t | leaf | leaf_i_idx
-- row: zzt4b_t | leaf | leaf_k_idx
-- row: zzt4b_u | mid | mid_i_idx
-- row: zzt4b_u | mid | mid_k_idx
-- end-expected
SELECT schemaname, tablename, indexname FROM pg_indexes
 WHERE schemaname IN ('zzt4b_s','zzt4b_t','zzt4b_u') ORDER BY 1, 2, 3;

DROP SCHEMA zzt4b_s CASCADE;
DROP SCHEMA zzt4b_t CASCADE;
DROP SCHEMA zzt4b_u CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_namespace WHERE nspname LIKE 'zzt4b%';
