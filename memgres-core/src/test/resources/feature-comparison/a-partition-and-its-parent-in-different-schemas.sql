-- ============================================================================
-- Feature Comparison: a partition and the table it partitions in different schemas
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- CREATE TABLE s.c PARTITION OF s.p did not work at all: the qualified parent
-- name was not read, so the partition never found the table it was declared
-- for. Everything below is written with the schema spelled out on both sides,
-- and the partition is put in a schema of its own wherever PostgreSQL allows
-- it -- including the sub-partitioned middle of a three-schema hierarchy.
-- ============================================================================

DROP SCHEMA IF EXISTS zzt4b_s CASCADE;
DROP SCHEMA IF EXISTS zzt4b_t CASCADE;
DROP SCHEMA IF EXISTS zzt4b_u CASCADE;
CREATE SCHEMA zzt4b_s;
CREATE SCHEMA zzt4b_t;
CREATE SCHEMA zzt4b_u;

-- ============================================================================
-- 1. The parent is named with its schema, and the partition may carry another
-- ============================================================================

CREATE TABLE zzt4b_s.p (i int, k text) PARTITION BY RANGE (i);
CREATE TABLE zzt4b_s.c0 PARTITION OF zzt4b_s.p FOR VALUES FROM (0) TO (10);
CREATE TABLE zzt4b_t.c1 PARTITION OF zzt4b_s.p FOR VALUES FROM (10) TO (20);
CREATE TABLE zzt4b_t.cd PARTITION OF zzt4b_s.p DEFAULT;
INSERT INTO zzt4b_s.p VALUES (1,'a'), (11,'b'), (99,'z');

-- stmt: a row written through the parent reaches the partition that holds its
-- key, whichever schema that partition lives in
-- begin-expected
-- columns: i | k
-- row: 1 | a
-- end-expected
SELECT i, k FROM zzt4b_s.c0 ORDER BY i;

-- begin-expected
-- columns: i | k
-- row: 11 | b
-- end-expected
SELECT i, k FROM zzt4b_t.c1 ORDER BY i;

-- stmt: and the row no bound accepts lands in the DEFAULT partition in the other schema
-- begin-expected
-- columns: i | k
-- row: 99 | z
-- end-expected
SELECT i, k FROM zzt4b_t.cd ORDER BY i;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*)::int AS n FROM zzt4b_s.p;

-- stmt: the partitioned table is not itself a partition; each partition is one,
-- and each is an ordinary relation in the schema it was created in
-- begin-expected
-- columns: nspname | relname | relkind | relispartition
-- row: zzt4b_s | c0 | r | t
-- row: zzt4b_s | p | p | f
-- row: zzt4b_t | c1 | r | t
-- row: zzt4b_t | cd | r | t
-- end-expected
SELECT n.nspname, c.relname, c.relkind, c.relispartition
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname IN ('zzt4b_s','zzt4b_t') AND c.relkind IN ('r','p')
 ORDER BY 1, 2;

-- stmt: pg_inherits joins the parent's namespace to the child's
-- begin-expected
-- columns: pnsp | par | cnsp | chi
-- row: zzt4b_s | p | zzt4b_s | c0
-- row: zzt4b_s | p | zzt4b_t | c1
-- row: zzt4b_s | p | zzt4b_t | cd
-- end-expected
SELECT pn.nspname AS pnsp, pc.relname AS par, cn.nspname AS cnsp, cc.relname AS chi
  FROM pg_inherits i
  JOIN pg_class pc ON pc.oid = i.inhparent
  JOIN pg_namespace pn ON pn.oid = pc.relnamespace
  JOIN pg_class cc ON cc.oid = i.inhrelid
  JOIN pg_namespace cn ON cn.oid = cc.relnamespace
 WHERE pn.nspname IN ('zzt4b_s','zzt4b_t')
 ORDER BY 1, 2, 3, 4;

-- stmt: the bound the partition in the other schema was given is the bound it kept
-- begin-expected
-- columns: bound
-- row: FOR VALUES FROM (10) TO (20)
-- end-expected
SELECT pg_get_expr(c.relpartbound, c.oid) AS bound
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'zzt4b_t' AND c.relname = 'c1';

-- ============================================================================
-- 2. ATTACH and DETACH name both relations with their schemas
-- ============================================================================

CREATE TABLE zzt4b_t.free (i int, k text);
ALTER TABLE zzt4b_s.p ATTACH PARTITION zzt4b_t.free FOR VALUES FROM (20) TO (30);

-- begin-expected
-- columns: relispartition
-- row: t
-- end-expected
SELECT c.relispartition FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'zzt4b_t' AND c.relname = 'free';

INSERT INTO zzt4b_s.p VALUES (25,'y');

-- stmt: the attached table takes the rows its bound accepts
-- begin-expected
-- columns: i | k
-- row: 25 | y
-- end-expected
SELECT i, k FROM zzt4b_t.free ORDER BY i;

ALTER TABLE zzt4b_s.p DETACH PARTITION zzt4b_t.free;

-- stmt: after DETACH it is a table of its own again
-- begin-expected
-- columns: relispartition
-- row: f
-- end-expected
SELECT c.relispartition FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'zzt4b_t' AND c.relname = 'free';

-- stmt: keeping the rows it was given
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM zzt4b_t.free;

-- stmt: which the parent no longer reads
-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*)::int AS n FROM zzt4b_s.p;

-- stmt: and the inheritance row is gone with the attachment
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_inherits i
  JOIN pg_class cc ON cc.oid = i.inhrelid
  JOIN pg_namespace cn ON cn.oid = cc.relnamespace
 WHERE cn.nspname = 'zzt4b_t' AND cc.relname = 'free';

DROP TABLE zzt4b_t.free;
DROP TABLE zzt4b_s.p;

-- ============================================================================
-- 3. Three schemas, one hierarchy: the middle level is a partition and a
--    partitioned table at once
-- ============================================================================

CREATE TABLE zzt4b_s.h (i int NOT NULL, k text CHECK (k <> 'no')) PARTITION BY RANGE (i);
CREATE TABLE zzt4b_t.mid PARTITION OF zzt4b_s.h FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (i);
CREATE TABLE zzt4b_u.leaf PARTITION OF zzt4b_t.mid FOR VALUES FROM (0) TO (50);
CREATE TABLE zzt4b_u.leaf2 PARTITION OF zzt4b_t.mid FOR VALUES FROM (50) TO (100);
INSERT INTO zzt4b_s.h VALUES (1,'a'), (60,'b');

-- stmt: a row written at the root is routed two levels down, across two schemas
-- begin-expected
-- columns: i | k
-- row: 1 | a
-- end-expected
SELECT i, k FROM zzt4b_u.leaf ORDER BY i;

-- begin-expected
-- columns: i | k
-- row: 1 | a
-- row: 60 | b
-- end-expected
SELECT i, k FROM zzt4b_t.mid ORDER BY i;

-- stmt: the root's CHECK constraint is reported against the leaf that refused the row
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "leaf" violates check constraint "h_k_check"
-- end-expected-error
INSERT INTO zzt4b_s.h VALUES (2,'no');

-- stmt: a row written straight into a leaf must satisfy that leaf's bound
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "leaf" violates partition constraint
-- end-expected-error
INSERT INTO zzt4b_u.leaf VALUES (60,'c');

-- stmt: and a key no bound accepts is refused by the root, which has no DEFAULT
-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation "h" found for row
-- end-expected-error
INSERT INTO zzt4b_s.h VALUES (500,'far');

-- stmt: an update that moves the key moves the row to the leaf that now holds it
UPDATE zzt4b_s.h SET i = 70 WHERE i = 1;

-- begin-expected
-- columns: i | k
-- row: 60 | b
-- row: 70 | a
-- end-expected
SELECT i, k FROM zzt4b_u.leaf2 ORDER BY i;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM zzt4b_u.leaf;

-- stmt: a column added at the root reaches the leaves in the third schema
ALTER TABLE zzt4b_s.h ADD COLUMN extra int DEFAULT 7;

-- begin-expected
-- columns: column_name
-- row: i
-- row: k
-- row: extra
-- end-expected
SELECT column_name FROM information_schema.columns
 WHERE table_schema = 'zzt4b_u' AND table_name = 'leaf2' ORDER BY ordinal_position;

-- stmt: the middle level is a partition of the root and partitioned itself
-- begin-expected
-- columns: nspname | relname | relkind | relispartition
-- row: zzt4b_s | h | p | f
-- row: zzt4b_t | mid | p | t
-- row: zzt4b_u | leaf | r | t
-- row: zzt4b_u | leaf2 | r | t
-- end-expected
SELECT n.nspname, c.relname, c.relkind, c.relispartition
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname IN ('zzt4b_s','zzt4b_t','zzt4b_u') AND c.relkind IN ('r','p')
 ORDER BY 1, 2;

-- begin-expected
-- columns: pnsp | par | cnsp | chi
-- row: zzt4b_s | h | zzt4b_t | mid
-- row: zzt4b_t | mid | zzt4b_u | leaf
-- row: zzt4b_t | mid | zzt4b_u | leaf2
-- end-expected
SELECT pn.nspname AS pnsp, pc.relname AS par, cn.nspname AS cnsp, cc.relname AS chi
  FROM pg_inherits i
  JOIN pg_class pc ON pc.oid = i.inhparent
  JOIN pg_namespace pn ON pn.oid = pc.relnamespace
  JOIN pg_class cc ON cc.oid = i.inhrelid
  JOIN pg_namespace cn ON cn.oid = cc.relnamespace
 WHERE pn.nspname IN ('zzt4b_s','zzt4b_t','zzt4b_u')
 ORDER BY 1, 2, 3, 4;

-- stmt: TRUNCATE at the root empties the leaves in the other schemas
TRUNCATE zzt4b_s.h;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM zzt4b_u.leaf2;

-- stmt: and dropping the root takes every level of the hierarchy with it
DROP TABLE zzt4b_s.h;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname IN ('zzt4b_s','zzt4b_t','zzt4b_u') AND c.relkind IN ('r','p');

-- ============================================================================
-- 4. An unqualified partition of a qualified parent is created where the
--    search path says, not where the parent lives
-- ============================================================================

SET search_path = zzt4b_t;
CREATE TABLE zzt4b_s.hash (i int, k text) PARTITION BY HASH (i);
CREATE TABLE h0 PARTITION OF zzt4b_s.hash FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE h1 PARTITION OF zzt4b_s.hash FOR VALUES WITH (MODULUS 2, REMAINDER 1);

-- begin-expected
-- columns: nspname | relname | relispartition
-- row: zzt4b_t | h0 | t
-- row: zzt4b_t | h1 | t
-- end-expected
SELECT n.nspname, c.relname, c.relispartition
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relname IN ('h0','h1') ORDER BY 1, 2;

INSERT INTO zzt4b_s.hash SELECT g, 'v' FROM generate_series(1,20) g;

-- begin-expected
-- columns: n
-- row: 20
-- end-expected
SELECT count(*)::int AS n FROM zzt4b_s.hash;

-- stmt: and every row reached one of the two partitions in the other schema
-- begin-expected
-- columns: all_routed
-- row: t
-- end-expected
SELECT (SELECT count(*) FROM zzt4b_t.h0) + (SELECT count(*) FROM zzt4b_t.h1) = 20 AS all_routed;

SET search_path = public;
DROP SCHEMA zzt4b_s CASCADE;
DROP SCHEMA zzt4b_t CASCADE;
DROP SCHEMA zzt4b_u CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_namespace WHERE nspname LIKE 'zzt4b%';
