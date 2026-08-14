-- ============================================================================
-- Feature Comparison: the schema written beside a parent decides which parent
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Once a qualified parent could be written at all, it had to be resolved
-- through the schema it names rather than through the search path. Every
-- statement below has a second relation of the same bare name standing in the
-- way -- in the search path, or in a third schema -- and names the one it
-- wants with its schema.
-- ============================================================================

DROP SCHEMA IF EXISTS zzt4b_s CASCADE;
DROP SCHEMA IF EXISTS zzt4b_t CASCADE;
DROP SCHEMA IF EXISTS zzt4b_u CASCADE;
CREATE SCHEMA zzt4b_s;
CREATE SCHEMA zzt4b_t;
CREATE SCHEMA zzt4b_u;

-- ============================================================================
-- 1. Two schemas hold a partitioned table of the same name
-- ============================================================================

CREATE TABLE zzt4b_s.p (i int, k text) PARTITION BY RANGE (i);
CREATE TABLE zzt4b_t.p (i int, k text) PARTITION BY RANGE (i);
SET search_path = zzt4b_t, public;
CREATE TABLE zzt4b_u.c PARTITION OF zzt4b_s.p FOR VALUES FROM (0) TO (10);

-- stmt: the partition belongs to the table the qualifier named, not to the one
-- the search path would have found
-- begin-expected
-- columns: pnsp | par
-- row: zzt4b_s | p
-- end-expected
SELECT pn.nspname AS pnsp, pc.relname AS par
  FROM pg_inherits i
  JOIN pg_class pc ON pc.oid = i.inhparent
  JOIN pg_namespace pn ON pn.oid = pc.relnamespace
  JOIN pg_class cc ON cc.oid = i.inhrelid
  JOIN pg_namespace cn ON cn.oid = cc.relnamespace
 WHERE cn.nspname = 'zzt4b_u' AND cc.relname = 'c';

INSERT INTO zzt4b_s.p VALUES (1,'s');

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM zzt4b_u.c;

-- stmt: the table in the search path was left with no partition at all
-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation "p" found for row
-- end-expected-error
INSERT INTO zzt4b_t.p VALUES (1,'t');

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM zzt4b_u.c;

-- ============================================================================
-- 2. The same for a qualified INHERITS parent
-- ============================================================================

CREATE TABLE zzt4b_s.b (i int, s_col text);
CREATE TABLE zzt4b_t.b (i int, t_col text);
CREATE TABLE zzt4b_u.kid (j int) INHERITS (zzt4b_s.b);

-- stmt: the columns are the ones the named schema's table declares
-- begin-expected
-- columns: column_name
-- row: i
-- row: s_col
-- row: j
-- end-expected
SELECT column_name FROM information_schema.columns
 WHERE table_schema = 'zzt4b_u' AND table_name = 'kid' ORDER BY ordinal_position;

-- begin-expected
-- columns: pnsp
-- row: zzt4b_s
-- end-expected
SELECT pn.nspname AS pnsp
  FROM pg_inherits i
  JOIN pg_class pc ON pc.oid = i.inhparent
  JOIN pg_namespace pn ON pn.oid = pc.relnamespace
  JOIN pg_class cc ON cc.oid = i.inhrelid
  JOIN pg_namespace cn ON cn.oid = cc.relnamespace
 WHERE cn.nspname = 'zzt4b_u' AND cc.relname = 'kid';

SET search_path = public;

-- ============================================================================
-- 3. DETACH is refused for a relation that is not this parent's partition
-- ============================================================================

CREATE TABLE zzt4b_u.other (i int, k text);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "other" is not a partition of relation "p"
-- end-expected-error
ALTER TABLE zzt4b_s.p DETACH PARTITION zzt4b_u.other;

-- stmt: and the partition of the other table of that name is not this one's
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "c" is not a partition of relation "p"
-- end-expected-error
ALTER TABLE zzt4b_t.p DETACH PARTITION zzt4b_u.c;

DROP TABLE zzt4b_s.p;
DROP TABLE zzt4b_t.p;
DROP TABLE zzt4b_u.other;
DROP TABLE zzt4b_u.kid;
DROP TABLE zzt4b_s.b;
DROP TABLE zzt4b_t.b;

-- ============================================================================
-- 4. ATTACH attaches the relation the qualifier named
-- ============================================================================

CREATE TABLE zzt4b_s.q (i int, k text) PARTITION BY RANGE (i);
CREATE TABLE zzt4b_t.part (i int, k text);
CREATE TABLE zzt4b_u.part (i int, k text);
ALTER TABLE zzt4b_s.q ATTACH PARTITION zzt4b_u.part FOR VALUES FROM (0) TO (10);

-- begin-expected
-- columns: nspname | relname | relispartition
-- row: zzt4b_t | part | f
-- row: zzt4b_u | part | t
-- end-expected
SELECT n.nspname, c.relname, c.relispartition
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relname = 'part' ORDER BY 1;

INSERT INTO zzt4b_s.q VALUES (5,'five');

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM zzt4b_u.part;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM zzt4b_t.part;

-- stmt: a qualifier naming no schema is refused before anything is attached
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zzt4b_nosuch" does not exist
-- end-expected-error
ALTER TABLE zzt4b_s.q ATTACH PARTITION zzt4b_nosuch.part FOR VALUES FROM (10) TO (20);

-- stmt: and so is a parent whose schema does not exist
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zzt4b_nosuch" does not exist
-- end-expected-error
ALTER TABLE zzt4b_nosuch.q ATTACH PARTITION zzt4b_t.part FOR VALUES FROM (10) TO (20);

-- stmt: what is already a partition cannot be attached again
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "part" is already a partition
-- end-expected-error
ALTER TABLE zzt4b_s.q ATTACH PARTITION zzt4b_u.part FOR VALUES FROM (10) TO (20);

-- begin-expected
-- columns: nspname | relname | relispartition
-- row: zzt4b_t | part | f
-- row: zzt4b_u | part | t
-- end-expected
SELECT n.nspname, c.relname, c.relispartition
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relname = 'part' ORDER BY 1;

-- ============================================================================
-- 5. What a qualified PARTITION OF is refused for
-- ============================================================================

CREATE TABLE zzt4b_s.plain (i int);

-- stmt: a qualified parent that is not partitioned, reported by its bare name
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: "plain" is not partitioned
-- end-expected-error
CREATE TABLE zzt4b_t.notpart PARTITION OF zzt4b_s.plain FOR VALUES FROM (0) TO (10);

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zzt4b_nosuch" does not exist
-- end-expected-error
CREATE TABLE zzt4b_t.nsp PARTITION OF zzt4b_nosuch.q FOR VALUES FROM (10) TO (20);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zzt4b_s.nosuch" does not exist
-- end-expected-error
CREATE TABLE zzt4b_t.nrel PARTITION OF zzt4b_s.nosuch FOR VALUES FROM (10) TO (20);

-- stmt: the partition's own schema must exist too
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zzt4b_nosuch" does not exist
-- end-expected-error
CREATE TABLE zzt4b_nosuch.c PARTITION OF zzt4b_s.q FOR VALUES FROM (10) TO (20);

-- stmt: a bound that overlaps one the parent already has is refused
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: partition "over" would overlap partition "part"
-- end-expected-error
CREATE TABLE zzt4b_t.over PARTITION OF zzt4b_s.q FOR VALUES FROM (5) TO (15);

-- stmt: and a name the target schema already holds
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "part" already exists
-- end-expected-error
CREATE TABLE zzt4b_t.part PARTITION OF zzt4b_s.q FOR VALUES FROM (10) TO (20);

-- stmt: none of the refusals left a relation or a partition behind
-- begin-expected
-- columns: nspname | relname | relispartition
-- row: zzt4b_s | plain | f
-- row: zzt4b_s | q | f
-- row: zzt4b_t | part | f
-- row: zzt4b_u | part | t
-- end-expected
SELECT n.nspname, c.relname, c.relispartition
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname IN ('zzt4b_s','zzt4b_t','zzt4b_u') AND c.relkind IN ('r','p')
 ORDER BY 1, 2;

DROP SCHEMA zzt4b_s CASCADE;
DROP SCHEMA zzt4b_t CASCADE;
DROP SCHEMA zzt4b_u CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_namespace WHERE nspname LIKE 'zzt4b%';
