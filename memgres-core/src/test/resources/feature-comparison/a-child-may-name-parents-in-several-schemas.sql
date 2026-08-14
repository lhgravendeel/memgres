-- ============================================================================
-- Feature Comparison: a child may name its parents in several schemas
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- INHERITS (s.b) did not parse: a parent could only be named without a schema,
-- so no table could inherit from a table in another schema, and the check that
-- refuses the same parent twice keyed on the bare name -- which is not what
-- PostgreSQL refuses. PostgreSQL refuses the same relation twice, decided by
-- identity: two parents of the same name in two schemas are two parents.
-- ============================================================================

DROP SCHEMA IF EXISTS zzt4b_s CASCADE;
DROP SCHEMA IF EXISTS zzt4b_t CASCADE;
DROP SCHEMA IF EXISTS zzt4b_u CASCADE;
DROP SCHEMA IF EXISTS "zzt4b_MiX" CASCADE;
CREATE SCHEMA zzt4b_s;
CREATE SCHEMA zzt4b_t;
CREATE SCHEMA zzt4b_u;
CREATE SCHEMA "zzt4b_MiX";

-- ============================================================================
-- 1. Three parents, three schemas, merged in the order they were written
-- ============================================================================

CREATE TABLE zzt4b_s.a1 (sa int);
CREATE TABLE zzt4b_t.a2 (ta text);
CREATE TABLE zzt4b_u.a3 (ua date);
CREATE TABLE zzt4b_s.three (own int) INHERITS (zzt4b_s.a1, zzt4b_t.a2, zzt4b_u.a3);

-- stmt: the inherited columns come first, in the order the parents were named,
-- and the child's own column last
-- begin-expected
-- columns: column_name | ordinal_position
-- row: sa | 1
-- row: ta | 2
-- row: ua | 3
-- row: own | 4
-- end-expected
SELECT column_name, ordinal_position FROM information_schema.columns
 WHERE table_schema = 'zzt4b_s' AND table_name = 'three' ORDER BY ordinal_position;

-- stmt: pg_inherits numbers the parents in that same order, each in its own schema
-- begin-expected
-- columns: pnsp | par | inhseqno
-- row: zzt4b_s | a1 | 1
-- row: zzt4b_t | a2 | 2
-- row: zzt4b_u | a3 | 3
-- end-expected
SELECT pn.nspname AS pnsp, pc.relname AS par, i.inhseqno
  FROM pg_inherits i
  JOIN pg_class pc ON pc.oid = i.inhparent
  JOIN pg_namespace pn ON pn.oid = pc.relnamespace
  JOIN pg_class cc ON cc.oid = i.inhrelid
  JOIN pg_namespace cn ON cn.oid = cc.relnamespace
 WHERE cn.nspname = 'zzt4b_s' AND cc.relname = 'three'
 ORDER BY i.inhseqno;

-- stmt: a parent in another schema is marked as having a child
-- begin-expected
-- columns: relhassubclass
-- row: t
-- end-expected
SELECT c.relhassubclass FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'zzt4b_t' AND c.relname = 'a2';

-- stmt: a written schema and a name left to the search path may stand side by side
SET search_path = zzt4b_t;
CREATE TABLE zzt4b_u.mixed (m int) INHERITS (zzt4b_s.a1, a2);

-- begin-expected
-- columns: column_name | ordinal_position
-- row: sa | 1
-- row: ta | 2
-- row: m | 3
-- end-expected
SELECT column_name, ordinal_position FROM information_schema.columns
 WHERE table_schema = 'zzt4b_u' AND table_name = 'mixed' ORDER BY ordinal_position;

-- begin-expected
-- columns: pnsp | par | inhseqno
-- row: zzt4b_s | a1 | 1
-- row: zzt4b_t | a2 | 2
-- end-expected
SELECT pn.nspname AS pnsp, pc.relname AS par, i.inhseqno
  FROM pg_inherits i
  JOIN pg_class pc ON pc.oid = i.inhparent
  JOIN pg_namespace pn ON pn.oid = pc.relnamespace
  JOIN pg_class cc ON cc.oid = i.inhrelid
  JOIN pg_namespace cn ON cn.oid = cc.relnamespace
 WHERE cn.nspname = 'zzt4b_u' AND cc.relname = 'mixed'
 ORDER BY i.inhseqno;

SET search_path = public;

-- ============================================================================
-- 2. Two parents of the same name in two schemas are two parents
-- ============================================================================

CREATE TABLE zzt4b_s.b (i int, a text);
CREATE TABLE zzt4b_t.b (i int, x text);
CREATE TABLE zzt4b_s.two (j int) INHERITS (zzt4b_s.b, zzt4b_t.b);

-- stmt: the column both parents declare is merged into one, and the columns of
-- the second parent follow the first's
-- begin-expected
-- columns: column_name | ordinal_position
-- row: i | 1
-- row: a | 2
-- row: x | 3
-- row: j | 4
-- end-expected
SELECT column_name, ordinal_position FROM information_schema.columns
 WHERE table_schema = 'zzt4b_s' AND table_name = 'two' ORDER BY ordinal_position;

-- begin-expected
-- columns: pnsp | par | inhseqno
-- row: zzt4b_s | b | 1
-- row: zzt4b_t | b | 2
-- end-expected
SELECT pn.nspname AS pnsp, pc.relname AS par, i.inhseqno
  FROM pg_inherits i
  JOIN pg_class pc ON pc.oid = i.inhparent
  JOIN pg_namespace pn ON pn.oid = pc.relnamespace
  JOIN pg_class cc ON cc.oid = i.inhrelid
  JOIN pg_namespace cn ON cn.oid = cc.relnamespace
 WHERE cn.nspname = 'zzt4b_s' AND cc.relname = 'two'
 ORDER BY i.inhseqno;

-- stmt: a child of two ordinary tables is not a partition of either
-- begin-expected
-- columns: relispartition | relkind
-- row: f | r
-- end-expected
SELECT c.relispartition, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'zzt4b_s' AND c.relname = 'two';

INSERT INTO zzt4b_s.two VALUES (1,'a','x',9);

-- stmt: the row is read through the parent, which holds none of its own
-- begin-expected
-- columns: i | a
-- row: 1 | a
-- end-expected
SELECT i, a FROM zzt4b_s.b ORDER BY 1;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM ONLY zzt4b_s.b;

-- stmt: and through the second parent, in the other schema, just as well
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM zzt4b_t.b;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM ONLY zzt4b_t.b;

-- stmt: a column added to the qualified parent reaches the child
ALTER TABLE zzt4b_s.b ADD COLUMN later int;

-- begin-expected
-- columns: column_name | ordinal_position
-- row: i | 1
-- row: a | 2
-- row: x | 3
-- row: j | 4
-- row: later | 5
-- end-expected
SELECT column_name, ordinal_position FROM information_schema.columns
 WHERE table_schema = 'zzt4b_s' AND table_name = 'two' ORDER BY ordinal_position;

-- stmt: and a child of that child, in a third schema, holds every column of both
CREATE TABLE zzt4b_t.chain (z int) INHERITS (zzt4b_s.two);

-- begin-expected
-- columns: column_name
-- row: i
-- row: a
-- row: x
-- row: j
-- row: later
-- row: z
-- end-expected
SELECT column_name FROM information_schema.columns
 WHERE table_schema = 'zzt4b_t' AND table_name = 'chain' ORDER BY ordinal_position;

-- ============================================================================
-- 3. What the parent declares, the child in the other schema is held to
-- ============================================================================

CREATE TABLE "zzt4b_MiX"."Par" (i int NOT NULL DEFAULT 3 CHECK (i > 0), t text);
CREATE TABLE zzt4b_s.kid () INHERITS ("zzt4b_MiX"."Par");
INSERT INTO zzt4b_s.kid (t) VALUES ('x');

-- stmt: the parent's DEFAULT was used for the column the insert did not name
-- begin-expected
-- columns: i | t
-- row: 3 | x
-- end-expected
SELECT i, t FROM zzt4b_s.kid ORDER BY 1;

-- begin-expected
-- columns: i | t
-- row: 3 | x
-- end-expected
SELECT i, t FROM "zzt4b_MiX"."Par" ORDER BY 1;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM ONLY "zzt4b_MiX"."Par";

-- stmt: the parent's CHECK reaches the child under the name the parent gave it
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "kid" violates check constraint "Par_i_check"
-- end-expected-error
INSERT INTO zzt4b_s.kid (i, t) VALUES (0, 'bad');

-- stmt: and so does the parent's NOT NULL
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "i" of relation "kid" violates not-null constraint
-- end-expected-error
INSERT INTO zzt4b_s.kid (i, t) VALUES (NULL, 'bad');

-- stmt: both stand on the child, named for the parent that handed them down
-- begin-expected
-- columns: conname
-- row: Par_i_check
-- row: Par_i_not_null
-- end-expected
SELECT conname FROM pg_constraint co
  JOIN pg_class c ON c.oid = co.conrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'zzt4b_s' AND c.relname = 'kid' ORDER BY 1;

-- begin-expected
-- columns: pnsp | par | inhseqno
-- row: zzt4b_MiX | Par | 1
-- end-expected
SELECT pn.nspname AS pnsp, pc.relname AS par, i.inhseqno
  FROM pg_inherits i
  JOIN pg_class pc ON pc.oid = i.inhparent
  JOIN pg_namespace pn ON pn.oid = pc.relnamespace
  JOIN pg_class cc ON cc.oid = i.inhrelid
  JOIN pg_namespace cn ON cn.oid = cc.relnamespace
 WHERE cn.nspname = 'zzt4b_s' AND cc.relname = 'kid'
 ORDER BY i.inhseqno;

-- ============================================================================
-- 4. What a qualified parent list is refused for
-- ============================================================================

-- stmt: the same relation named twice, reported by its bare name
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "b" would be inherited from more than once
-- end-expected-error
CREATE TABLE zzt4b_s.dup (j int) INHERITS (zzt4b_s.b, zzt4b_s.b);

-- stmt: a qualifier naming no schema is reported as the schema it named
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zzt4b_nosuch" does not exist
-- end-expected-error
CREATE TABLE zzt4b_s.noschema (j int) INHERITS (zzt4b_nosuch.b);

-- stmt: a schema that holds no such relation is reported with the whole name written
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zzt4b_s.nosuch" does not exist
-- end-expected-error
CREATE TABLE zzt4b_s.norel (j int) INHERITS (zzt4b_s.nosuch);

-- stmt: none of the three left a relation behind
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'zzt4b_s' AND c.relname IN ('dup','noschema','norel');

DROP SCHEMA zzt4b_s CASCADE;
DROP SCHEMA zzt4b_t CASCADE;
DROP SCHEMA zzt4b_u CASCADE;
DROP SCHEMA "zzt4b_MiX" CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_namespace WHERE nspname LIKE 'zzt4b%';
