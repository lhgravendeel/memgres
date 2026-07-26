-- ============================================================================
-- Feature Comparison: object identity across renames
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL records a dependency as an OID, so renaming or moving a relation
-- leaves its foreign keys enforcing and its views reading the same relation --
-- and the deparsed view definition shows the new name. Renaming an enum label
-- onto an existing one is rejected instead of silently duplicating it.
-- ============================================================================

DROP VIEW IF EXISTS oir_v CASCADE;
DROP TABLE IF EXISTS oir_c CASCADE;
DROP TABLE IF EXISTS oir_p CASCADE;
DROP SCHEMA IF EXISTS oir_s CASCADE;
DROP TYPE IF EXISTS oir_e CASCADE;
DROP DOMAIN IF EXISTS oir_d CASCADE;
DROP FUNCTION IF EXISTS oir_f(int);

CREATE TABLE oir_p (id int PRIMARY KEY, v text);
CREATE TABLE oir_c (id int PRIMARY KEY, p int REFERENCES oir_p(id));
INSERT INTO oir_p VALUES (1,'x'),(2,'y');
INSERT INTO oir_c VALUES (10,1);
CREATE VIEW oir_v AS SELECT id, v FROM oir_p;

-- ============================================================================
-- 1. RENAME keeps foreign keys and views pointing at the same relation
-- ============================================================================

ALTER TABLE oir_p RENAME TO oir_p2;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM oir_v;

-- begin-expected
-- columns: conname | reftable
-- row: oir_c_p_fkey, oir_p2
-- end-expected
SELECT conname, confrelid::regclass::text AS reftable
  FROM pg_constraint WHERE conrelid = 'oir_c'::regclass AND contype = 'f';

-- The FK still accepts a valid parent key
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
INSERT INTO oir_c VALUES (11,2) RETURNING 1 AS n;

-- ...and still rejects a missing one
-- begin-expected-error
-- sqlstate: 23503
-- message-like: violates foreign key constraint "oir_c_p_fkey"
-- end-expected-error
INSERT INTO oir_c VALUES (12,99);

-- ...and still restricts the parent delete
-- begin-expected-error
-- sqlstate: 23503
-- message-like: violates foreign key constraint "oir_c_p_fkey" on table "oir_c"
-- end-expected-error
DELETE FROM oir_p2 WHERE id = 1;

ALTER TABLE oir_p2 RENAME TO oir_p;

-- ============================================================================
-- 2. SET SCHEMA keeps dependents working, and frees the old name
-- ============================================================================

CREATE SCHEMA oir_s;
ALTER TABLE oir_p SET SCHEMA oir_s;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM oir_v;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
INSERT INTO oir_c VALUES (13,2) RETURNING 1 AS n;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM oir_c;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM oir_s.oir_p ORDER BY id;

-- The relation is gone from its old schema
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "oir_p" does not exist
-- end-expected-error
SELECT count(*) FROM oir_p;

ALTER TABLE oir_s.oir_p SET SCHEMA public;

-- ============================================================================
-- 3. ALTER TYPE ... RENAME VALUE validates both labels
-- ============================================================================

CREATE TYPE oir_e AS ENUM ('a','b','c');

-- begin-expected-error
-- sqlstate: 42710
-- message-like: enum label "b" already exists
-- end-expected-error
ALTER TYPE oir_e RENAME VALUE 'a' TO 'b';

ALTER TYPE oir_e RENAME VALUE 'a' TO 'z';

-- begin-expected
-- columns: r
-- row: {z,b,c}
-- end-expected
SELECT enum_range(NULL::oir_e)::text AS r;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: is not an existing enum label
-- end-expected-error
ALTER TYPE oir_e RENAME VALUE 'nope' TO 'q';

-- ============================================================================
-- 4. COMMENT on non-table objects is retrievable
-- ============================================================================

CREATE DOMAIN oir_d AS int CHECK (VALUE > 0);
CREATE FUNCTION oir_f(int) RETURNS int AS $$ SELECT $1 + 1 $$ LANGUAGE sql;

COMMENT ON COLUMN oir_v.v IS 'view column comment';
COMMENT ON CONSTRAINT oir_c_p_fkey ON oir_c IS 'fk comment';
COMMENT ON FUNCTION oir_f(int) IS 'function comment';
COMMENT ON DOMAIN oir_d IS 'domain comment';
COMMENT ON TYPE oir_e IS 'enum comment';

-- begin-expected
-- columns: a | b | c | d | e
-- row: view column comment, fk comment, function comment, domain comment, enum comment
-- end-expected
SELECT col_description('oir_v'::regclass, 2) AS a,
       (SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname = 'oir_c_p_fkey') AS b,
       (SELECT obj_description(oid, 'pg_proc') FROM pg_proc WHERE proname = 'oir_f') AS c,
       (SELECT obj_description(oid, 'pg_type') FROM pg_type WHERE typname = 'oir_d') AS d,
       (SELECT obj_description(oid, 'pg_type') FROM pg_type WHERE typname = 'oir_e') AS e;

-- ============================================================================
-- 5. Temp objects live in a pg_temp namespace and are marked temporary
-- ============================================================================

CREATE TEMP TABLE oir_tt (id int);
INSERT INTO oir_tt VALUES (1);

-- begin-expected
-- columns: p | in_temp_ns
-- row: t, true
-- end-expected
SELECT relpersistence AS p,
       relnamespace::regnamespace::text LIKE 'pg\_temp%' AS in_temp_ns
  FROM pg_class WHERE relname = 'oir_tt';

-- begin-expected
-- columns: r
-- row: oir_tt
-- end-expected
SELECT 'oir_tt'::regclass::text AS r;

-- begin-expected
-- columns: has_temp_schema | is_other
-- row: true, false
-- end-expected
SELECT pg_my_temp_schema() <> 0 AS has_temp_schema,
       pg_is_other_temp_schema(pg_my_temp_schema()) AS is_other;

-- A view over a temp table is itself temporary
CREATE VIEW oir_tv AS SELECT * FROM oir_tt;

-- begin-expected
-- columns: p | in_temp_ns
-- row: t, true
-- end-expected
SELECT relpersistence AS p,
       relnamespace::regnamespace::text LIKE 'pg\_temp%' AS in_temp_ns
  FROM pg_class WHERE relname = 'oir_tv';

DROP VIEW oir_tv;
DROP TABLE oir_tt;

DROP VIEW oir_v;
DROP TABLE oir_c;
DROP TABLE oir_p;
DROP SCHEMA oir_s;
DROP TYPE oir_e;
DROP DOMAIN oir_d;
DROP FUNCTION oir_f(int);
