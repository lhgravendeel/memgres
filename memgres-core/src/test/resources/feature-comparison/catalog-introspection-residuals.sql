-- ============================================================================
-- Feature Comparison: catalog / introspection residuals
-- Target: PostgreSQL 18 vs Memgres
-- Covers bugs-review residuals H14, H15, H16, M14, M15, M19, M21, M22, L12, L13.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS cir_child CASCADE;
DROP TABLE IF EXISTS cir_parent CASCADE;
DROP TABLE IF EXISTS cir_t CASCADE;
DROP TABLE IF EXISTS "CirMixed" CASCADE;
DROP VIEW IF EXISTS cir_v CASCADE;
DROP TABLE IF EXISTS cir_vt CASCADE;
DROP DOMAIN IF EXISTS cir_posint CASCADE;
DROP TYPE IF EXISTS cir_addr CASCADE;
DROP FUNCTION IF EXISTS cir_add(int, int) CASCADE;

CREATE TYPE cir_addr AS (street text, zip int);
CREATE DOMAIN cir_posint AS integer CHECK (VALUE > 0);
CREATE TABLE cir_t (
  id int GENERATED ALWAYS AS IDENTITY,
  home cir_addr,
  q cir_posint,
  created timestamptz DEFAULT now(),
  name text NOT NULL
);
CREATE TABLE cir_parent (id int PRIMARY KEY);
CREATE TABLE cir_child (id int, pid int REFERENCES cir_parent(id));
CREATE TABLE "CirMixed" (x int);

-- ============================================================================
-- H14: information_schema.columns
-- ============================================================================

-- Composite-typed column reports USER-DEFINED / the composite type name.
-- begin-expected
-- columns: data_type | udt_name
-- row: USER-DEFINED, cir_addr
-- end-expected
SELECT data_type, udt_name FROM information_schema.columns
WHERE table_name = 'cir_t' AND column_name = 'home';

-- Domain column reports the BASE type in data_type, with domain_name set.
-- begin-expected
-- columns: data_type | udt_name | domain_name
-- row: integer, int4, cir_posint
-- end-expected
SELECT data_type, udt_name, domain_name FROM information_schema.columns
WHERE table_name = 'cir_t' AND column_name = 'q';

-- DEFAULT now() renders as now() (not CURRENT_TIMESTAMP).
-- begin-expected
-- columns: column_default
-- row: now()
-- end-expected
SELECT column_default FROM information_schema.columns
WHERE table_name = 'cir_t' AND column_name = 'created';

-- ============================================================================
-- H15: information_schema.check_constraints
-- ============================================================================

-- NOT NULL constraints appear with a "<col> IS NOT NULL" clause.
-- begin-expected
-- columns: check_clause
-- row: name IS NOT NULL
-- end-expected
SELECT check_clause FROM information_schema.check_constraints
WHERE constraint_name = 'cir_t_name_not_null';

-- Domain CHECK constraint appears with its clause.
-- begin-expected
-- columns: check_clause
-- row: (VALUE > 0)
-- end-expected
SELECT check_clause FROM information_schema.check_constraints
WHERE constraint_name = 'cir_posint_check';

-- ============================================================================
-- H16: pg_get_constraintdef
-- ============================================================================

-- NOT NULL constraint definition.
-- begin-expected
-- columns: def
-- row: NOT NULL name
-- end-expected
SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint
WHERE conname = 'cir_t_name_not_null';

-- Domain CHECK constraint definition.
-- begin-expected
-- columns: def
-- row: CHECK ((VALUE > 0))
-- end-expected
SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint
WHERE conname = 'cir_posint_check';

-- FK definition is NOT schema-qualified under the current search_path.
-- begin-expected
-- columns: def
-- row: FOREIGN KEY (pid) REFERENCES cir_parent(id)
-- end-expected
SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint
WHERE conname = 'cir_child_pid_fkey';

-- ============================================================================
-- M14: identity column has no pg_attrdef default
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::text AS cnt FROM pg_attrdef d
JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attrelid = 'cir_t'::regclass AND a.attname = 'id';

-- ============================================================================
-- M15: regtype / regclass / regproc
-- ============================================================================

-- oid::regtype prints the user type name.
-- begin-expected
-- columns: t
-- row: cir_posint
-- end-expected
SELECT 'cir_posint'::regtype::text AS t;

-- regclass output quotes mixed-case names.
-- begin-expected
-- columns: t
-- row: "CirMixed"
-- end-expected
SELECT (SELECT oid FROM pg_class WHERE relname = 'CirMixed')::regclass::text AS t;

-- Ambiguous bare-name ::regproc errors (42725).
-- begin-expected-error
-- sqlstate: 42725
-- message-like: more than one function named "lower"
-- end-expected-error
SELECT 'lower'::regproc;

-- ============================================================================
-- M19: one-argument pg_get_viewdef uses the pretty multi-line form
-- ============================================================================

CREATE TABLE cir_vt (id int, name text);
CREATE VIEW cir_v AS SELECT id, name FROM cir_vt;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT (position(E'\n' IN pg_get_viewdef('cir_v'::regclass)) > 0)::int::text AS c;

-- ============================================================================
-- M21: information_schema gaps
-- ============================================================================

CREATE FUNCTION cir_add(a int, b int) RETURNS int LANGUAGE sql AS 'SELECT a + b';

-- routines.external_language is uppercase.
-- begin-expected
-- columns: lang
-- row: SQL
-- end-expected
SELECT external_language AS lang FROM information_schema.routines
WHERE routine_name = 'cir_add';

-- ============================================================================
-- M22: pg_tables.hastriggers true for FK endpoints
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::text AS cnt FROM pg_tables
WHERE tablename IN ('cir_parent', 'cir_child') AND hastriggers;

-- ============================================================================
-- L12: pg_settings exposes the canonical TimeZone name
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM pg_settings WHERE name = 'TimeZone';

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP FUNCTION IF EXISTS cir_add(int, int) CASCADE;
DROP VIEW IF EXISTS cir_v CASCADE;
DROP TABLE IF EXISTS cir_vt CASCADE;
DROP TABLE IF EXISTS cir_child CASCADE;
DROP TABLE IF EXISTS cir_parent CASCADE;
DROP TABLE IF EXISTS cir_t CASCADE;
DROP TABLE IF EXISTS "CirMixed" CASCADE;
DROP DOMAIN IF EXISTS cir_posint CASCADE;
DROP TYPE IF EXISTS cir_addr CASCADE;
