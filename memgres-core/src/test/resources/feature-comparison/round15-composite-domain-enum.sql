-- ============================================================================
-- Feature Comparison: Round 15 — Composite types, domains, enums
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r15_cde CASCADE;
CREATE SCHEMA r15_cde;
SET search_path = r15_cde, public;

-- ============================================================================
-- SECTION A: ALTER TYPE … ATTRIBUTE
-- ============================================================================

CREATE TYPE r15_comp_add AS (a int, b text);
ALTER TYPE r15_comp_add ADD ATTRIBUTE c double precision;

-- 1. ADD ATTRIBUTE
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_attribute a
  JOIN pg_type t ON a.attrelid = t.typrelid
  WHERE t.typname='r15_comp_add' AND a.attname='c';

CREATE TYPE r15_comp_drop AS (a int, b text, c int);
ALTER TYPE r15_comp_drop DROP ATTRIBUTE b;

-- 2. DROP ATTRIBUTE
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM pg_attribute a
  JOIN pg_type t ON a.attrelid = t.typrelid
  WHERE t.typname='r15_comp_drop' AND a.attname='b' AND NOT a.attisdropped;

CREATE TYPE r15_comp_alt AS (a int, b text);
ALTER TYPE r15_comp_alt ALTER ATTRIBUTE a TYPE bigint;

-- 3. ALTER ATTRIBUTE TYPE
SELECT t2.typname AS ty
  FROM pg_attribute a
  JOIN pg_type t ON a.attrelid = t.typrelid
  JOIN pg_type t2 ON a.atttypid = t2.oid
  WHERE t.typname='r15_comp_alt' AND a.attname='a';

CREATE TYPE r15_comp_ren AS (a int, b text);
ALTER TYPE r15_comp_ren RENAME ATTRIBUTE b TO c;

-- 4. RENAME ATTRIBUTE
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_attribute a
  JOIN pg_type t ON a.attrelid = t.typrelid
  WHERE t.typname='r15_comp_ren' AND a.attname='c';

-- ============================================================================
-- SECTION B: ROW()::composite cast
-- ============================================================================

CREATE TYPE r15_rowct AS (a int, b text);

-- 5. Row literal cast
-- begin-expected
-- columns: v
-- row: (1,x)
-- end-expected
SELECT (ROW(1, 'x')::r15_rowct)::text AS v;

CREATE TYPE r15_rowfd AS (a int, b text);

-- 6. Composite field extraction
-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT ((ROW(10, 'y')::r15_rowfd)).a AS a;

-- ============================================================================
-- SECTION C: Enum functions
-- ============================================================================

CREATE TYPE r15_en AS ENUM ('a','b','c');

-- 7. enum_cmp(a,b) negative
SELECT enum_cmp('a'::r15_en, 'b'::r15_en)::int AS r;

-- 8. enum_cmp(b,a) positive
SELECT enum_cmp('b'::r15_en, 'a'::r15_en)::int AS r;

-- 9. enum_cmp(a,a) zero
-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT enum_cmp('a'::r15_en, 'a'::r15_en)::int AS r;

CREATE TYPE r15_enfl AS ENUM ('low','mid','high');

-- 10. enum_first
-- begin-expected
-- columns: v
-- row: low
-- end-expected
SELECT enum_first(NULL::r15_enfl)::text AS v;

-- 11. enum_last
-- begin-expected
-- columns: v
-- row: high
-- end-expected
SELECT enum_last(NULL::r15_enfl)::text AS v;

CREATE TYPE r15_enr AS ENUM ('x','y','z');

-- 12. enum_range full
-- begin-expected
-- columns: v
-- row: {x,y,z}
-- end-expected
SELECT enum_range(NULL::r15_enr)::text AS v;

CREATE TYPE r15_enrb AS ENUM ('w','x','y','z');

-- 13. enum_range bounded
-- begin-expected
-- columns: v
-- row: {x,y}
-- end-expected
SELECT enum_range('x'::r15_enrb, 'y'::r15_enrb)::text AS v;

-- ============================================================================
-- SECTION D: ALTER DOMAIN VALIDATE / RENAME CONSTRAINT
-- ============================================================================

CREATE DOMAIN r15_dom_rc AS int CONSTRAINT r15_dc_pos CHECK (VALUE > 0);
ALTER DOMAIN r15_dom_rc RENAME CONSTRAINT r15_dc_pos TO r15_dc_positive;

-- 14. RENAME CONSTRAINT
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_constraint c
  JOIN pg_type t ON c.contypid = t.oid
  WHERE t.typname='r15_dom_rc' AND c.conname='r15_dc_positive';

CREATE DOMAIN r15_dom_v AS int;
ALTER DOMAIN r15_dom_v ADD CONSTRAINT r15_dv_pos CHECK (VALUE > 0) NOT VALID;

-- 15. NOT VALID initially
SELECT count(*)::int AS c FROM pg_constraint c
  JOIN pg_type t ON c.contypid = t.oid
  WHERE t.typname='r15_dom_v' AND c.conname='r15_dv_pos' AND NOT c.convalidated;

ALTER DOMAIN r15_dom_v VALIDATE CONSTRAINT r15_dv_pos;

-- 16. VALIDATE flips convalidated
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_constraint c
  JOIN pg_type t ON c.contypid = t.oid
  WHERE t.typname='r15_dom_v' AND c.conname='r15_dv_pos' AND c.convalidated;

-- ============================================================================
-- SECTION E: Domain enforcement on columns
-- ============================================================================

CREATE DOMAIN r15_dom_e AS int CHECK (VALUE > 0);
CREATE TABLE r15_dom_col (id int, v r15_dom_e);
INSERT INTO r15_dom_col VALUES (1, 5);

-- 17. Domain CHECK rejects invalid insert
-- begin-expected-error
-- message-like: check
-- end-expected-error
INSERT INTO r15_dom_col VALUES (2, -1);

CREATE DOMAIN r15_dom_upd AS int CHECK (VALUE > 0);
CREATE TABLE r15_dom_u (id int, v r15_dom_upd);
INSERT INTO r15_dom_u VALUES (1, 5);

-- 18. Domain CHECK rejects invalid UPDATE
-- begin-expected-error
-- message-like: cannot
-- end-expected-error
UPDATE r15_dom_u SET v=-5 WHERE id=1;

CREATE DOMAIN r15_dom_nn AS int NOT NULL;
CREATE TABLE r15_dom_nn_c (v r15_dom_nn);

-- 19. Domain NOT NULL
-- begin-expected-error
-- message-like: null
-- end-expected-error
INSERT INTO r15_dom_nn_c VALUES (NULL);
