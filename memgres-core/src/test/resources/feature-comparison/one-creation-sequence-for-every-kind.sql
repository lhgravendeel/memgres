-- One creation sequence, whatever the kind of object.
--
-- PostgreSQL numbers every catalogue row from one sequence at the moment the row is written, and
-- that number is what settles the order of everything a catalogue reports by it: pg_class as a
-- client reads it, pg_type, pg_proc, pg_policy, and the list of dependents a refused DROP prints.
-- A relation, a type, a routine and a policy all draw from the same sequence, so the numbers of
-- two objects of different kinds say which of them was made first. A list holding more than one
-- kind therefore reads in the order the objects were made, not relations first and everything
-- else after them.
--
-- The DETAIL text of a refusal cannot be checked here -- this harness compares two errors by
-- SQLSTATE alone -- so the ordered DETAIL and the NOTICE that CASCADE raises are asserted over
-- JDBC in CatalogueTextAndRuleLifecycleTest. What is read as data below is the ordering itself.
-- Every answer was measured against PostgreSQL 18.


-- ============================================================================
-- Relations of every kind, numbered in the order they were created
-- ============================================================================

CREATE TABLE cso_zt (i int);
CREATE TABLE cso_at (i int);
CREATE VIEW cso_zv AS SELECT i FROM cso_zt;
CREATE SEQUENCE cso_asq;
CREATE TYPE cso_ze AS ENUM ('a');
CREATE DOMAIN cso_zd AS int;
CREATE TYPE cso_zc AS (a int);
CREATE MATERIALIZED VIEW cso_zm AS SELECT i FROM cso_zt;

-- Creation order, not name order. A composite type owns a pg_class row of its own and takes its
-- place among the relations by when it was made.
-- begin-expected
-- columns: n
-- row: cso_zt
-- row: cso_at
-- row: cso_zv
-- row: cso_asq
-- row: cso_zc
-- row: cso_zm
-- end-expected
SELECT c.relname AS n FROM pg_class c JOIN pg_namespace ns ON ns.oid = c.relnamespace
WHERE ns.nspname = 'public' AND c.relname LIKE 'cso!_%' ESCAPE '!' ORDER BY c.oid;

-- A type made after a relation is numbered after it: one sequence, not one per kind.
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (SELECT min(oid) FROM pg_type WHERE typname = 'cso_ze')
     > (SELECT max(oid) FROM pg_class WHERE relname = 'cso_asq') AS r;

-- A composite made after a domain is numbered after it, whatever kind either of them is.
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (SELECT oid FROM pg_type WHERE typname = 'cso_zc')
     > (SELECT oid FROM pg_type WHERE typname = 'cso_zd') AS r;


-- ============================================================================
-- Routines, and the overloads of one name, numbered in the order they were made
-- ============================================================================

CREATE FUNCTION cso_zf() RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE FUNCTION cso_af() RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE FUNCTION cso_zf(a int) RETURNS int LANGUAGE sql AS 'SELECT a';
CREATE FUNCTION cso_mf() RETURNS int LANGUAGE sql AS 'SELECT 1';

-- Each overload of a name is a routine of its own with a row of its own, and takes its place by
-- when it was written rather than beside the other overloads of its name.
-- begin-expected
-- columns: n
-- row: cso_zf/0
-- row: cso_af/0
-- row: cso_zf/1
-- row: cso_mf/0
-- end-expected
SELECT p.proname || '/' || p.pronargs AS n FROM pg_proc p
JOIN pg_namespace ns ON ns.oid = p.pronamespace
WHERE ns.nspname = 'public' AND p.proname LIKE 'cso!_%f' ESCAPE '!' ORDER BY p.oid;

-- A routine made after a type is numbered after it.
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (SELECT oid FROM pg_proc WHERE proname = 'cso_af')
     > (SELECT oid FROM pg_type WHERE typname = 'cso_zc') AS r;


-- ============================================================================
-- A number is never handed out twice, and a rename never moves one
-- ============================================================================

DROP TYPE cso_ze;
CREATE TYPE cso_ze AS ENUM ('b');

-- The second cso_ze is a new object and is numbered where it was made, not where the first one
-- stood.
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (SELECT oid FROM pg_type WHERE typname = 'cso_ze')
     > (SELECT oid FROM pg_proc WHERE proname = 'cso_mf') AS r;

ALTER DOMAIN cso_zd RENAME TO cso_zd2;

-- A renamed type is the same object and keeps its place.
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (SELECT oid FROM pg_type WHERE typname = 'cso_zd2')
     < (SELECT oid FROM pg_type WHERE typname = 'cso_zc') AS r;

DROP MATERIALIZED VIEW cso_zm;
DROP VIEW cso_zv;
DROP TABLE cso_zt;
DROP TABLE cso_at;
DROP SEQUENCE cso_asq;
DROP TYPE cso_ze;
DROP DOMAIN cso_zd2;
DROP TYPE cso_zc;
DROP FUNCTION cso_zf();
DROP FUNCTION cso_af();
DROP FUNCTION cso_zf(int);
DROP FUNCTION cso_mf();


-- ============================================================================
-- One schema holding all three kinds, read in one order
-- ============================================================================

CREATE SCHEMA cso_s;
CREATE TABLE cso_s.t (i int);
CREATE TYPE cso_s.e AS ENUM ('a');
CREATE FUNCTION cso_s.f() RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE TABLE cso_s.u (i int);
CREATE DOMAIN cso_s.d AS int;

-- This is the order a refused DROP SCHEMA names them in.
-- begin-expected
-- columns: n
-- row: t
-- row: e
-- row: f
-- row: u
-- row: d
-- end-expected
SELECT n FROM (
  SELECT c.relname AS n, c.oid AS o FROM pg_class c
  JOIN pg_namespace ns ON ns.oid = c.relnamespace WHERE ns.nspname = 'cso_s'
  UNION ALL
  SELECT t.typname, t.oid FROM pg_type t
  JOIN pg_namespace ns ON ns.oid = t.typnamespace
  WHERE ns.nspname = 'cso_s' AND t.typtype IN ('e', 'd')
  UNION ALL
  SELECT p.proname, p.oid FROM pg_proc p
  JOIN pg_namespace ns ON ns.oid = p.pronamespace WHERE ns.nspname = 'cso_s'
) q ORDER BY o;

DROP SCHEMA cso_s CASCADE;


-- ============================================================================
-- A schema holding every kind is refused, and the refusal names them all
-- ============================================================================

CREATE SCHEMA cso_a;
CREATE TABLE cso_a.zt (i int);
CREATE TABLE cso_a.at (i int);
CREATE VIEW cso_a.zv AS SELECT i FROM cso_a.zt;
CREATE SEQUENCE cso_a.asq;
CREATE TYPE cso_a.ze AS ENUM ('a');
CREATE DOMAIN cso_a.zd AS int;
CREATE TYPE cso_a.zc AS (a int);
CREATE FUNCTION cso_a.zf() RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE MATERIALIZED VIEW cso_a.zm AS SELECT i FROM cso_a.zt;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema cso_a because other objects depend on it
-- detail-like: table cso_a.zt depends on schema cso_a
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP SCHEMA cso_a;

DROP SCHEMA cso_a CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_namespace WHERE nspname = 'cso_a';


-- ============================================================================
-- A policy is a catalogue row too, numbered when it is created
-- ============================================================================

CREATE TABLE cso_pt (i int);
CREATE FUNCTION cso_pf() RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE POLICY cso_pp ON cso_pt USING (i = cso_pf());
CREATE VIEW cso_pv AS SELECT cso_pf() AS a;

-- A policy has an identity of its own, so it can be counted and ordered like anything else.
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_policy WHERE polname = 'cso_pp';

-- The policy was written after the routine and before the view, so it is numbered between them --
-- which is why a refused DROP of the routine both hang from names the policy first.
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (SELECT oid FROM pg_policy WHERE polname = 'cso_pp')
     > (SELECT oid FROM pg_proc WHERE proname = 'cso_pf') AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (SELECT oid FROM pg_policy WHERE polname = 'cso_pp')
     < (SELECT oid FROM pg_class WHERE relname = 'cso_pv') AS r;

DROP VIEW cso_pv;
DROP POLICY cso_pp ON cso_pt;
DROP FUNCTION cso_pf();
DROP TABLE cso_pt;
