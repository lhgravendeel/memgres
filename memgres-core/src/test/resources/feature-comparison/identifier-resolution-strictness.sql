-- ============================================================================
-- Feature Comparison: identifiers that resolve to nothing
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A bare identifier matching no column, variable or system value must be an
-- error, not its own name as text — a typo that becomes a plausible value
-- defeats the declared type of whatever it is assigned to. Covers the SELECT
-- list, PL/pgSQL expressions, composite variables used as whole values, and
-- the aggregate FILTER clause that reaches the same fallback.
-- ============================================================================

DROP TYPE IF EXISTS irs_outer CASCADE;
DROP TYPE IF EXISTS irs_inner CASCADE;
DROP TABLE IF EXISTS irs_t CASCADE;
CREATE TABLE irs_t (a int, b text);
INSERT INTO irs_t VALUES (1, 'x'), (2, 'y');
CREATE TYPE irs_inner AS (a int, b int);
CREATE TYPE irs_outer AS (x int, y irs_inner);

-- ============================================================================
-- 1. Unresolvable identifiers in queries
-- ============================================================================
SELECT nosuchthing;
SELECT nosuchcol FROM irs_t;
SELECT 1 FROM irs_t WHERE nosuchcol = 1;
SELECT a FROM irs_t ORDER BY nosuchcol;
SELECT nosuchthing + 1;
SELECT upper(nosuchthing);
SELECT count(*) FROM irs_t GROUP BY nosuchcol;
-- resolvable references are unaffected
SELECT string_agg(b, ',' ORDER BY a) AS a FROM irs_t;
SELECT string_agg(x::text, ',' ORDER BY x) AS a FROM (SELECT a AS x FROM irs_t) t;
SELECT string_agg(xx::text, ',' ORDER BY xx) AS a FROM (SELECT a AS xx FROM irs_t ORDER BY xx) t;
SELECT 'literal' AS a;
SELECT current_user IS NOT NULL AS a;
SELECT count(*)::text AS a FROM irs_t t1 JOIN irs_t t2 ON t1.a = t2.a;

-- ============================================================================
-- 2. Unresolvable identifiers inside PL/pgSQL
-- ============================================================================
DO $$ declare a int := b; b int := 5; begin raise notice '%', a; end $$;
DO $$ declare a int; begin a := nosuchthing; end $$;
DO $$ declare a text; begin a := nosuchthing; end $$;
DO $$ begin raise notice '%', nosuchthing; end $$;
-- declared variables still resolve
DO $$ declare a int := 5; b int; begin b := a; if b <> 5 then raise 'wrong'; end if; end $$;
DO $$ declare a int := 5; begin if a::text <> '5' then raise 'wrong'; end if; end $$;

-- ============================================================================
-- 3. Composite variables used as whole values
-- ============================================================================
DROP FUNCTION IF EXISTS irs_one();
CREATE FUNCTION irs_one() RETURNS text LANGUAGE plpgsql AS $$
declare v irs_inner;
begin v.a := 1; v.b := 2; return v::text; end $$;
SELECT irs_one() AS a;

DROP FUNCTION IF EXISTS irs_nested();
CREATE FUNCTION irs_nested() RETURNS text LANGUAGE plpgsql AS $$
declare v irs_outer;
begin v.x := 1; v.y.a := 2; v.y.b := 3; return v::text; end $$;
SELECT irs_nested() AS a;

DROP FUNCTION IF EXISTS irs_typed();
CREATE FUNCTION irs_typed() RETURNS irs_inner LANGUAGE plpgsql AS $$
declare v irs_inner;
begin v.a := 1; v.b := 2; return v; end $$;
SELECT irs_typed()::text AS a;
SELECT (irs_typed()).a::text AS a;

-- a composite whose fields need quoting in the row's text form
DROP TYPE IF EXISTS irs_txt CASCADE;
CREATE TYPE irs_txt AS (a text, b text);
DROP FUNCTION IF EXISTS irs_quoted();
CREATE FUNCTION irs_quoted() RETURNS text LANGUAGE plpgsql AS $$
declare v irs_txt;
begin v.a := 'has,comma'; v.b := 'has "quote"'; return v::text; end $$;
SELECT irs_quoted() AS a;

DROP FUNCTION IF EXISTS irs_empty();
CREATE FUNCTION irs_empty() RETURNS text LANGUAGE plpgsql AS $$
declare v irs_txt;
begin v.a := ''; v.b := NULL; return v::text; end $$;
SELECT irs_empty() AS a;

-- field access still works alongside whole-value use
DROP FUNCTION IF EXISTS irs_field();
CREATE FUNCTION irs_field() RETURNS text LANGUAGE plpgsql AS $$
declare v irs_outer;
begin v.x := 7; v.y.a := 8; return v.x::text || '/' || (v.y).a::text; end $$;
SELECT irs_field() AS a;

-- a record variable holding a whole row
DO $$ declare r record; begin
  select * into r from irs_t where a = 1;
  if r.b <> 'x' then raise 'wrong'; end if;
end $$;

-- ============================================================================
-- 4. A row assigned to a scalar variable
-- ============================================================================
DO $$ declare r irs_t%rowtype; v int; begin select * into r from irs_t where a = 1; v := r; end $$;
DO $$ declare r irs_t%rowtype; v bigint; begin select * into r from irs_t where a = 1; v := r; end $$;
DO $$ declare r irs_t%rowtype; v boolean; begin select * into r from irs_t where a = 1; v := r; end $$;
-- assigning to a text variable is allowed, since a row has a text form
DO $$ declare r irs_t%rowtype; v text; begin select * into r from irs_t where a = 1; v := r; end $$;

-- normalization form keywords are part of the grammar, not column references
SELECT normalize(U&'\FB01', NFKC) AS a;
SELECT normalize(U&'\FB01', NFKD) AS a;
SELECT normalize(U&'\FB01', NFC) AS a;
SELECT normalize(U&'\FB01', NFD) AS a;
SELECT normalize(U&'\FB01') AS a;
SELECT (U&'\FB01' IS NFKC NORMALIZED)::text AS a;

-- ============================================================================
-- 5. FILTER may not swallow the expression before it
-- ============================================================================
SELECT b FILTER (WHERE a = 1) FROM irs_t;
SELECT a FILTER (WHERE a = 1) FROM irs_t;
SELECT 1 filter;
-- FILTER on a real aggregate is unaffected
SELECT count(*) FILTER (WHERE a = 1)::text AS a FROM irs_t;
SELECT sum(a) FILTER (WHERE b = 'x')::text AS a FROM irs_t;
SELECT string_agg(b, ',') FILTER (WHERE a > 0) AS a FROM irs_t;
-- and the word is still usable as a quoted or AS-introduced alias
SELECT 1 AS filter;
SELECT 1 AS "filter";

DROP FUNCTION IF EXISTS irs_one();
DROP FUNCTION IF EXISTS irs_nested();
DROP FUNCTION IF EXISTS irs_typed();
DROP FUNCTION IF EXISTS irs_quoted();
DROP FUNCTION IF EXISTS irs_empty();
DROP FUNCTION IF EXISTS irs_field();
DROP TYPE IF EXISTS irs_txt CASCADE;
DROP TYPE IF EXISTS irs_outer CASCADE;
DROP TYPE IF EXISTS irs_inner CASCADE;
DROP TABLE IF EXISTS irs_t CASCADE;
