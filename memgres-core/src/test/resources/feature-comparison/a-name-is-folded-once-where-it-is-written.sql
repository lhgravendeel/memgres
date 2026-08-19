-- ============================================================================
-- A name is folded once, where it is written
--
-- PostgreSQL folds an identifier exactly where it is written and nowhere else:
-- an unquoted one is lower-cased as it is read, a quoted one is kept exactly as
-- it stands, and every catalogue and every lookup downstream then matches the
-- result as it is. So a relation, a routine, a constraint, an alias and a window
-- created under a quoted name answer to that name and to no other spelling of
-- it -- which is the whole of what the quotes were written to say. Folding a
-- second time, or matching a name whatever its case, undoes them. Every value
-- below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
DROP TABLE IF EXISTS nf_a CASCADE;
DROP VIEW IF EXISTS "ZzView" CASCADE;
DROP FUNCTION IF EXISTS "ZzFn"() CASCADE;
DROP FUNCTION IF EXISTS zzfn2() CASCADE;
CREATE TABLE nf_a (a int CONSTRAINT "ZzCk" CHECK (a > 0));
INSERT INTO nf_a VALUES (1),(2),(3);
CREATE VIEW "ZzView" AS SELECT 1 AS a;
CREATE FUNCTION "ZzFn"() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE FUNCTION zzfn2() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;

-- ============================================================================
-- An unquoted name is folded, and quoting the folded name names the same thing
-- ============================================================================

-- The relation was created unquoted, so it is filed under nf_a and every
-- unquoted spelling of it folds to that.
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM nf_a;
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM NF_A;
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM Nf_A;

-- Quotes ask for the name exactly as written, which is only the same relation
-- when what they hold is what the fold produced.
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM "nf_a";
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM "NF_A";

-- A column is a name like any other.
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT a FROM nf_a WHERE A = 1;
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT "a" FROM nf_a WHERE a = 1;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT "A" FROM nf_a WHERE a = 1;

-- ============================================================================
-- A quoted name keeps the case it was created with
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM "ZzView";
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM zzview;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM "zzview";
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM ZzView;

-- The catalogue holds the name as it was written, which is why only that
-- spelling reaches it.
-- begin-expected
-- columns: relname
-- row: ZzView
-- end-expected
SELECT relname FROM pg_class WHERE relname = 'ZzView';

-- ============================================================================
-- A routine answers to the name it was declared under
-- ============================================================================

-- begin-expected
-- columns: ZzFn
-- row: 1
-- end-expected
SELECT "ZzFn"();
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT zzfn();
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT "zzfn"();
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT ZzFn();

-- Declared unquoted, so filed folded, and found by anything that folds to it.
-- begin-expected
-- columns: zzfn2
-- row: 2
-- end-expected
SELECT zzfn2();
-- begin-expected
-- columns: zzfn2
-- row: 2
-- end-expected
SELECT ZZFN2();
-- begin-expected
-- columns: zzfn2
-- row: 2
-- end-expected
SELECT "zzfn2"();
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT "ZzFn2"();

-- begin-expected
-- columns: proname
-- row: ZzFn
-- row: zzfn2
-- end-expected
SELECT proname FROM pg_proc WHERE proname IN ('ZzFn', 'zzfn2') ORDER BY proname;

-- ============================================================================
-- A built-in is a name on the same terms
-- ============================================================================

-- begin-expected
-- columns: abs
-- row: 1
-- end-expected
SELECT abs(-1);
-- begin-expected
-- columns: abs
-- row: 1
-- end-expected
SELECT ABS(-1);
-- begin-expected
-- columns: abs
-- row: 1
-- end-expected
SELECT AbS(-1);
-- begin-expected
-- columns: abs
-- row: 1
-- end-expected
SELECT "abs"(-1);
-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
SELECT "ABS"(-1);

-- ============================================================================
-- An alias hides the relation's name, and keeps the case it was written in
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT x.a FROM nf_a x ORDER BY 1;
-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT X.a FROM nf_a x ORDER BY 1;
-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT "x".a FROM nf_a x ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT "X".a FROM nf_a x ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT "Q".a FROM nf_a AS "Q" ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT q.a FROM nf_a AS "Q" ORDER BY 1;

-- An alias is the only name the relation has left inside the query.
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT nf_a.a FROM nf_a AS "Q" ORDER BY 1;

-- ============================================================================
-- A window is the window of that name
-- ============================================================================

-- begin-expected
-- columns: sum
-- row: 1
-- row: 3
-- row: 6
-- end-expected
SELECT sum(a) OVER w FROM nf_a WINDOW w AS (ORDER BY a) ORDER BY 1;
-- begin-expected
-- columns: sum
-- row: 1
-- row: 3
-- row: 6
-- end-expected
SELECT sum(a) OVER W FROM nf_a WINDOW w AS (ORDER BY a) ORDER BY 1;
-- begin-expected
-- columns: sum
-- row: 1
-- row: 3
-- row: 6
-- end-expected
SELECT sum(a) OVER "w" FROM nf_a WINDOW w AS (ORDER BY a) ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
SELECT sum(a) OVER "W" FROM nf_a WINDOW w AS (ORDER BY a) ORDER BY 1;

-- "W" and w are two windows, so a query may define both and name each.
-- begin-expected
-- columns: a | count | count
-- row: 1 | 3 | 1
-- row: 2 | 3 | 1
-- row: 3 | 3 | 1
-- end-expected
SELECT a, count(*) OVER "W", count(*) OVER "w" FROM nf_a WINDOW "W" AS (), "w" AS (PARTITION BY a) ORDER BY a;
-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT a FROM nf_a WINDOW w AS (), "W" AS () ORDER BY a;

-- Whereas "w" and w are one window, defined twice.
-- begin-expected-error
-- sqlstate: 42P20
-- end-expected-error
SELECT a FROM nf_a WINDOW w AS (), "w" AS () ORDER BY a;

-- ============================================================================
-- A constraint is named as it was written
-- ============================================================================

-- begin-expected
-- columns: conname
-- row: ZzCk
-- end-expected
SELECT conname FROM pg_constraint WHERE conrelid = 'nf_a'::regclass ORDER BY conname;

-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
ALTER TABLE nf_a DROP CONSTRAINT zzck;
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
ALTER TABLE nf_a DROP CONSTRAINT "zzck";
-- The name it was created under is the one that reaches it.
ALTER TABLE nf_a DROP CONSTRAINT "ZzCk";

-- begin-expected
-- columns: remaining
-- row: 0
-- end-expected
SELECT count(*) AS remaining FROM pg_constraint WHERE conrelid = 'nf_a'::regclass;

-- teardown
DROP VIEW "ZzView";
DROP TABLE nf_a;
DROP FUNCTION "ZzFn"();
DROP FUNCTION zzfn2();
