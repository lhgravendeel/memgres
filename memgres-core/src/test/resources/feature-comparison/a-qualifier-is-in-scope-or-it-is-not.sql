-- ============================================================================
-- A qualifier is in scope or it is not, whatever rows the statement turns out to read.
--
-- PostgreSQL resolves every name a statement writes against the relations that statement lists, before
-- a page is read. memgres resolved some of them as each row reached the evaluator, so a statement over
-- an empty table read no row, tripped over nothing and quietly did nothing; and where the relation is
-- written inside a sub-SELECT the two refusals came out the wrong way round.
--
-- The two are worded differently on purpose: a relation the statement does not list is missing, and one
-- it does list but has renamed is there and out of reach.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
DROP TABLE IF EXISTS zz_sc0 CASCADE;
DROP TABLE IF EXISTS zz_sc1 CASCADE;
CREATE TABLE zz_sc0 (a int);
CREATE TABLE zz_sc1 (b int);

-- ============================================================================
-- A relation written only inside a sub-SELECT is not one this query lists
-- ============================================================================
-- The sub-SELECT's FROM belongs to a query of its own, so the name is missing rather than covered.
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM (SELECT a FROM zz_sc0) s WHERE zz_sc0.a = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM (SELECT a FROM zz_sc0) s, zz_sc1 WHERE zz_sc0.a = 1;
-- A parenthesised join is one FROM item of this query, so its relations are covered by its alias.
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM (zz_sc0 JOIN zz_sc1 ON zz_sc0.a = zz_sc1.b) j WHERE zz_sc0.a = 1;
-- What the item does answer to still resolves.
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT a FROM zz_sc0) s WHERE s.a = 1;
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (zz_sc0 JOIN zz_sc1 ON zz_sc0.a = zz_sc1.b) j WHERE j.a = 1;
-- A relation nothing wrote down at all is missing wherever it is written.
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM zz_sc0 WHERE zz_sc1.b = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM zz_sc0 WHERE nosuch.a = 1;

-- ============================================================================
-- An alias hides the relation's own name, and a query over no rows says so
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM zz_sc0 x WHERE zz_sc0.a = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM zz_sc0 x WHERE public.zz_sc0.a = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT zz_sc0.a FROM zz_sc0 x;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT a FROM zz_sc0 x ORDER BY zz_sc0.a;

-- ============================================================================
-- An UPDATE and a DELETE resolve their names before they read a row
-- ============================================================================
-- Without a FROM or a USING the target is the whole scope, so any other qualifier is missing.
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
UPDATE zz_sc0 SET a = 1 WHERE zz_sc1.b = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
DELETE FROM zz_sc0 WHERE zz_sc1.b = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
UPDATE zz_sc0 SET a = zz_sc1.b;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
UPDATE zz_sc0 SET a = 1 WHERE nosuch.a = 1;
-- An alias hides the target's own name here too.
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
UPDATE zz_sc0 x SET a = 1 WHERE zz_sc0.a = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
DELETE FROM zz_sc0 x WHERE zz_sc0.a = 1;
-- A name that is no column of the target is refused before the scan as it always was.
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
UPDATE zz_sc0 SET a = 1 WHERE nope = 1;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
DELETE FROM zz_sc0 WHERE nope = 1;
-- A FROM or a USING brings the other relation in, and then it is in scope.
UPDATE zz_sc0 SET a = 1 FROM zz_sc1 WHERE zz_sc1.b = 1;
DELETE FROM zz_sc0 USING zz_sc1 WHERE zz_sc1.b = 1;
-- A subquery brings its own FROM list, so the same name in one is a relation of its own.
UPDATE zz_sc0 SET a = 1 WHERE a IN (SELECT b FROM zz_sc1);
DELETE FROM zz_sc0 WHERE a IN (SELECT b FROM zz_sc1);
-- The target answers to its own name, its schema-qualified name and its alias.
UPDATE zz_sc0 SET a = 1 WHERE zz_sc0.a = 1;
UPDATE zz_sc0 SET a = 1 WHERE public.zz_sc0.a = 1;
UPDATE zz_sc0 t SET a = t.a WHERE t.a = 1;
UPDATE zz_sc0 SET a = 1 WHERE zz_sc0.ctid IS NOT NULL;

-- ============================================================================
-- A name that is no column at all is refused whether or not there is a row for it
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT count(*) FROM zz_sc0 WHERE nope = 1;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT nope FROM zz_sc0;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT a FROM zz_sc0 ORDER BY nope;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT count(*) FROM zz_sc0 HAVING nope > 0;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT a FROM zz_sc0 GROUP BY nope;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT count(*) FROM zz_sc0 x WHERE x.nope = 1;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT count(*) FROM zz_sc0 WHERE (SELECT count(*) FROM zz_sc1 WHERE nope = 1) = 0;

-- ============================================================================
-- The same statements once the table has a row in it
-- ============================================================================
INSERT INTO zz_sc0 VALUES (1);
INSERT INTO zz_sc1 VALUES (1);
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM zz_sc0 x WHERE zz_sc0.a = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
UPDATE zz_sc0 SET a = 1 WHERE zz_sc1.b = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
DELETE FROM zz_sc0 WHERE zz_sc1.b = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
UPDATE zz_sc0 x SET a = 1 WHERE zz_sc0.a = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT count(*) FROM (SELECT a FROM zz_sc0) s WHERE zz_sc0.a = 1;
-- And the ones that were always going to work still do.
UPDATE zz_sc0 SET a = a + 1 WHERE a = 1;
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT a FROM zz_sc0 ORDER BY 1;
DELETE FROM zz_sc0 WHERE a = 2;
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zz_sc0;

-- teardown
DROP TABLE IF EXISTS zz_sc0 CASCADE;
DROP TABLE IF EXISTS zz_sc1 CASCADE;
