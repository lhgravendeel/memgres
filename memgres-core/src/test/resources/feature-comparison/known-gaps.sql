-- ============================================================================
-- Known gaps: what memgres does not yet answer the way PostgreSQL 18 does
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- THIS FILE IS EXPECTED TO FAIL. Every annotation below is PostgreSQL's own
-- answer, measured on the reference server, for a case that branch 3 and its
-- predecessors deliberately left open. It is a to-do list that reports itself:
-- a gap that closes turns a mem-vs-annot difference into a pass, and a gap that
-- reopens turns a pass back into a difference.
--
-- Each group names why it was left, because in every case the reason was that
-- the obvious fix would have refused SQL PostgreSQL runs.
-- ============================================================================

DROP TABLE IF EXISTS kg_u CASCADE;
DROP TABLE IF EXISTS kg_t CASCADE;
DROP DOMAIN IF EXISTS kg_yn CASCADE;
CREATE DOMAIN kg_yn AS boolean;
CREATE TABLE kg_t (id int PRIMARY KEY, v int, n int, txt text, y kg_yn);
INSERT INTO kg_t VALUES (1,1,1,'aa',true),(2,2,0,'ab',false);
CREATE TABLE kg_u (id int PRIMARY KEY, v int);
INSERT INTO kg_u VALUES (1,1);

-- ============================================================================
-- A. A condition over a column a derived relation supplies
-- ============================================================================
-- PostgreSQL types a derived table's, a CTE's and a FROM-function's columns and
-- refuses a non-boolean condition over one. memgres cannot: the binding it
-- builds for a derived relation carries an inferred type, often the wrong one,
-- and refusing on the strength of that rejected ordinary joins and sub-queries.
-- BooleanContext therefore answers "unknown" and lets these through.

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT id FROM (SELECT id, n FROM kg_t) q WHERE n;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
WITH c AS (SELECT id, n FROM kg_t) SELECT id FROM c WHERE n;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT g FROM generate_series(1,2) g WHERE g;

-- ============================================================================
-- B. A name that is no function, reported before the clause it carries
-- ============================================================================
-- PostgreSQL resolves a call before judging the clause on it, so an unknown
-- name outranks a complaint about FILTER, about OVER, or about a later column.
-- Deciding "this name is no function" without evaluating needs a complete
-- register of the names memgres can call, and BuiltinFunctionNames is not one:
-- sin, coalesce, greatest and some five hundred other engine case labels are
-- absent from it, so reading it as a register would refuse working SQL.

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function kg_nosuchfn(integer) does not exist
-- end-expected-error
SELECT kg_nosuchfn(1), abs(1) FILTER (WHERE true) FROM kg_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function kg_nosuchfn(integer) does not exist
-- end-expected-error
SELECT kg_nosuchfn(1), nosuchcol FROM kg_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function kg_nosuchfn(integer) does not exist
-- end-expected-error
SELECT kg_nosuchfn(v) OVER () FROM kg_t;

-- ============================================================================
-- C. Clause order where both faults are plain column references
-- ============================================================================
-- The ordered walk that gets this right only runs once a refusal at that query
-- level has already been found, so it can never refuse a statement that was
-- going to succeed. Running it unconditionally cost 340 test failures, because
-- the scope predicates are one-sided and unsound for FROM-function bindings.

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT abs(nosuch2) FROM kg_t ORDER BY nosuch3;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT abs(nosuch2) FROM kg_t WHERE sum(v) > 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT v FROM kg_t WHERE nosuch2 = 1 AND sum(v) > 1;

-- GROUP BY is transformed last, so HAVING's fault is the one reported
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch_c" does not exist
-- end-expected-error
SELECT v FROM kg_t GROUP BY nosuch_b HAVING nosuch_c > 1;

-- ============================================================================
-- D. Too few arguments
-- ============================================================================
-- The arity rule is one-sided on purpose -- it refuses only more arguments than
-- the longest recorded signature -- because BuiltinFunctionSignatures records
-- several names only in their long form. Reading "too few" out of that table
-- refused working SQL, so a short call reaches the implementation and crashes.

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lpad(unknown) does not exist
-- end-expected-error
SELECT lpad('a');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function split_part(unknown, unknown) does not exist
-- end-expected-error
SELECT split_part('a,b', ',');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function age() does not exist
-- end-expected-error
SELECT age();

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function random(integer) does not exist
-- end-expected-error
SELECT random(1);

-- ============================================================================
-- E. Finding #14: a schema that does not exist
-- ============================================================================
-- A qualified function name already reports 3F000. A qualified type name in a
-- cast and a qualified relation in CREATE INDEX do not.

-- this one already agrees, and is here so it stays agreed
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "kg_noschema" does not exist
-- end-expected-error
SELECT kg_noschema.abs(1);

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "kg_noschema" does not exist
-- end-expected-error
SELECT 1::kg_noschema.int4;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "kg_noschema" does not exist
-- end-expected-error
CREATE INDEX kg_ix ON kg_noschema.kg_t (id);

-- ============================================================================
-- F. A call resolved only on the path to a refusal
-- ============================================================================
-- abs('x'::text) has a certain argument type and no matching signature, but the
-- resolution check runs only where something else has already gone wrong, so
-- the value reaches the implementation and fails on input syntax instead.

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function abs(text) does not exist
-- end-expected-error
SELECT abs('x'::text);

-- ============================================================================
-- G. A stored definition never reaches the clause checks
-- ============================================================================
-- DdlTableExecutor and DdlObjectExecutor call the placement check and never
-- FilterCheck, so FILTER, DISTINCT and an aggregate ORDER BY are all accepted
-- inside a CHECK constraint, an index expression and a DEFAULT.

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE kg_g1 (i int CHECK (abs(i) FILTER (WHERE true) > 0));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: DISTINCT specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE kg_g2 (i int CHECK (abs(DISTINCT i) > 0));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE kg_g3 (i int CHECK (abs(i ORDER BY i) > 0));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
CREATE INDEX kg_g4 ON kg_t ((abs(id ORDER BY id)));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE kg_g5 (i int DEFAULT (abs(1 ORDER BY 1)));

-- ============================================================================
-- H. An aggregate ORDER BY refused by the parser before the name is resolved
-- ============================================================================
-- ExpressionParser refuses the combination at parse time, so the missing
-- signature is never reached.

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function row_number(integer) does not exist
-- end-expected-error
SELECT row_number(v ORDER BY v) OVER () FROM kg_t;

-- ============================================================================
-- I. Which complaint an out-of-scope FROM entry earns
-- ============================================================================
-- The relation is in the statement but not in this part of it, which PostgreSQL
-- words differently from a name that is nowhere. memgres makes the distinction
-- in one path only, and this failure comes from evaluation-time resolution.

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "a"
-- end-expected-error
SELECT count(*) FROM kg_t a JOIN (SELECT a.v) b ON true;

-- ============================================================================
-- J. A join condition whose type is settled only at evaluation
-- ============================================================================
-- upper is overloaded, so the "every signature returns one type" rule stays
-- silent and the runtime coercion reports the input rather than the type.

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type text
-- end-expected-error
SELECT count(*) FROM kg_t a JOIN kg_t b ON upper('a');

-- ============================================================================
-- K. A domain answers to its own name
-- ============================================================================
-- pg_typeof reads enum, composite and array element names but not a domain's,
-- so a domain-typed column reports its base type.

-- begin-expected
-- columns: t
-- row: kg_yn
-- end-expected
SELECT pg_typeof(y)::text AS t FROM kg_t LIMIT 1;

-- the same through a derived table
-- begin-expected
-- columns: t
-- row: kg_yn
-- end-expected
SELECT pg_typeof(b)::text AS t FROM (SELECT y AS b FROM kg_t) q LIMIT 1;

-- ============================================================================
-- L. Two that already agree, kept so they stay agreed
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "v" is ambiguous
-- end-expected-error
UPDATE kg_t SET v = v FROM kg_u WHERE kg_t.id = kg_u.id;

-- begin-expected
-- columns: t
-- row: boolean
-- end-expected
SELECT pg_typeof(f)::text AS t FROM (SELECT starts_with('a','a') AS f) q;

DROP TABLE IF EXISTS kg_g1 CASCADE;
DROP TABLE IF EXISTS kg_g2 CASCADE;
DROP TABLE IF EXISTS kg_g3 CASCADE;
DROP TABLE IF EXISTS kg_g5 CASCADE;
DROP TABLE IF EXISTS kg_u CASCADE;
DROP TABLE IF EXISTS kg_t CASCADE;
DROP DOMAIN IF EXISTS kg_yn CASCADE;
