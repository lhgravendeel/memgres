-- ============================================================================
-- Feature Comparison: which error a statement with several faults reports
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL analyses a query in a fixed order, and the order decides which
-- complaint a reader sees when more than one thing is wrong. Raw parse comes
-- first, so a syntax error beats everything. Then the range table is built, so
-- a relation that does not exist beats every complaint about a clause. Only
-- then is the rest of the query transformed against that range table.
--
-- Within one function call the order is the same idea again: the arguments are
-- transformed, then the FILTER predicate — coerced to boolean — and only then is
-- the function resolved. A complaint that the call is not an aggregate is
-- therefore the last thing said about it, not the first.
--
-- memgres now judges a query level's clauses after its FROM clause has been
-- resolved, and resolves what a call names before refusing the call. The cases
-- where the two engines still differ are recorded in the unit test beside this
-- file rather than asserted, because an annotation has to hold on both engines.
--
-- What is pinned: that a syntax error outranks every lookup; that a missing
-- relation outranks a missing column and every clause-level refusal; that
-- inside a call an unresolvable argument, a non-boolean FILTER and a function
-- that does not exist each outrank the FILTER/DISTINCT/OVER complaint; that
-- each refusal fires on its own, including with no rows to read; and that the
-- ordinary shapes are untouched.
-- ============================================================================

DROP TABLE IF EXISTS ep_t CASCADE;
CREATE TABLE ep_t (id int PRIMARY KEY, v int, txt text, b boolean);
INSERT INTO ep_t VALUES (1,10,'a',true),(2,20,'b',false);

-- ============================================================================
-- 1. A syntax error is raised before anything is looked up
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SELECT abs(v) FILTER (WHERE b) FROM ep_t WHERE;

-- ============================================================================
-- 2. A missing relation outranks a clause-level refusal
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ep_nosuch" does not exist
-- end-expected-error
SELECT abs(id) OVER () FROM ep_nosuch;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ep_nosuch" does not exist
-- end-expected-error
SELECT abs(1) WITHIN GROUP (ORDER BY v) FROM ep_nosuch;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ep_nosuch" does not exist
-- end-expected-error
SELECT id FROM ep_nosuch WHERE count(*) > 0;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ep_nosuch" does not exist
-- end-expected-error
SELECT v, count(*) FROM ep_nosuch GROUP BY id;






-- Only the NAMES of the relations are resolved before the clauses are judged,
-- never their rows: reading a FROM item is observable, and the statement may yet
-- be refused.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ep_nosuch" does not exist
-- end-expected-error
SELECT abs(id) FILTER (WHERE true) FROM ep_nosuch;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ep_nosuch" does not exist
-- end-expected-error
SELECT abs(DISTINCT id) FROM ep_nosuch;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ep_nosuch" does not exist
-- end-expected-error
SELECT id FROM ep_nosuch WHERE generate_series(1,2) > 0;

-- the range table covers the whole statement, sub-queries included
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ep_nosuch" does not exist
-- end-expected-error
SELECT * FROM ep_nosuch x WHERE EXISTS (SELECT abs(1) FILTER (WHERE true));

-- ============================================================================
-- A refused statement performs none of the writes its WITH items describe
-- ============================================================================
DROP TABLE IF EXISTS ep_sink CASCADE;
CREATE TABLE ep_sink (id int PRIMARY KEY);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
WITH ins AS (INSERT INTO ep_sink VALUES (1) RETURNING id)
SELECT abs(1) FILTER (WHERE true) FROM ins;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM ep_sink;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ep_nosuch" does not exist
-- end-expected-error
WITH ins AS (INSERT INTO ep_sink VALUES (2) RETURNING id)
SELECT id FROM ep_nosuch;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM ep_sink;

DROP TABLE IF EXISTS ep_sink CASCADE;
-- ============================================================================
-- 3. A missing relation outranks a missing column, and the first one named wins
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ep_nosuch" does not exist
-- end-expected-error
SELECT nosuchcol FROM ep_nosuch;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ep_nosuch_a" does not exist
-- end-expected-error
SELECT 1 FROM ep_nosuch_a, ep_nosuch_b;

-- among faults of one stage, the select list is transformed before WHERE
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch_a" does not exist
-- end-expected-error
SELECT nosuch_a FROM ep_t WHERE nosuch_b > 0;

-- and WHERE before ORDER BY
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch_b" does not exist
-- end-expected-error
SELECT id FROM ep_t WHERE nosuch_b > 0 ORDER BY nosuch_c;

-- ============================================================================
-- 4. Inside one call: the arguments, then the FILTER, then the function itself
-- ============================================================================
-- an argument naming a column that is not there is reported before the call is
-- judged to be the wrong kind of function
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT abs(nosuchcol) FILTER (WHERE true) FROM ep_t;

-- and so is one in the FILTER predicate, which is transformed next
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT abs(id) FILTER (WHERE nosuchcol) FROM ep_t;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT abs(nosuchcol) OVER () FROM ep_t;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT abs(DISTINCT nosuchcol) FROM ep_t;

-- an aggregate's arguments too, before the clause it may not stand in
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT id FROM ep_t WHERE count(nosuchcol) > 0;

-- a qualified argument is named in full
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column x.nosuchcol does not exist
-- end-expected-error
SELECT abs(x.nosuchcol) FILTER (WHERE true) FROM ep_t x;

-- FILTER takes a condition, and the coercion to boolean happens before the
-- function is resolved
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of FILTER must be type boolean, not type integer
-- end-expected-error
SELECT abs(id) FILTER (WHERE 1) FROM ep_t;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of FILTER must be type boolean, not type text
-- end-expected-error
SELECT abs(v) FILTER (WHERE txt) FROM ep_t;

-- a quoted name keeps its case, so "ABS" is not abs and resolves to nothing
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function ABS(integer) does not exist
-- end-expected-error
SELECT "ABS"(1) FILTER (WHERE true);

-- a function is resolved by its argument types as well as its name
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function abs(text) does not exist
-- end-expected-error
SELECT abs(txt) FILTER (WHERE b) FROM ep_t;

-- and a qualifier has to name the schema the function is really in
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function information_schema.abs(integer) does not exist
-- end-expected-error
SELECT information_schema.abs(v) FILTER (WHERE true) FROM ep_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function information_schema.abs(integer) does not exist
-- end-expected-error
SELECT information_schema.abs(-1);

-- ============================================================================
-- 5. Each clause-level refusal still fires on its own
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v) FILTER (WHERE b) FROM ep_t;

-- DISTINCT inside a call means what FILTER does, and is refused in the same words
-- begin-expected-error
-- sqlstate: 42809
-- message-like: DISTINCT specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(DISTINCT v) FROM ep_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: OVER specified, but abs is not a window function nor an aggregate function
-- end-expected-error
SELECT abs(v) OVER () FROM ep_t;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
SELECT id FROM ep_t WHERE count(*) > 0;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in WHERE
-- end-expected-error
SELECT id FROM ep_t WHERE generate_series(1,2) > 0;

-- a name that is no function at all is a missing function first
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function ep_nosuchfn(integer) does not exist
-- end-expected-error
SELECT ep_nosuchfn(id) FILTER (WHERE true) FROM ep_t;

-- ============================================================================
-- 6. The refusal does not depend on there being rows to read
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v) FILTER (WHERE b) FROM ep_t LIMIT 0;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v) FILTER (WHERE b) FROM ep_t WHERE false;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v) FILTER (WHERE b) FROM ep_t WHERE id = -1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
WITH c AS (SELECT abs(1) FILTER (WHERE true)) SELECT * FROM c;

-- a WITH item nothing reads is analysed all the same
-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
WITH c AS (SELECT abs(1) FILTER (WHERE true)) SELECT 1;

-- a derived table supplies columns like any other relation, so the call is
-- still what is refused
-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(s.v) FILTER (WHERE s.b) FROM (SELECT * FROM ep_t) s;

-- a FROM-function's column is not knowable from the catalog, and a scope this
-- cannot read is a scope it does not judge
-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(g) FILTER (WHERE true) FROM generate_series(1,3) g;

-- ============================================================================
-- 7. The ordinary shapes these rules must not touch
-- ============================================================================
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) FILTER (WHERE b)::text AS n FROM ep_t;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(DISTINCT v)::text AS n FROM ep_t;

-- begin-expected
-- columns: n
-- row: 1
-- row: 1
-- end-expected
SELECT (count(*) FILTER (WHERE b) OVER ())::text AS n FROM ep_t;

-- a CTE name is not a stored relation and must not be looked for as one
-- begin-expected
-- columns: x
-- row: 1
-- end-expected
WITH ep_cte AS (SELECT 1 AS x) SELECT x::text AS x FROM ep_cte;

-- nor is a derived table's alias
-- begin-expected
-- columns: x
-- row: 10
-- end-expected
SELECT s.x::text AS x FROM (SELECT v AS x FROM ep_t WHERE id = 1) s;

-- a catalog relation is a resolvable name
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM pg_class WHERE relname = 'ep_t';

-- a qualifier that does name the function's own schema still resolves
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT pg_catalog.abs(-1)::text AS n;

-- and a quoted name that is already folded is the same name
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT "abs"(-1)::text AS n;

DROP TABLE IF EXISTS ep_t CASCADE;
