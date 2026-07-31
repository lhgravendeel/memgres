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
-- memgres runs its placement checks over the raw syntax tree, before resolving
-- anything, so a later complaint can win where PostgreSQL reports an earlier
-- one. Where the two already agree it is pinned here so it stays agreed; the
-- cases where they still differ are recorded in the unit test beside this file
-- rather than asserted, because an annotation has to hold on both engines.
--
-- What is pinned: that a syntax error outranks every lookup; that a missing
-- relation outranks a missing column and the clause-level refusals memgres
-- already orders correctly; that each refusal fires on its own, including with
-- no rows to read; and that the ordinary shapes are untouched.
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
-- 4. Each clause-level refusal still fires on its own
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
-- 5. The refusal does not depend on there being rows to read
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

-- ============================================================================
-- 6. The ordinary shapes these rules must not touch
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

DROP TABLE IF EXISTS ep_t CASCADE;
