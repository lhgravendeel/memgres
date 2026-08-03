-- ============================================================================
-- Clause order: which fault a statement wrong in two places reports
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL analyses one query level in a fixed order and that order decides
-- which of its faults is named: WITH items, the FROM clause and its join
-- conditions, the select list, WHERE, HAVING, ORDER BY, and GROUP BY last of
-- all. Inside one call the arguments are transformed before the function is
-- resolved, and the OVER specification is not read while the clause holding
-- the call is being judged.
--
-- The second half of this file is the guard: everything ordinary that stands
-- near the refusals above, and which no rule here may touch. A refusal is only
-- worth having if the SQL beside it still runs.
-- ============================================================================

DROP TABLE IF EXISTS co_u CASCADE;
DROP TABLE IF EXISTS co_t CASCADE;
CREATE TABLE co_t (id int PRIMARY KEY, v int, n int, txt text, b boolean);
INSERT INTO co_t VALUES (1,1,1,'aa',true),(2,2,0,'ab',false);
CREATE TABLE co_u (id int PRIMARY KEY, v int);
INSERT INTO co_u VALUES (1,1),(2,2);

-- ============================================================================
-- 1. The select list is read before WHERE, HAVING, ORDER BY and GROUP BY
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT abs(nosuch2) FROM co_t ORDER BY nosuch3;

-- an aggregate may not stand in WHERE, and the select list is read first anyway
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT abs(nosuch2) FROM co_t WHERE sum(v) > 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT abs(nosuch2) FROM co_t GROUP BY nosuch3;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT abs(nosuch2) FROM co_t HAVING count(nosuch3) > 0;

-- ============================================================================
-- 2. Within a clause, left to right
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT v FROM co_t WHERE nosuch2 = 1 AND sum(v) > 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch1" does not exist
-- end-expected-error
SELECT nosuch1, nosuch2 FROM co_t;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch1" does not exist
-- end-expected-error
SELECT v FROM co_t WHERE nosuch1 = 1 OR nosuch2 = 2;

-- a call's arguments are transformed before the function itself is resolved
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT co_nosuchfn(nosuch2) FROM co_t;

-- ============================================================================
-- 3. HAVING before ORDER BY, and GROUP BY last of all
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch_c" does not exist
-- end-expected-error
SELECT v FROM co_t GROUP BY nosuch_b HAVING nosuch_c > 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch_c" does not exist
-- end-expected-error
SELECT v FROM co_t GROUP BY nosuch_b ORDER BY nosuch_c;

-- an ORDER BY position is judged before an unresolvable grouping item
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 9 is not in select list
-- end-expected-error
SELECT id, v FROM co_t GROUP BY nosuch_b ORDER BY 9;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT id, v FROM co_t GROUP BY nosuch_b ORDER BY 2.5;

-- ============================================================================
-- 4. WHERE before everything WHERE stands in front of
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT v FROM co_t WHERE nosuch2 = 1 ORDER BY nosuch3;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT v FROM co_t WHERE nosuch2 = 1 GROUP BY nosuch3;

-- ============================================================================
-- 5. A clause is judged without a row to judge it on
-- ============================================================================
-- A query that reads nothing is analysed all the same: what a clause is
-- transformed against is what the relations supply, not what they hold.

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT abs(nosuchcol) FROM co_t WHERE false;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT v FROM co_t WHERE id > 1000 ORDER BY abs(nosuchcol);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT abs(nosuchcol) FROM co_t LIMIT 0;

-- ============================================================================
-- 6. The OVER specification is not read while the clause is being judged
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
SELECT v FROM co_t WHERE row_number() OVER (ORDER BY nosuchcol) = 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
SELECT v FROM co_t WHERE row_number() OVER (PARTITION BY nosuchcol) = 1;

-- the call's own arguments, though, are transformed with the clause
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT v FROM co_t WHERE lag(nosuchcol) OVER () = 1;

-- ============================================================================
-- 7. The range table is finished before any clause is read
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "x" specified more than once
-- end-expected-error
SELECT * FROM co_t x JOIN co_u x ON nosuchcol = 1;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "co_nosuchtable" does not exist
-- end-expected-error
SELECT nosuchcol FROM co_nosuchtable;

-- ============================================================================
-- 8. An out-of-scope FROM entry is worded differently from one that is nowhere
-- ============================================================================
-- The range table is built left to right, and a sub-select is transformed
-- against the entries made before it. An entry already made and out of reach
-- is "invalid reference"; one not yet made, or written nowhere, is "missing".

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "a"
-- end-expected-error
SELECT count(*) FROM co_t a JOIN (SELECT a.v) b ON true;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "a"
-- end-expected-error
SELECT count(*) FROM co_t a, (SELECT a.v) b;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "a"
-- end-expected-error
SELECT count(*) FROM co_t a CROSS JOIN (SELECT a.v) b;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "a"
-- end-expected-error
SELECT count(*) FROM co_t a LEFT JOIN (SELECT a.v) b ON true;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "a"
-- end-expected-error
SELECT count(*) FROM co_t a RIGHT JOIN (SELECT a.v) b ON true;

-- nested in a join below the entry, and nested in a sub-select below the item
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "b"
-- end-expected-error
SELECT count(*) FROM (co_t a JOIN co_u b ON true) JOIN (SELECT b.v) c ON true;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "a"
-- end-expected-error
SELECT count(*) FROM co_t a JOIN ((SELECT a.v) x JOIN co_u b ON true) ON true;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "a"
-- end-expected-error
SELECT count(*) FROM co_t a, (SELECT (SELECT a.v)) b;

-- an alias hides the relation's name, and the entry answers to either
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "co_t"
-- end-expected-error
SELECT count(*) FROM co_t a JOIN (SELECT co_t.v) b ON true;

-- ...and the entry not yet made is missing, however plainly it is written
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "a"
-- end-expected-error
SELECT count(*) FROM (SELECT a.v) b JOIN co_t a ON true;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "a"
-- end-expected-error
SELECT count(*) FROM (SELECT a.v) b, co_t a;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "z"
-- end-expected-error
SELECT count(*) FROM co_t a JOIN (SELECT z.v) b ON true;

-- a WITH item is not a FROM entry of the level that defines it
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "w"
-- end-expected-error
WITH w AS (SELECT 1 AS n) SELECT count(*) FROM co_t a JOIN (SELECT w.n) b ON true;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "a"
-- end-expected-error
SELECT a.v FROM co_t b;

-- ============================================================================
-- 9. THE GUARD: the ordinary shapes none of the above may touch
-- ============================================================================

-- LATERAL is the word that brings the entry into reach
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM co_t a JOIN LATERAL (SELECT a.v) b ON true;

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM co_t a, LATERAL (SELECT a.v) b;

-- a function in FROM is lateral whether or not it says so
-- begin-expected
-- columns: c
-- row: 3
-- end-expected
SELECT count(*) AS c FROM co_t a, generate_series(1, a.v) g;

-- a name the sub-select binds for itself is its own, not the sibling's
-- begin-expected
-- columns: c
-- row: 4
-- end-expected
SELECT count(*) AS c FROM co_t a JOIN (SELECT a.v FROM co_u a) b ON true;

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM co_t a JOIN (WITH a AS (SELECT 1 AS v) SELECT a.v FROM a) b ON true;

-- an enclosing level's entry is in scope, sub-select in FROM or not
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT (SELECT count(*) FROM (SELECT a.v) c) AS c FROM co_t a LIMIT 1;

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM co_t a JOIN co_u b ON b.v = (SELECT a.v);

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM co_t a WHERE EXISTS (SELECT 1 FROM co_u b WHERE b.v = a.v AND a.id = 1);

-- the ordinary joins and sub-selects, none of which references anything odd
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM co_t a JOIN co_u b ON a.id = b.id;

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM (SELECT v FROM co_u) b JOIN co_t a ON a.v = b.v;

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM co_t a JOIN generate_series(1,2) g ON g = a.v;

-- ORDER BY a position, a name, an alias and an expression
-- begin-expected
-- columns: v
-- row: 1
-- row: 2
-- end-expected
SELECT v FROM co_t ORDER BY 1;

-- begin-expected
-- columns: id, v
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT * FROM co_u ORDER BY 2;

-- begin-expected
-- columns: k
-- row: 2
-- row: 1
-- end-expected
SELECT v AS k FROM co_t ORDER BY k DESC;

-- begin-expected
-- columns: v
-- row: 2
-- row: 1
-- end-expected
SELECT v FROM co_t ORDER BY abs(v) DESC;

-- begin-expected
-- columns: g, c
-- row: 0, 1
-- row: 1, 1
-- end-expected
SELECT n AS g, count(*) AS c FROM co_t GROUP BY n HAVING count(*) > 0 ORDER BY 1;

-- a window call in a clause that may hold one, with an OVER that names columns
-- begin-expected
-- columns: r
-- row: 1
-- row: 2
-- end-expected
SELECT row_number() OVER (ORDER BY v) AS r FROM co_t;

-- begin-expected
-- columns: r
-- row: 1
-- row: 1
-- end-expected
SELECT row_number() OVER (PARTITION BY n ORDER BY v) AS r FROM co_t;

-- begin-expected
-- columns: l
-- row: null
-- row: 1
-- end-expected
SELECT lag(v) OVER (ORDER BY v) AS l FROM co_t;

-- a relation renamed by an alias list, and a derived one
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM co_t x(c1,c2,c3,c4,c5) WHERE c2 > 0;

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM (SELECT v FROM co_t) q(z) WHERE z > 0;

-- a call in FROM whose columns only running it settles
-- begin-expected
-- columns: key
-- row: a
-- end-expected
SELECT key FROM jsonb_each('{"a":1}');

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM jsonb_each('{"a":1}') e WHERE e.key = 'a';

-- begin-expected
-- columns: g
-- row: 1
-- row: 2
-- end-expected
SELECT g FROM generate_series(1,2) g;

-- a catalog function called over a catalog column, which is an oid
-- begin-expected
-- columns: c
-- row: t
-- end-expected
SELECT (count(*) > 0) AS c FROM pg_class WHERE pg_table_is_visible(oid) AND relname = 'co_t';

-- begin-expected
-- columns: c
-- row: t
-- end-expected
SELECT (pg_get_userbyid(relowner) IS NOT NULL) AS c FROM pg_class WHERE relname = 'co_t';

-- ...and over a plain integer, which PostgreSQL casts to oid on its own
-- begin-expected
-- columns: c
-- row: t
-- end-expected
SELECT (pg_type_is_visible(23) IS NOT NULL) AS c;

-- begin-expected
-- columns: c
-- row:
-- end-expected
SELECT pg_get_indexdef(0) AS c;

DROP TABLE IF EXISTS co_u CASCADE;
DROP TABLE IF EXISTS co_t CASCADE;
