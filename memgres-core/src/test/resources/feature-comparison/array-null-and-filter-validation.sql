-- Two things were reported together: that the array containment operators must reject a NULL
-- element, and that FILTER must be refused on a call that is not an aggregate. Only the second
-- turned out to be PostgreSQL.
--
-- WHAT FILTER MEANS. FILTER says which rows a call accumulates, so it means something only to a
-- call that accumulates rows. Written on an ordinary function it is not a clause that can be
-- ignored but a call that cannot be built, and PostgreSQL refuses it naming the function:
-- 42809 "FILTER specified, but abs is not an aggregate function". memgres already said exactly
-- that, but only for the set-returning functions, only in a select list, and only for an
-- unqualified name; every other call carrying a FILTER was accepted and the FILTER thrown away,
-- so "abs(v) FILTER (WHERE b = 1)" quietly answered as if the FILTER had never been written.
--
-- The refusal is a property of the call, not of the clause it stands in, so it holds in a select
-- list, in WHERE, in HAVING, in ORDER BY, inside a CTE or a derived table, inside an aggregate's
-- own argument list, in the SET list of an UPDATE and in a VALUES row of an INSERT. It holds
-- with no rows to read at all, under LIMIT 0, and under a WHERE that can never be true. It is
-- decided before the OVER complaint, before the grouping complaint and before the set-returning
-- placement complaint, all three of which memgres reached first.
--
-- The name in the message is the name as parsed: ABS and AbS become abs because the lexer folds
-- an unquoted identifier, "Mixed" keeps its case, and a schema qualifier is kept and joined with
-- a dot -- pg_catalog.abs, not abs. memgres stripped the qualifier and reported the wrong name
-- for pg_catalog.generate_series.
--
-- Three calls are deliberately left alone: an aggregate, which is what FILTER is for; a window
-- function, which has its own two answers (0A000 with OVER, "requires an OVER clause" without);
-- and a name that is no function at all, which is a missing function first (42883), because
-- PostgreSQL resolves the call before it judges the FILTER.
--
-- WHAT THE ARRAY OPERATORS DO. The reported 22004 "array must not contain nulls" is not
-- PostgreSQL's. It comes from the intarray contrib extension, which redeclares @>, <@ and &&
-- over integer[] (_int_contains / _int_contained / _int_overlap in _int_op.c) and rejects a NULL
-- element in either operand. The corpus installs intarray and never drops it, so the shared
-- server answers 22004 where a PostgreSQL without it answers a plain t or f. Measured three
-- ways: a freshly created database with no extensions answered f/t/f/f/t exactly as memgres
-- does and flipped to 22004 the moment CREATE EXTENSION intarray ran in it; OPERATOR(pg_catalog.@>)
-- on the corpus server bypasses intarray and gives the plain answers; and pg_operator on the
-- corpus server shows the three integer[] operators pointing at intarray's C functions. Nothing
-- was changed for it -- an unconditional null rule would be wrong for every element type except
-- integer[], wrong for integer[] on any server without intarray, and would refuse the whole of
-- section D below. The operators are therefore asserted here over the element types intarray
-- never touches, which is where both servers agree.
--
-- What the same reading did turn up is four real gaps, all of which hold on any PostgreSQL:
--
--  1. arraycontains, arraycontained and arrayoverlap -- the function spellings of the three
--     operators, which no extension shadows -- were listed in pg_proc but not implemented, so
--     calling one was 42883 for a function the catalog said existed.
--  2. array_positions, array_remove and array_replace answered {} for a NULL array where they
--     are strict and answer NULL.
--  3. array_position, array_positions and array_remove walked a multidimensional array instead
--     of refusing it with 0A000.
--  4. array_replace rewrote only the top level, so it left a two-dimensional array untouched.
--
-- Every statement that returns more than one row sorts them.

-- @skipon
DROP TABLE IF EXISTS anf_t CASCADE;
DROP TABLE IF EXISTS anf_empty CASCADE;
DROP TABLE IF EXISTS anf_a CASCADE;
DROP FUNCTION IF EXISTS anf_double(int) CASCADE;
-- @skipoff

CREATE TABLE anf_t (id int PRIMARY KEY, v int, b int);
INSERT INTO anf_t VALUES (1,10,1),(2,20,2);
CREATE TABLE anf_empty (id int PRIMARY KEY, v int, b int);
CREATE FUNCTION anf_double(int) RETURNS int AS 'SELECT $1 * 2' LANGUAGE sql;

-- ============================================================================
-- SECTION A: FILTER on a call that does not accumulate rows
-- ============================================================================

-- 1: a plain function over a column

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v) FILTER (WHERE b = 1) FROM anf_t;

-- 2: the name is the function that was written, whichever function it is

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but length is not an aggregate function
-- end-expected-error
SELECT length(v::text) FILTER (WHERE b = 1) FROM anf_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but upper is not an aggregate function
-- end-expected-error
SELECT upper('x') FILTER (WHERE b = 1) FROM anf_t;

-- 3: no table is needed -- the refusal is a property of the call, not of grouping

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(1) FILTER (WHERE true);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but now is not an aggregate function
-- end-expected-error
SELECT now() FILTER (WHERE true);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but random is not an aggregate function
-- end-expected-error
SELECT random() FILTER (WHERE true);

-- 4: an unquoted name is folded to lower case in the message

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT ABS(v) FILTER (WHERE b = 1) FROM anf_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT "abs"(v) FILTER (WHERE b = 1) FROM anf_t;

-- 5: a schema qualifier is kept, and joined with a dot

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but pg_catalog.abs is not an aggregate function
-- end-expected-error
SELECT pg_catalog.abs(v) FILTER (WHERE b = 1) FROM anf_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but pg_catalog.abs is not an aggregate function
-- end-expected-error
SELECT pg_catalog . abs (v) FILTER (WHERE b = 1) FROM anf_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but pg_catalog.generate_series is not an aggregate function
-- end-expected-error
SELECT pg_catalog.generate_series(1,2) FILTER (WHERE true);

-- 6: a set-returning function is refused under the same message

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but generate_series is not an aggregate function
-- end-expected-error
SELECT generate_series(1,2) FILTER (WHERE true);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but unnest is not an aggregate function
-- end-expected-error
SELECT unnest(ARRAY[1,2]) FILTER (WHERE true);

-- 7: a function the user declared is refused exactly like a built-in

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but anf_double is not an aggregate function
-- end-expected-error
SELECT anf_double(v) FILTER (WHERE b = 1) FROM anf_t;

-- 8: the FILTER complaint is decided before the OVER complaint

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v) FILTER (WHERE b = 1) OVER () FROM anf_t;

-- 9: ... before the grouping complaint

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(count(*)) FILTER (WHERE b = 1) FROM anf_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v) FILTER (WHERE b = 1) FROM anf_t GROUP BY v;

-- 10: ... and before the set-returning placement complaint

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but generate_series is not an aggregate function
-- end-expected-error
SELECT id FROM anf_t WHERE generate_series(1,2) FILTER (WHERE true) > 0;

-- 11: every clause an expression may stand in

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT id FROM anf_t WHERE abs(v) FILTER (WHERE b = 1) > 0;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT id FROM anf_t ORDER BY abs(v) FILTER (WHERE b = 1);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT b FROM anf_t GROUP BY b HAVING abs(b) FILTER (WHERE b = 1) > 0;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
WITH c AS (SELECT abs(v) FILTER (WHERE b = 1) AS x FROM anf_t) SELECT x FROM c;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT x FROM (SELECT abs(v) FILTER (WHERE b = 1) AS x FROM anf_t) q;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT id FROM anf_t WHERE id IN (SELECT abs(v) FILTER (WHERE b = 1) FROM anf_t);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT count(*) FILTER (WHERE b = 1) AS c FROM anf_t
UNION ALL
SELECT abs(v) FILTER (WHERE b = 1) AS c FROM anf_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT CASE WHEN false THEN abs(1) FILTER (WHERE true) ELSE 1 END;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT 'a' AS c WHERE false AND (abs(1) FILTER (WHERE true)) > 0;

-- 12: the data-modifying statements

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
UPDATE anf_t SET v = abs(v) FILTER (WHERE b = 1) WHERE id = 1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
DELETE FROM anf_t WHERE abs(v) FILTER (WHERE b = 1) > 100;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
INSERT INTO anf_t SELECT 3, abs(v) FILTER (WHERE b = 1), 3 FROM anf_t LIMIT 1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
INSERT INTO anf_t VALUES (9, abs(1) FILTER (WHERE true), 9);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
MERGE INTO anf_t t USING (SELECT 1 AS k) s ON t.id = s.k
  WHEN MATCHED THEN UPDATE SET v = abs(v) FILTER (WHERE b = 1);

-- 13: a plain call nested inside an aggregate's argument is still refused

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT sum(abs(v) FILTER (WHERE b = 1)) FROM anf_t;

-- 14: the predicate's value is irrelevant, and so is having any rows to read

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v) FILTER (WHERE false) FROM anf_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v) FILTER (WHERE b = 1) FROM anf_empty;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
SELECT abs(v) FILTER (WHERE b = 1) FROM anf_t LIMIT 0;

-- 15: what is inside the FILTER is judged first

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in FILTER
-- end-expected-error
SELECT abs(v) FILTER (WHERE count(*) > 0) FROM anf_t;

-- 16: an unrecognised name is a missing function before it is a misused FILTER

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function anf_nosuch(integer) does not exist
-- end-expected-error
SELECT anf_nosuch(v) FILTER (WHERE b = 1) FROM anf_t;

-- 17: a window function keeps its own two answers

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FILTER is not implemented for non-aggregate window functions
-- end-expected-error
SELECT row_number() FILTER (WHERE b = 1) OVER () FROM anf_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
SELECT row_number() FILTER (WHERE b = 1) FROM anf_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function rank requires an OVER clause
-- end-expected-error
SELECT rank() FILTER (WHERE b = 1) FROM anf_t;

-- 18: FILTER after something that is not a call at all is a syntax error

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FILTER"
-- end-expected-error
SELECT v FILTER (WHERE b = 1) FROM anf_t;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FILTER"
-- end-expected-error
SELECT coalesce(v,0) FILTER (WHERE b = 1) FROM anf_t;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FILTER"
-- end-expected-error
SELECT CAST(v AS text) FILTER (WHERE b = 1) FROM anf_t;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FILTER"
-- end-expected-error
SELECT (abs(v)) FILTER (WHERE b = 1) FROM anf_t;

-- ============================================================================
-- SECTION B: what a FILTER is for, which must keep answering
-- ============================================================================

-- 19: the plain aggregate shapes

-- begin-expected
-- columns: a | b | c
-- row: 1 | 1 | 10
-- end-expected
SELECT count(*) FILTER (WHERE b = 1) AS a,
       count(v) FILTER (WHERE b = 1) AS b,
       sum(v) FILTER (WHERE b = 1) AS c
FROM anf_t;

-- begin-expected
-- columns: a | b | c
-- row: 10.0000000000000000 | 10 | 20
-- end-expected
SELECT avg(v) FILTER (WHERE b = 1) AS a,
       min(v) FILTER (WHERE b = 1) AS b,
       max(v) FILTER (WHERE b = 2) AS c
FROM anf_t;

-- 20: DISTINCT, an intra-aggregate ORDER BY and a two-argument aggregate

-- begin-expected
-- columns: a | b | c
-- row: 1 | 10 | 10
-- end-expected
SELECT count(DISTINCT v) FILTER (WHERE b = 1) AS a,
       string_agg(v::text, ',') FILTER (WHERE b = 1) AS b,
       string_agg(DISTINCT v::text, ',' ORDER BY v::text) FILTER (WHERE b = 1) AS c
FROM anf_t;

-- begin-expected
-- columns: a | b | c
-- row: {10} | [10] | [10]
-- end-expected
SELECT array_agg(v ORDER BY v DESC) FILTER (WHERE b = 1) AS a,
       json_agg(v) FILTER (WHERE b = 1) AS b,
       jsonb_agg(v) FILTER (WHERE b = 1) AS c
FROM anf_t;

-- begin-expected
-- columns: a | b | c
-- row: t | t | 10
-- end-expected
SELECT bool_and(v > 5) FILTER (WHERE b = 1) AS a,
       every(v > 0) FILTER (WHERE b = 1) AS b,
       bit_and(v) FILTER (WHERE b = 1) AS c
FROM anf_t;

-- 21: FILTER under GROUP BY, HAVING, ORDER BY and GROUPING SETS

-- begin-expected
-- columns: b | n
-- row: 1 | 1
-- row: 2 | 1
-- end-expected
SELECT b, count(*) FILTER (WHERE v > 5) AS n FROM anf_t GROUP BY b ORDER BY b;

-- begin-expected
-- columns: b
-- row: 1
-- row: 2
-- end-expected
SELECT b FROM anf_t GROUP BY b HAVING count(*) FILTER (WHERE v > 5) > 0 ORDER BY b;

-- begin-expected
-- columns: b
-- row: 1
-- row: 2
-- end-expected
SELECT b FROM anf_t GROUP BY b ORDER BY count(*) FILTER (WHERE v > 5), b;

-- begin-expected
-- columns: b | n
-- row: 1 | 1
-- row: 2 | 1
-- row: NULL | 2
-- end-expected
SELECT b, count(*) FILTER (WHERE v > 5) AS n
FROM anf_t GROUP BY GROUPING SETS ((b), ()) ORDER BY b;

-- 22: the empty grouping set, a scalar sub-query, and two filtered aggregates in one expression

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*) FILTER (WHERE b = 1) AS a FROM anf_t GROUP BY ();

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT (SELECT count(*) FILTER (WHERE b = 1) FROM anf_t) AS c;

-- begin-expected
-- columns: s
-- row: 2
-- end-expected
SELECT count(*) FILTER (WHERE b = 1) + count(*) FILTER (WHERE b = 2) AS s FROM anf_t;

-- 23: a compound predicate, a sub-query in the predicate, and -- the one that matters -- a plain
-- function INSIDE the predicate, which the rule must never look at

-- begin-expected
-- columns: a | b | c
-- row: 1 | 2 | 2
-- end-expected
SELECT count(*) FILTER (WHERE b = 1 AND v > 5) AS a,
       count(*) FILTER (WHERE b IN (1,2)) AS b,
       count(*) FILTER (WHERE EXISTS (SELECT 1)) AS c
FROM anf_t;

-- begin-expected
-- columns: a | b
-- row: 10 | 10
-- end-expected
SELECT sum(v) FILTER (WHERE abs(b) = 1) AS a,
       sum(v) FILTER (WHERE length(b::text) = 1 AND b = 1) AS b
FROM anf_t;

-- 24: an aggregate used as a window function keeps its FILTER

-- begin-expected
-- columns: a
-- row: 1
-- row: 1
-- end-expected
SELECT count(*) FILTER (WHERE b = 1) OVER () AS a FROM anf_t;

-- begin-expected
-- columns: a
-- row: 10
-- row: 10
-- end-expected
SELECT sum(v) FILTER (WHERE b = 1) OVER (ORDER BY id) AS a FROM anf_t;

-- begin-expected
-- columns: a
-- row: 1
-- row: 0
-- end-expected
SELECT count(*) FILTER (WHERE b = 1) OVER (PARTITION BY b) AS a FROM anf_t ORDER BY id;

-- begin-expected
-- columns: a
-- row: 10
-- row: 10
-- end-expected
SELECT sum(v) FILTER (WHERE b = 1)
       OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a
FROM anf_t;

-- begin-expected
-- columns: a
-- row: 1
-- row: 1
-- end-expected
SELECT count(*) FILTER (WHERE b = 1) OVER w AS a FROM anf_t WINDOW w AS ();

-- begin-expected
-- columns: a | b
-- row: 15.0000000000000000 | {10}
-- row: 15.0000000000000000 | {10}
-- end-expected
SELECT avg(v) FILTER (WHERE b IS NOT NULL) OVER () AS a,
       array_agg(v) FILTER (WHERE b = 1) OVER () AS b
FROM anf_t;

-- 25: a schema-qualified aggregate is an aggregate -- the qualifier is resolved away first

-- begin-expected
-- columns: a | b
-- row: 1 | 10
-- end-expected
SELECT pg_catalog.count(*) FILTER (WHERE b = 1) AS a,
       pg_catalog.sum(v) FILTER (WHERE b = 1) AS b
FROM anf_t;

-- 26: a plain function without a FILTER is untouched

-- begin-expected
-- columns: a | b
-- row: 10 | 20
-- row: 20 | 40
-- end-expected
SELECT abs(v) AS a, anf_double(v) AS b FROM anf_t ORDER BY v;

-- ============================================================================
-- SECTION C: the array functions that were read while checking the null rule
-- ============================================================================

-- 27: the function spellings of @>, <@ and && -- which no extension shadows -- exist, and carry
-- the built-in semantics where a NULL element simply matches nothing

-- begin-expected
-- columns: a | b | c
-- row: t | f | t
-- end-expected
SELECT arraycontains(ARRAY[1,NULL]::int[], ARRAY[1]::int[]) AS a,
       arraycontained(ARRAY[1,NULL]::int[], ARRAY[1,2]::int[]) AS b,
       arrayoverlap(ARRAY[1,NULL]::int[], ARRAY[1]::int[]) AS c;

-- begin-expected
-- columns: a | b | c
-- row: f | t | f
-- end-expected
SELECT arrayoverlap(ARRAY[NULL]::int[], ARRAY[NULL]::int[]) AS a,
       arraycontains(ARRAY['a',NULL]::text[], ARRAY['a']::text[]) AS b,
       arraycontains(ARRAY[1,2]::int[], ARRAY[3]::int[]) AS c;

-- begin-expected
-- columns: a | b | c
-- row: t | f | t
-- end-expected
SELECT arraycontained(ARRAY[1]::int[], ARRAY[1,2]::int[]) AS a,
       arrayoverlap(ARRAY[]::int[], ARRAY[]::int[]) AS b,
       arraycontains(ARRAY[]::int[], ARRAY[]::int[]) AS c;

-- 28: they are strict, and they answer in boolean

-- begin-expected
-- columns: a | b | c
-- row: t | t | boolean
-- end-expected
SELECT (arraycontains(NULL::int[], ARRAY[1]::int[])) IS NULL AS a,
       (arraycontains(ARRAY[1]::int[], NULL::int[])) IS NULL AS b,
       pg_typeof(arraycontains(ARRAY[1]::int[], ARRAY[1]::int[]))::text AS c;

-- 29: array_position and array_positions have no null rule at all -- searching FOR a null finds
-- the null, because they compare with IS NOT DISTINCT FROM

-- begin-expected
-- columns: a | b | c
-- row: 3 | 2 | {1,3}
-- end-expected
SELECT array_position(ARRAY[1,NULL,2]::int[], 2) AS a,
       array_position(ARRAY[1,NULL,2]::int[], NULL::int) AS b,
       array_positions(ARRAY[1,NULL,1]::int[], 1) AS c;

-- begin-expected
-- columns: a | b | c
-- row: {2} | {1,2} | 2
-- end-expected
SELECT array_positions(ARRAY[1,NULL,1]::int[], NULL::int) AS a,
       array_positions(ARRAY[NULL,NULL]::int[], NULL::int) AS b,
       array_position(ARRAY['a',NULL,'b']::text[], NULL::text) AS c;

-- 30: a NULL array is not an empty array: these three are strict in it

-- begin-expected
-- columns: a | b | c
-- row: NULL | NULL | NULL
-- end-expected
SELECT array_positions(NULL::int[], 1) AS a,
       array_remove(NULL::int[], 1) AS b,
       array_replace(NULL::int[], 1, 2) AS c;

-- begin-expected
-- columns: a | b | c
-- row: {} | {} | NULL
-- end-expected
SELECT array_positions(ARRAY[]::int[], 1) AS a,
       array_remove(ARRAY[]::int[], 1) AS b,
       array_position(ARRAY[]::int[], 1) AS c;

-- 31: removing and replacing a null is allowed, and so is replacing with one

-- begin-expected
-- columns: a | b | c
-- row: {NULL,2} | {1,2} | {a}
-- end-expected
SELECT array_remove(ARRAY[1,NULL,2]::int[], 1) AS a,
       array_remove(ARRAY[1,NULL,2]::int[], NULL::int) AS b,
       array_remove(ARRAY['a',NULL]::text[], NULL::text) AS c;

-- begin-expected
-- columns: a | b | c
-- row: {9,NULL,2} | {1,9,2} | {NULL,NULL,2}
-- end-expected
SELECT array_replace(ARRAY[1,NULL,2]::int[], 1, 9) AS a,
       array_replace(ARRAY[1,NULL,2]::int[], NULL::int, 9) AS b,
       array_replace(ARRAY[1,NULL,2]::int[], 1, NULL::int) AS c;

-- 34: a null element on the left is ignored; one on the right matches nothing

-- begin-expected
-- columns: a | b | c
-- row: t | f | f
-- end-expected
SELECT ARRAY['a',NULL]::text[] @> ARRAY['a']::text[] AS a,
       ARRAY['a']::text[] @> ARRAY['a',NULL]::text[] AS b,
       ARRAY['a',NULL]::text[] @> ARRAY[NULL]::text[] AS c;

-- 35: NULL does not contain NULL, and an array with one is not even contained in itself

-- begin-expected
-- columns: a | b | c
-- row: f | f | f
-- end-expected
SELECT ARRAY[NULL]::text[] @> ARRAY[NULL]::text[] AS a,
       ARRAY[NULL]::text[] <@ ARRAY[NULL]::text[] AS b,
       ARRAY['a',NULL]::text[] <@ ARRAY['a',NULL]::text[] AS c;

-- 36: overlap is the same rule read the other way

-- begin-expected
-- columns: a | b | c
-- row: t | f | t
-- end-expected
SELECT ARRAY['a',NULL]::text[] && ARRAY['a']::text[] AS a,
       ARRAY[NULL]::text[] && ARRAY[NULL]::text[] AS b,
       ARRAY['a',NULL]::text[] && ARRAY[NULL,'a']::text[] AS c;

-- 37: an empty array on the other side is answered, not refused

-- begin-expected
-- columns: a | b | c
-- row: f | t | f
-- end-expected
SELECT ARRAY[]::text[] @> ARRAY[NULL]::text[] AS a,
       ARRAY[NULL]::text[] @> ARRAY[]::text[] AS b,
       ARRAY[NULL]::text[] && ARRAY[]::text[] AS c;

-- 38: a NULL array VALUE is not an array containing a null -- the operator is strict, so the
-- operand that does hold a null is never even looked at

-- begin-expected
-- columns: a | b | c
-- row: t | t | t
-- end-expected
SELECT (NULL::text[] @> ARRAY[NULL]::text[]) IS NULL AS a,
       (ARRAY[NULL]::text[] @> NULL::text[]) IS NULL AS b,
       (NULL::text[] && NULL::text[]) IS NULL AS c;

-- 39: every other element type reads the same way

-- begin-expected
-- columns: a | b | c
-- row: t | f | t
-- end-expected
SELECT ARRAY[1,NULL]::bigint[] @> ARRAY[1]::bigint[] AS a,
       ARRAY[NULL]::bigint[] @> ARRAY[1]::bigint[] AS b,
       ARRAY[1,NULL]::numeric[] @> ARRAY[1]::numeric[] AS c;

-- begin-expected
-- columns: a | b | c
-- row: t | t | t
-- end-expected
SELECT ARRAY['2020-01-01',NULL]::date[] @> ARRAY['2020-01-01']::date[] AS a,
       ARRAY[true,NULL]::boolean[] @> ARRAY[true]::boolean[] AS b,
       ARRAY['a',NULL]::varchar[] && ARRAY['a']::varchar[] AS c;

-- 40: a null-free integer array is unaffected, and so is an empty one

-- begin-expected
-- columns: a | b | c
-- row: t | t | t
-- end-expected
SELECT ARRAY[1,2,3] @> ARRAY[2,1] AS a,
       ARRAY[1,1] <@ ARRAY[1] AS b,
       ARRAY[1,2] && ARRAY[2] AS c;

-- begin-expected
-- columns: a | b | c
-- row: t | f | t
-- end-expected
SELECT ARRAY[]::int[] @> ARRAY[]::int[] AS a,
       ARRAY[]::int[] && ARRAY[1]::int[] AS b,
       ARRAY[1]::int[] @> ARRAY[]::int[] AS c;

-- 41: a multidimensional array is flattened for the operators, not refused

-- begin-expected
-- columns: a | b | c
-- row: t | t | t
-- end-expected
SELECT ARRAY[[1,2],[3,4]]::text[] @> ARRAY['1']::text[] AS a,
       ARRAY[[1,2],[3,4]]::text[] && ARRAY['3']::text[] AS b,
       ARRAY[[1,2],[3,4]]::text[] <@ ARRAY['1','2','3','4']::text[] AS c;

-- 42: nothing about a null element leaks into equality, length or concatenation

-- begin-expected
-- columns: a | b | c
-- row: t | t | 2
-- end-expected
SELECT ARRAY[1,NULL]::int[] = ARRAY[1,NULL]::int[] AS a,
       ARRAY[1,NULL]::int[] IS NOT NULL AS b,
       array_length(ARRAY[1,NULL]::int[],1) AS c;

-- begin-expected
-- columns: a | b | c
-- row: {1,NULL,3} | {1,NULL,3} | 1,X
-- end-expected
SELECT ARRAY[1,NULL]::int[] || ARRAY[3]::int[] AS a,
       array_cat(ARRAY[1,NULL]::int[], ARRAY[3]::int[]) AS b,
       array_to_string(ARRAY[1,NULL]::int[], ',', 'X') AS c;

-- begin-expected
-- columns: x
-- row: 1
-- row: NULL
-- end-expected
SELECT x FROM unnest(ARRAY[1,NULL]::int[]) AS x ORDER BY x NULLS LAST;

-- 43: jsonb containment matches a JSON null, which is the opposite rule and stays that way

-- begin-expected
-- columns: a | b | c
-- row: t | t | t
-- end-expected
SELECT '[null]'::jsonb @> '[null]'::jsonb AS a,
       '{"a":null}'::jsonb @> '{"a":null}'::jsonb AS b,
       '[null,1]'::jsonb @> '[null]'::jsonb AS c;

-- 44: @>, <@ and && over a range, a network address or a shape are other operators entirely

-- begin-expected
-- columns: a | b | c
-- row: t | t | t
-- end-expected
SELECT int4range(1,5) @> int4range(2,3) AS a,
       int4range(1,5) && int4range(4,9) AS b,
       int4range(2,3) <@ int4range(1,5) AS c;

-- begin-expected
-- columns: a | b | c
-- row: t | t | t
-- end-expected
SELECT inet '192.168.1.0/24' && inet '192.168.1.5/32' AS a,
       box '((0,0),(2,2))' @> point '(1,1)' AS b,
       circle '<(0,0),5>' && circle '<(1,1),1>' AS c;

-- 45: a column holding a null element answers per row rather than aborting the scan

CREATE TABLE anf_a (id int PRIMARY KEY, ta text[], ba bigint[]);
INSERT INTO anf_a VALUES (1,'{a,b}','{1,2}'),(2,'{b,NULL}','{3,NULL}'),(3,'{}','{}'),(4,NULL,NULL);

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM anf_a WHERE ta @> ARRAY['b'] ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- row: 3
-- end-expected
SELECT id FROM anf_a WHERE ta <@ ARRAY['a','b'] ORDER BY id;

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
SELECT id FROM anf_a WHERE ba && ARRAY[3]::bigint[] ORDER BY id;

-- begin-expected
-- columns: id | c
-- row: 1 | t
-- row: 2 | t
-- row: 3 | f
-- row: 4 | NULL
-- end-expected
SELECT id, ta @> ARRAY['b'] AS c FROM anf_a ORDER BY id;

-- @skipon
DROP TABLE IF EXISTS anf_a CASCADE;
DROP FUNCTION IF EXISTS anf_double(int) CASCADE;
DROP TABLE IF EXISTS anf_empty CASCADE;
DROP TABLE IF EXISTS anf_t CASCADE;
-- @skipoff
