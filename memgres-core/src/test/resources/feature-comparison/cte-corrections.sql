-- ============================================================================
-- Feature Comparison: WITH clause corrections
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Four subjects, all reached through the same WITH clause.
--
-- The clauses after a WITH item. PostgreSQL's grammar allows one SEARCH and one
-- CYCLE, in that order, attached to the item they follow. Reading them twice --
-- once per item and again after the whole clause -- let the second pass rebuild
-- the last item from whichever of the two it found and drop the other. Written
-- CYCLE ... SEARCH ... that dropped the CYCLE clause, which was the only thing
-- bounding the recursion, and a query that is a syntax error ran out of memory
-- instead. They are read once, and a second SEARCH or CYCLE is a syntax error.
--
-- What SEARCH and CYCLE may name. Declaring RECURSIVE does not make an item
-- recursive; naming itself does, and only an item that recurses can be ordered
-- or cut. A column the item does not have cannot be searched or compared, and a
-- column the clause adds under a name the query already uses would leave two
-- columns answering to it.
--
-- What the recursive term's columns may be. The union resolves one type per
-- column from both arms and the seed's rows have to already carry it, so a
-- recursive term that widens a column is refused -- but only where the widening
-- can be shown. The character types are not a ladder (PostgreSQL runs a varchar
-- seed with a text recursive term), and a bare string literal has no type of its
-- own, so neither is refused.
--
-- What a name reaches. A WITH item lives in no schema, so public.c is always the
-- stored relation; a quoted declaration keeps its case; and an inner WITH clause
-- declaring the same name means its own item everywhere below.
--
-- The neighbouring shapes that must keep working are here too: one SEARCH then
-- one CYCLE on any item, a search key of several columns, a narrowing recursive
-- term, an unadorned string literal, a cast in a CASE condition, an alias list
-- shorter than the query's output, and LIMIT ALL outside a recursive query.
-- ============================================================================

DROP TABLE IF EXISTS ctc_t CASCADE;
CREATE TABLE ctc_t (i int);
INSERT INTO ctc_t VALUES (1), (2), (3);

DROP TABLE IF EXISTS ctc_w CASCADE;
CREATE TABLE ctc_w (a int, b text);
INSERT INTO ctc_w VALUES (1,'x'), (2,'y'), (2,'z'), (3,'q');

DROP TABLE IF EXISTS ctc_e CASCADE;
CREATE TABLE ctc_e (src int, dst int);
INSERT INTO ctc_e VALUES (1,2), (1,3), (2,4), (3,5), (5,1);

-- ============================================================================
-- 1. SEARCH and CYCLE are read once, in PostgreSQL's order
-- ============================================================================

-- CYCLE before SEARCH: the grammar has no production for it. Reading the words
-- twice discarded the CYCLE clause and left this recursion unbounded.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SEARCH"
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT (n+1) % 3 FROM r WHERE n < 10
) CYCLE n SET is_cycle USING path SEARCH DEPTH FIRST BY n SET ord
SELECT n FROM r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SEARCH"
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET a SEARCH BREADTH FIRST BY n SET b
SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "CYCLE"
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) CYCLE n SET a USING p CYCLE n SET b USING q
SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SEARCH"
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET a CYCLE n SET c USING p
  SEARCH BREADTH FIRST BY n SET b
SELECT * FROM r;

-- one of each, in order, still reads
-- begin-expected
-- columns: n | c
-- row: 0 | f
-- row: 1 | f
-- row: 1 | t
-- row: 2 | f
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT (n+1) % 3 FROM r WHERE n < 10
) SEARCH DEPTH FIRST BY n SET o CYCLE n SET c USING p
SELECT n, c FROM r ORDER BY n, c;

-- a clause on an item that is not the last one, and the comma after it
-- begin-expected
-- columns: n | k
-- row: 1 | 9
-- row: 2 | 9
-- row: 3 | 9
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET ord, s AS (SELECT 9 AS k)
SELECT r.n, s.k FROM r, s ORDER BY 1;

-- ============================================================================
-- 2. SEARCH and CYCLE need a recursion, and columns that exist
-- ============================================================================

-- s never names itself and is never read; the clause is refused all the same
-- begin-expected-error
-- sqlstate: 42601
-- message-like: WITH query is not recursive
-- end-expected-error
WITH RECURSIVE s AS (SELECT 9 AS k) SEARCH DEPTH FIRST BY k SET ord,
     r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3)
SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: search column "zz" not in WITH query column list
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY zz SET ord SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: cycle column "zz" not in WITH query column list
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT (n+1) % 3 FROM r WHERE n < 10
) CYCLE zz SET is_cycle USING path SELECT * FROM r;

-- the column a clause adds may not be one the query already answers to
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "n" is ambiguous
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET n SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "n" is ambiguous
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) CYCLE n SET n USING path SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: cycle mark column name and cycle path column name are the same
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT (n+1) % 3 FROM r WHERE n < 10
) CYCLE n SET c USING c SELECT * FROM r;

-- ============================================================================
-- 3. What SEARCH and CYCLE put in the row
-- ============================================================================

-- The breadth-first ordering column leads with how deep the row is. Its record
-- text carries a comma, which the row directive reads as a column separator, so
-- the depth alone is compared here; CteCorrectionTest asserts the whole of it.
-- begin-expected
-- columns: node | depth | ord_depth
-- row: 1 | 0 | 0
-- row: 2 | 1 | 1
-- row: 3 | 1 | 1
-- row: 4 | 2 | 2
-- row: 5 | 2 | 2
-- row: 1 | 3 | 3
-- end-expected
WITH RECURSIVE r(node, depth) AS (
  SELECT 1, 0
  UNION ALL
  SELECT e.dst, r.depth + 1 FROM r JOIN ctc_e e ON e.src = r.node WHERE r.depth < 3
) SEARCH BREADTH FIRST BY node SET ord
SELECT node, depth, split_part(btrim(ord::text, '()'), ',', 1) AS ord_depth
FROM r ORDER BY depth, node;

-- The depth-first ordering column is an array of one record per step, so it is
-- record[] and grows a record per level. Both are compared without a comma;
-- CteCorrectionTest asserts the rendered arrays themselves.
-- begin-expected
-- columns: n | ord_type | steps
-- row: 1 | record[] | 1
-- row: 2 | record[] | 2
-- row: 3 | record[] | 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET ord
SELECT n, pg_typeof(ord)::text AS ord_type, array_length(ord, 1) AS steps
FROM r ORDER BY n;


-- the cycle path column is an array of records too
-- begin-expected
-- columns: n | path_type | steps
-- row: 1 | record[] | 1
-- row: 2 | record[] | 2
-- row: 3 | record[] | 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) CYCLE n SET c USING p
SELECT n, pg_typeof(p)::text AS path_type, array_length(p, 1) AS steps
FROM r ORDER BY n;

-- CYCLE ... TO v DEFAULT d marks with those values, in their own type
-- begin-expected
-- columns: n | c | mark_type
-- row: 0 | 0 | integer
-- row: 1 | 0 | integer
-- row: 1 | 1 | integer
-- row: 2 | 0 | integer
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT (n+1) % 3 FROM r WHERE n < 10
) CYCLE n SET c TO 1 DEFAULT 0 USING p
SELECT n, c, pg_typeof(c)::text AS mark_type FROM r ORDER BY n, c;

-- writing the two the same way marks every row, the seed included
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT (n+1) % 3 FROM r WHERE n < 10
) CYCLE n SET c TO true DEFAULT true USING p
SELECT n FROM r;

-- ============================================================================
-- 4. The recursive term's column types
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42804
-- message-like: recursive query "r" column 1 has type integer in non-recursive term but type numeric overall
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1.0 FROM r WHERE n < 3
) SELECT * FROM r;

-- a function's declared result type counts as much as a literal's
-- begin-expected-error
-- sqlstate: 42804
-- message-like: has type integer in non-recursive term but type numeric overall
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT trunc(n + 1.0) FROM r WHERE n < 3
) SELECT * FROM r;

-- a window call is allowed in a recursive term; only its type makes this illegal
-- begin-expected-error
-- sqlstate: 42804
-- message-like: has type integer in non-recursive term but type bigint overall
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + row_number() OVER () FROM r WHERE n < 3
) SELECT * FROM r;

-- a cast in a value position of COALESCE reaches the result
-- begin-expected-error
-- sqlstate: 42804
-- message-like: has type integer in non-recursive term but type bigint overall
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT coalesce(n, 0::bigint) + 1 FROM r WHERE n < 3
) SELECT * FROM r;

-- so does one in a CASE branch
-- begin-expected-error
-- sqlstate: 42804
-- message-like: has type integer in non-recursive term but type bigint overall
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT CASE WHEN n > 0 THEN n + 1 ELSE 0::bigint END FROM r WHERE n < 3
) SELECT * FROM r;

-- the column is named, not just the query
-- begin-expected-error
-- sqlstate: 42804
-- message-like: recursive query "r" column 2 has type integer in non-recursive term but type numeric overall
-- end-expected-error
WITH RECURSIVE r(a, b) AS (
  SELECT 1, 1 UNION ALL SELECT a + 1, b + 1.0 FROM r WHERE a < 3
) SELECT * FROM r;

-- a seed whose type comes from a table column is checked the same way
-- begin-expected-error
-- sqlstate: 42804
-- message-like: has type integer in non-recursive term but type numeric overall
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT i FROM ctc_t WHERE i = 1 UNION ALL SELECT n + 1.0 FROM r WHERE n < 3
) SELECT * FROM r;

-- and so is a recursive term that is itself a set operation
-- begin-expected-error
-- sqlstate: 42804
-- message-like: has type integer in non-recursive term but type numeric overall
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT n + 1.0 FROM r WHERE n < 3 UNION ALL SELECT 9.5 WHERE false)
) SELECT * FROM r;

-- date widened to timestamp
-- begin-expected-error
-- sqlstate: 42804
-- message-like: has type date in non-recursive term but type timestamp without time zone overall
-- end-expected-error
WITH RECURSIVE r(d) AS (
  SELECT DATE '2020-01-01'
  UNION ALL
  SELECT d + interval '1 day' FROM r WHERE d < DATE '2020-01-03'
) SELECT * FROM r;

-- a seed written as a bare NULL has no type of its own
-- begin-expected-error
-- sqlstate: 42804
-- message-like: has type text in non-recursive term but type integer overall
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT NULL UNION ALL SELECT 1 FROM r WHERE n IS NULL
) SELECT * FROM r;

-- two arms with no common type at all are refused earlier and differently
-- begin-expected-error
-- sqlstate: 42804
-- message-like: UNION types integer and text cannot be matched
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n::text FROM r WHERE n < 3
) SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: UNION types text and integer cannot be matched
-- end-expected-error
WITH RECURSIVE r(n, m) AS (
  SELECT 1, 'x'::text UNION ALL SELECT n + 1, 5 FROM r WHERE n < 3
) SELECT * FROM r;

-- narrowing into the seed's own type is fine
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1::bigint UNION ALL SELECT (n + 1)::int FROM r WHERE n < 3
) SELECT n FROM r ORDER BY n;

-- the character types are not a widening ladder
-- begin-expected
-- columns: s
-- row: a
-- row: aa
-- row: aaa
-- end-expected
WITH RECURSIVE r(s) AS (
  SELECT 'a'::varchar UNION ALL SELECT (s || 'a')::text FROM r WHERE length(s) < 3
) SELECT s FROM r ORDER BY s;

-- begin-expected
-- columns: s
-- row: a
-- row: aa
-- row: aaa
-- end-expected
WITH RECURSIVE r(s) AS (
  SELECT 'a'::name UNION ALL SELECT (s || 'a')::text FROM r WHERE length(s) < 3
) SELECT s FROM r ORDER BY s;

-- an unadorned string literal is of no type yet: it takes the seed's
-- begin-expected
-- columns: n
-- row: 1
-- row: 5
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT '5' FROM r WHERE n < 3
) SELECT n FROM r ORDER BY n;

-- and one that is not a number in an integer column is bad input, as before
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT 'x' FROM r WHERE n < 3
) SELECT * FROM r;

-- a cast in a CASE condition decides nothing about the value's type
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT CASE WHEN n::bigint > 0 THEN n + 1 ELSE 0 END FROM r WHERE n < 3
) SELECT n FROM r ORDER BY n;

-- ============================================================================
-- 5. Where a self-reference may sit
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within EXCEPT
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT 2 EXCEPT SELECT n FROM r)
) SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within INTERSECT
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT i FROM ctc_t INTERSECT ALL SELECT n + 1 FROM r WHERE n < 3)
) SELECT * FROM r;

-- two references, one of them in a sub-select: the sub-select is what is named
-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within a subquery
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 AND n NOT IN (SELECT n FROM r)
) SELECT * FROM r;

-- an inner WITH item of the same name is not a self-reference: r never recurses,
-- so the body is an ordinary UNION ALL run once
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1
  UNION ALL
  SELECT z.n + 1 FROM (WITH r AS (SELECT 1 AS n) SELECT n FROM r) z WHERE z.n < 3
) SELECT n FROM r ORDER BY n;

-- an inner WITH of a different name leaves the recursion alone
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1
  UNION ALL
  SELECT z.k + 1 FROM (WITH q AS (SELECT n AS k FROM r) SELECT k FROM q) z WHERE z.k < 3
) SELECT n FROM r ORDER BY n;

-- LIMIT ALL is a LIMIT clause, and a recursive query has not implemented one
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: LIMIT in a recursive query is not implemented
-- end-expected-error
WITH RECURSIVE t(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3 LIMIT ALL
) SELECT count(*) FROM t;

-- TABLESAMPLE reads a fraction of a stored relation's pages; a WITH item has none
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: TABLESAMPLE clause can only be applied to tables and materialized views
-- end-expected-error
WITH c AS (SELECT 1 AS i) SELECT * FROM c TABLESAMPLE SYSTEM (100);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: TABLESAMPLE clause can only be applied to tables and materialized views
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r TABLESAMPLE SYSTEM (100) WHERE n < 3
) SELECT * FROM r;

-- ============================================================================
-- 6. What a WITH name reaches
-- ============================================================================

-- a schema-qualified name is always the stored relation
-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH ctc_t AS (SELECT 99 AS i) SELECT * FROM public.ctc_t ORDER BY i;

-- the unqualified one is the WITH item
-- begin-expected
-- columns: i
-- row: 99
-- end-expected
WITH ctc_t AS (SELECT 99 AS i) SELECT * FROM ctc_t ORDER BY i;

-- and a qualified name that is only a WITH item does not exist
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "public.r" does not exist
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM public.r WHERE n < 3
) SELECT * FROM r;

-- a quoted WITH name keeps its case, so a plain one is a different name
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "x" does not exist
-- end-expected-error
WITH "X" AS (SELECT 1 AS n) SELECT * FROM x;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
WITH "X" AS (SELECT 1 AS n) SELECT * FROM "X";

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
WITH MyCte AS (SELECT 1 AS n) SELECT * FROM mycte;

-- a forward reference says which WITH item it was and how to fix it
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "b" does not exist
-- end-expected-error
WITH a AS (SELECT 1 AS x FROM b), b AS (SELECT 2 AS y) SELECT * FROM a;

-- RECURSIVE lets the same forward reference through
-- begin-expected
-- columns: x
-- row: 1
-- end-expected
WITH RECURSIVE a AS (SELECT 1 AS x FROM b), b AS (SELECT 2 AS y) SELECT * FROM a;

-- ============================================================================
-- 7. Column lists on a WITH item and on a FROM item
-- ============================================================================

-- an alias list shorter than the query's output renames as far as it reaches
-- begin-expected
-- columns: a | ?column?
-- row: 1 | 2
-- end-expected
WITH x(a) AS (SELECT 1, 2) SELECT * FROM x;

-- begin-expected
-- columns: n | ?column?
-- row: 1 | 5
-- row: 2 | 6
-- row: 3 | 6
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1, 5 UNION ALL SELECT n + 1, 6 FROM r WHERE n < 3
) SELECT * FROM r ORDER BY n;

-- naming more columns than the query has names the WITH query
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: WITH query "x" has 1 columns available but 2 columns specified
-- end-expected-error
WITH x(a, b) AS (SELECT 1) SELECT * FROM x;

-- an alias list on a FROM item renames that item's columns
-- begin-expected
-- columns: m | n
-- row: 1 | x
-- row: 2 | y
-- row: 2 | z
-- row: 3 | q
-- end-expected
SELECT * FROM ctc_w AS z(m, n) ORDER BY m, n;

-- begin-expected
-- columns: m | b
-- row: 1 | x
-- row: 2 | y
-- row: 2 | z
-- row: 3 | q
-- end-expected
SELECT * FROM ctc_w z(m) ORDER BY m, b;

-- begin-expected
-- columns: m
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SELECT * FROM r AS z(m) ORDER BY m;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "z" has 2 columns available but 3 columns specified
-- end-expected-error
SELECT * FROM ctc_w AS z(m, n, o);

-- ============================================================================
-- 8. TABLE, LIMIT and WITH TIES around a WITH clause
-- ============================================================================

-- TABLE t is a query wherever a query may stand, the body of a WITH included
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) TABLE r;

-- a LIMIT past bigint is out of range, not a negative count
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT a FROM ctc_w ORDER BY 1 LIMIT 9999999999999999999;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT a FROM ctc_w ORDER BY 1 OFFSET 9999999999999999999;

-- WITH TIES keeps the rows tied with the last one, on a set operation too
-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 2
-- row: 2
-- end-expected
SELECT a FROM ctc_w UNION ALL SELECT 2 ORDER BY 1 FETCH FIRST 2 ROWS WITH TIES;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 2
-- end-expected
SELECT a FROM ctc_w ORDER BY a FETCH FIRST 2 ROWS WITH TIES;

-- ROWS ONLY still cuts at the count
-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM ctc_w ORDER BY a FETCH FIRST 2 ROWS ONLY;

-- LIMIT ALL outside a recursive query is still no limit
-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT i FROM ctc_t ORDER BY 1 LIMIT ALL;

DROP TABLE ctc_e;
DROP TABLE ctc_w;
DROP TABLE ctc_t;
