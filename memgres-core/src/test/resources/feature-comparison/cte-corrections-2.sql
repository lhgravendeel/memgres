-- ============================================================================
-- Feature Comparison: WITH clause corrections, second round
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Four subjects, all reached through the same WITH clause.
--
-- Where a self-reference may sit. PostgreSQL evaluates the self-reference as one
-- scan of the rows the previous round produced, so it refuses the places that
-- would need the whole result at once -- and only those. A sub-select of an
-- expression and the null-extended side of an outer join are two of them. A set
-- operation is one only where an arm is subtracted or where duplicate counts
-- matter: the right-hand side of any EXCEPT, and either side of an INTERSECT ALL
-- or EXCEPT ALL. Plain INTERSECT, and the left side of a plain EXCEPT, grow with
-- the rows the previous round produced, and PostgreSQL runs them. Refusing those
-- too turned five queries PostgreSQL answers into errors.
--
-- Which complaint comes first. PostgreSQL reads ORDER BY, LIMIT, OFFSET and FOR
-- UPDATE off the set operation before it goes looking for the self-reference at
-- all, so a recursive query that is wrong in both ways is refused for the clause
-- it has not implemented, not for the reference. Among the references, where one
-- sits is decided before how many there are, and "more than once" is what is
-- left when every one of them was somewhere admissible.
--
-- What the clauses after an item may say. SEARCH and CYCLE each add a column, so
-- two of them under one name leave the query no way to say which it meant. The
-- value a CYCLE marks a row with is a constant -- a sign, an operator, a cast or
-- a function call is a syntax error where PostgreSQL stops reading it -- and the
-- mark and its default resolve to one type between them, so TO 1 DEFAULT 'x' is
-- a bad integer and TO true DEFAULT 1 has no common type at all.
--
-- What the generated columns hold. SEARCH BREADTH FIRST BY p,c orders by depth
-- and then by p and then by c, so its column is the record (depth, p, c) rather
-- than a record holding the pair as one field. A CYCLE clause's two columns
-- count towards a UNION's duplicate removal, so the row that closes a cycle
-- survives beside the row that started it. And subscripting an array answers
-- with the array's element type, not with jsonb.
--
-- The neighbouring shapes that must keep working are here too: parenthesised set
-- operations as an arm, a WITH clause parenthesised as an arm, ordinary joins
-- and FROM subqueries in a recursive term, every constant a CYCLE may name, and
-- WITH items the query never reads.
-- ============================================================================

DROP TABLE IF EXISTS ctn_edge CASCADE;
CREATE TABLE ctn_edge (a int, b int);
INSERT INTO ctn_edge VALUES (1,2), (2,3), (3,1), (3,4);

DROP TABLE IF EXISTS ctn_tree CASCADE;
CREATE TABLE ctn_tree (p int, c int);
INSERT INTO ctn_tree VALUES (1,2), (1,3), (2,4), (2,5), (3,6);

-- ============================================================================
-- 1. A set operation inside the recursive term
-- ============================================================================

-- The left side of a plain EXCEPT grows with the previous round's rows.
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT n+1 FROM r WHERE n < 3 EXCEPT SELECT 99)
) SELECT * FROM r ORDER BY n;

-- Either side of a plain INTERSECT does too.
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT n+1 FROM r WHERE n < 3 INTERSECT SELECT 2)
) SELECT * FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT 2 INTERSECT SELECT n+1 FROM r WHERE n < 3)
) SELECT * FROM r ORDER BY n;

-- A FROM subquery is a place the reference may sit, set operation and all.
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT s.n+1 FROM (SELECT n FROM r EXCEPT SELECT 99) s WHERE s.n < 3
) SELECT * FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT s.n+1 FROM (SELECT n FROM r) s WHERE s.n < 3 EXCEPT SELECT 99)
) SELECT * FROM r ORDER BY n;

-- The right side of an EXCEPT is subtracted, so it is refused.
-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within EXCEPT
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT 99 EXCEPT SELECT n+1 FROM r WHERE n < 3)
) SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within EXCEPT
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT * FROM (SELECT 99 EXCEPT SELECT n+1 FROM r WHERE n < 3) t
) SELECT * FROM r;

-- EXCEPT ALL and INTERSECT ALL count duplicates, so either side is refused.
-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within EXCEPT
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT n+1 FROM r WHERE n < 3 EXCEPT ALL SELECT 99)
) SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within INTERSECT
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT 2 INTERSECT ALL SELECT n+1 FROM r WHERE n < 3)
) SELECT * FROM r;

-- A sub-select of an expression is nearer than the EXCEPT around it.
-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within a subquery
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT 99 EXCEPT SELECT 1 WHERE 1 IN (SELECT n FROM r))
) SELECT * FROM r;

-- Two admissible references are "more than once", whatever encloses them.
-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear more than once
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT n+1 FROM r WHERE n < 3 INTERSECT SELECT n+1 FROM r WHERE n < 3)
) SELECT * FROM r;

-- ============================================================================
-- 2. The clause PostgreSQL has not implemented is read first
-- ============================================================================

-- Two references and a LIMIT: the LIMIT is what is reported.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: LIMIT in a recursive query is not implemented
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT a.n+b.n FROM r a, r b WHERE a.n < 3 LIMIT 5
) SELECT * FROM r;

-- A reference in the non-recursive term and a LIMIT.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: LIMIT in a recursive query is not implemented
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT n FROM r UNION ALL SELECT n+1 FROM r WHERE n < 3 LIMIT 5
) SELECT * FROM r;

-- A reference in a sub-select and a LIMIT.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: LIMIT in a recursive query is not implemented
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT 2 FROM ctn_edge WHERE 2 NOT IN (SELECT n FROM r) LIMIT 1
) SELECT * FROM r;

-- A reference on an outer join's nullable side and an ORDER BY.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ORDER BY in a recursive query is not implemented
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT t.n+1 FROM (VALUES (1)) v LEFT JOIN r t ON true ORDER BY 1
) SELECT * FROM r;

-- A reference in the non-recursive term and an OFFSET.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: OFFSET in a recursive query is not implemented
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT n FROM r UNION ALL SELECT 1 OFFSET 1
) SELECT * FROM r;

-- Where nothing outranks them, the reference rules still speak.
-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within an outer join
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT t.n+1 FROM r t LEFT JOIN r u ON true
) SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within a subquery
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r a WHERE n IN (SELECT n FROM r b)
) SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear more than once
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT max(n) FROM r a, r b
) SELECT * FROM r;

-- ============================================================================
-- 3. A WITH item the query never reads is checked all the same
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive query "unused_c" does not have the form non-recursive-term UNION [ALL] recursive-term
-- end-expected-error
WITH RECURSIVE unused_c(n) AS (
  SELECT 1 INTERSECT SELECT n+1 FROM unused_c
) SELECT 5 AS m;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "unused_c" must not appear within its non-recursive term
-- end-expected-error
WITH RECURSIVE unused_c(n) AS (
  SELECT n FROM unused_c UNION ALL SELECT 1
) SELECT 5 AS m;

-- An unread item that is well formed is still no error, and neither is one that
-- never names itself.
-- begin-expected
-- columns: m
-- row: 5
-- end-expected
WITH RECURSIVE unused_c(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM unused_c WHERE n < 3
) SELECT 5 AS m;

-- begin-expected
-- columns: m
-- row: 5
-- end-expected
WITH unused_c AS (SELECT 1 INTERSECT SELECT 1) SELECT 5 AS m;

-- begin-expected
-- columns: m
-- row: 5
-- end-expected
WITH RECURSIVE unused_c(n) AS (SELECT 1) SELECT 5 AS m;

-- ============================================================================
-- 4. Parenthesised arms of a set operation
-- ============================================================================

-- A parenthesised set operation as the right arm of another.
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL ((SELECT n+1 FROM r WHERE n < 3) EXCEPT (SELECT 99))
) SELECT * FROM r ORDER BY n;

-- begin-expected
-- columns: ?column?
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT 1 UNION ALL ((SELECT 2) UNION ALL (SELECT 3));

-- begin-expected
-- columns: ?column?
-- row: 1
-- row: 2
-- end-expected
SELECT 1 UNION ALL (((SELECT 2) EXCEPT (SELECT 3)));

-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT 1 INTERSECT ((SELECT 1) UNION (SELECT 2));

-- begin-expected
-- columns: ?column?
-- row: 1
-- row: 2
-- end-expected
SELECT 1 UNION ALL ((SELECT 2));

-- A WITH clause is not one of the productions an arm may be.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "WITH"
-- end-expected-error
SELECT 1 UNION ALL WITH x AS (SELECT 2) SELECT * FROM x;

-- Parenthesised it is a query in its own right.
-- begin-expected
-- columns: ?column?
-- row: 1
-- row: 2
-- end-expected
SELECT 1 UNION ALL (WITH x AS (SELECT 2) SELECT * FROM x);

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (WITH i AS (SELECT n FROM r) SELECT n+1 FROM i WHERE n < 3)
) SELECT * FROM r ORDER BY n;

-- ============================================================================
-- 5. What a CYCLE clause may mark a row with
-- ============================================================================

-- A function call: the argument list may not be empty.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) CYCLE n SET c TO random() DEFAULT 0 USING p SELECT n FROM r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "+"
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) CYCLE n SET c TO 1+1 DEFAULT 0 USING p SELECT n FROM r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "-"
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) CYCLE n SET c TO -1 DEFAULT 0 USING p SELECT n FROM r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "::"
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) CYCLE n SET c TO 'x'::text DEFAULT 'y' USING p SELECT n FROM r;

-- The mark and its default resolve to one type between them.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
WITH RECURSIVE r(a,b) AS (
  SELECT a,b FROM ctn_edge WHERE a = 1
  UNION ALL SELECT e.a, e.b FROM ctn_edge e, r WHERE e.a = r.b
) CYCLE a SET is_cycle TO 1 DEFAULT 'x' USING path SELECT a,b,is_cycle FROM r;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: CYCLE types boolean and integer cannot be matched
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) CYCLE n SET c TO true DEFAULT 1 USING p SELECT n, c FROM r;

-- The constants a CYCLE may name, and the type each pair resolves to.
-- begin-expected
-- columns: n | c | pg_typeof
-- row: 1 | 0 | integer
-- row: 2 | 0 | integer
-- row: 3 | 0 | integer
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) CYCLE n SET c TO 1 DEFAULT 0 USING p SELECT n, c, pg_typeof(c)::text FROM r ORDER BY n;

-- begin-expected
-- columns: n | c | pg_typeof
-- row: 1 | 1.5 | numeric
-- row: 2 | 1.5 | numeric
-- row: 3 | 1.5 | numeric
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) CYCLE n SET c TO 1 DEFAULT 1.5 USING p SELECT n, c, pg_typeof(c)::text FROM r ORDER BY n;

-- begin-expected
-- columns: n | c | pg_typeof
-- row: 1 | 2 | integer
-- row: 2 | 2 | integer
-- row: 3 | 2 | integer
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) CYCLE n SET c TO 1 DEFAULT '2' USING p SELECT n, c, pg_typeof(c)::text FROM r ORDER BY n;

-- begin-expected
-- columns: n | c | pg_typeof
-- row: 1 | n | text
-- row: 2 | n | text
-- row: 3 | n | text
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) CYCLE n SET c TO 'y' DEFAULT 'n' USING p SELECT n, c, pg_typeof(c)::text FROM r ORDER BY n;

-- begin-expected
-- columns: n | c | pg_typeof
-- row: 1 | 2021-01-01 | date
-- row: 2 | 2021-01-01 | date
-- row: 3 | 2021-01-01 | date
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) CYCLE n SET c TO DATE '2020-01-01' DEFAULT DATE '2021-01-01' USING p
SELECT n, c, pg_typeof(c)::text FROM r ORDER BY n;

-- ============================================================================
-- 6. The columns SEARCH and CYCLE add
-- ============================================================================

-- Two of them under one name leave the query no way to say which it meant.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: search sequence column name and cycle mark column name are the same
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET z CYCLE n SET z USING p SELECT n FROM r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: search sequence column name and cycle path column name are the same
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET z CYCLE n SET c USING z SELECT n FROM r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: cycle mark column name and cycle path column name are the same
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) CYCLE n SET c USING c SELECT n FROM r;

-- Distinct names are three more columns beside the query's own.
-- begin-expected
-- columns: n | o | c | p
-- row: 1 | {(1)} | f | {(1)}
-- row: 2 | {(1),(2)} | f | {(1),(2)}
-- row: 3 | {(1),(2),(3)} | f | {(1),(2),(3)}
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET o CYCLE n SET c USING p
SELECT n, o::text, c, p::text FROM r ORDER BY n;

-- SEARCH BREADTH FIRST BY p,c orders by depth and then by p and then by c, so
-- its column is the record (depth, p, c) -- not a record holding the pair.
-- begin-expected
-- columns: p | c | ord
-- row: 1 | 2 | (0,1,2)
-- row: 1 | 3 | (0,1,3)
-- row: 2 | 4 | (1,2,4)
-- row: 2 | 5 | (1,2,5)
-- row: 3 | 6 | (1,3,6)
-- end-expected
WITH RECURSIVE r(p,c) AS (
  SELECT p,c FROM ctn_tree WHERE p = 1
  UNION ALL SELECT t.p, t.c FROM ctn_tree t, r WHERE t.p = r.c
) SEARCH BREADTH FIRST BY p,c SET ord SELECT p, c, ord::text FROM r ORDER BY p, c;

-- Depth first over the same two columns is one record per step of the path.
-- begin-expected
-- columns: p | c | ord
-- row: 1 | 2 | {"(1,2)"}
-- row: 1 | 3 | {"(1,3)"}
-- row: 2 | 4 | {"(1,2)","(2,4)"}
-- row: 2 | 5 | {"(1,2)","(2,5)"}
-- row: 3 | 6 | {"(1,3)","(3,6)"}
-- end-expected
WITH RECURSIVE r(p,c) AS (
  SELECT p,c FROM ctn_tree WHERE p = 1
  UNION ALL SELECT t.p, t.c FROM ctn_tree t, r WHERE t.p = r.c
) SEARCH DEPTH FIRST BY p,c SET ord SELECT p, c, ord::text FROM r ORDER BY p, c;

-- Subscripting the array answers with its element type.
-- begin-expected
-- columns: pg_typeof | pg_typeof
-- row: record[] | record
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) CYCLE n SET c USING p SELECT pg_typeof(p)::text, pg_typeof(p[1])::text FROM r LIMIT 1;

-- ============================================================================
-- 7. A CYCLE clause's columns count towards a UNION's duplicate removal
-- ============================================================================

-- The row that closes the cycle carries a different mark and a longer path than
-- the row that started it, so UNION keeps both.
-- begin-expected
-- columns: a | b | is_cycle
-- row: 1 | 2 | f
-- row: 1 | 2 | t
-- row: 2 | 3 | f
-- row: 3 | 1 | f
-- row: 3 | 4 | f
-- end-expected
WITH RECURSIVE r(a,b) AS (
  SELECT a,b FROM ctn_edge WHERE a = 1
  UNION SELECT e.a, e.b FROM ctn_edge e, r WHERE e.a = r.b
) CYCLE a SET is_cycle USING path SELECT a, b, is_cycle FROM r ORDER BY a, b, is_cycle;

-- UNION ALL over the same graph answers the same way.
-- begin-expected
-- columns: a | b | is_cycle
-- row: 1 | 2 | f
-- row: 1 | 2 | t
-- row: 2 | 3 | f
-- row: 3 | 1 | f
-- row: 3 | 4 | f
-- end-expected
WITH RECURSIVE r(a,b) AS (
  SELECT a,b FROM ctn_edge WHERE a = 1
  UNION ALL SELECT e.a, e.b FROM ctn_edge e, r WHERE e.a = r.b
) CYCLE a SET is_cycle USING path SELECT a, b, is_cycle FROM r ORDER BY a, b, is_cycle;

-- Without a CYCLE clause the duplicate removal is over the query's own columns.
-- begin-expected
-- columns: a | b
-- row: 1 | 2
-- row: 2 | 3
-- row: 3 | 1
-- row: 3 | 4
-- end-expected
WITH RECURSIVE r(a,b) AS (
  SELECT a,b FROM ctn_edge WHERE a = 1
  UNION SELECT e.a, e.b FROM ctn_edge e, r WHERE e.a = r.b
) SELECT a, b FROM r ORDER BY a, b;

-- ============================================================================
-- 8. A recursive term's column type
-- ============================================================================

-- Types with no common type at all are named, both ways round.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: UNION types jsonb and integer cannot be matched
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT '{}'::jsonb UNION ALL SELECT 1 FROM r WHERE n IS NOT NULL
) SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: UNION types boolean and integer cannot be matched
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT true UNION ALL SELECT 1 FROM r WHERE n IS TRUE
) SELECT * FROM r;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: UNION types integer and timestamp with time zone cannot be matched
-- end-expected-error
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT now() FROM r) SELECT * FROM r;

-- An array and a scalar are not each other's type whatever their elements are.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: UNION types integer and integer[] cannot be matched
-- end-expected-error
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT '{1}'::int[] FROM r) SELECT * FROM r;

-- A bare NULL and a bare string literal carry no type of their own, so neither
-- is refused here.
-- begin-expected
-- columns: n
-- row: 1
-- row: NULL
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT NULL FROM r WHERE n < 3
) SELECT * FROM r;

-- A seed written as a bare NULL is text by default, so a recursive term of any
-- other type widens the column and PostgreSQL says so.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: recursive query "r" column 1 has type text in non-recursive term but type integer overall
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT NULL UNION ALL SELECT 1 FROM r WHERE n IS NULL
) SELECT * FROM r;

-- ============================================================================
-- 9. Ordinary recursion, unchanged
-- ============================================================================

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3
) SELECT * FROM r ORDER BY n;

-- begin-expected
-- columns: a | b
-- row: 1 | 2
-- row: 2 | 3
-- row: 3 | 4
-- end-expected
WITH RECURSIVE r(a,b) AS (
  SELECT a,b FROM ctn_edge WHERE a = 1
  UNION ALL SELECT e.a, e.b FROM ctn_edge e JOIN r ON e.a = r.b WHERE e.b <> 1
) SELECT * FROM r ORDER BY a, b;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n+1 FROM (SELECT n FROM r) q WHERE n < 3
) SELECT * FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  (SELECT 1 EXCEPT SELECT 99) UNION ALL SELECT n+1 FROM r WHERE n < 3
) SELECT * FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT t.n+1 FROM r t LEFT JOIN (VALUES (1)) v(x) ON true WHERE t.n < 3
) SELECT * FROM r ORDER BY n;

-- begin-expected
-- columns: s
-- row: a
-- row: aa
-- row: aaa
-- end-expected
WITH RECURSIVE r(s) AS (
  SELECT 'a'::text UNION ALL SELECT s || 'a' FROM r WHERE length(s) < 3
) SELECT * FROM r ORDER BY s;

DROP TABLE ctn_tree;
DROP TABLE ctn_edge;
