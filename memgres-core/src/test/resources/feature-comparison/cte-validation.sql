-- ============================================================================
-- Feature Comparison: WITH clause validation
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A WITH RECURSIVE item is evaluated by scanning, over and over, the rows the
-- previous round produced. That is the only thing its self-reference can mean,
-- so PostgreSQL insists the reference sit where a single such scan makes sense:
-- once, in the FROM clause of the recursive term, not on a side an outer join
-- may null-extend and not inside a sub-select of an expression. Everything else
-- asks for the finished result while it is still being built and is refused
-- with 42P19. A few clauses -- ORDER BY, OFFSET, LIMIT, FOR UPDATE -- are
-- simply not implemented for a recursive query and are 0A000, and a recursive
-- term whose column type is wider than the seed's is 42804.
--
-- The second half is name resolution. Without RECURSIVE a WITH item sees only
-- the items written before it: not itself, and not what follows. A name that is
-- not visible falls through to a stored relation of that name if there is one,
-- and is 42P01 if there is not.
--
-- The neighbouring shapes that must keep working are here too: a self-reference
-- in a FROM-clause subquery, on the preserved side of an outer join, an
-- aggregate or a LIMIT one query level down, a window function, DISTINCT,
-- GROUP BY, and a WITH RECURSIVE item that never names itself at all.
-- ============================================================================

DROP TABLE IF EXISTS cte_t CASCADE;
CREATE TABLE cte_t (n int);
INSERT INTO cte_t VALUES (1), (2), (3);

DROP TABLE IF EXISTS cte_edge CASCADE;
CREATE TABLE cte_edge (a int, b int);
INSERT INTO cte_edge VALUES (1,2), (2,3), (3,4);

-- ============================================================================
-- 1. The recursive term may name the WITH item exactly once
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear more than once
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT a.n + b.n FROM r a, r b WHERE a.n < 4
) SELECT n FROM r ORDER BY n;

-- references inside FROM subqueries count towards the one allowed
-- begin-expected-error
-- sqlstate: 42P19
-- message-like: must not appear more than once
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL
  SELECT q.n + 1 FROM (SELECT n FROM r) q, (SELECT n FROM r) p WHERE q.n < 3 AND p.n = q.n
) SELECT n FROM r ORDER BY n;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: must not appear more than once
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL
  SELECT q.n + 1 FROM r, (SELECT n FROM r) q WHERE q.n < 3 AND r.n = q.n
) SELECT n FROM r ORDER BY n;

-- the seed has to be computable without the result it seeds
-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within its non-recursive term
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT n FROM r UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SELECT n FROM r ORDER BY n;

-- with three arms, everything left of the last UNION is the non-recursive term
-- begin-expected-error
-- sqlstate: 42P19
-- message-like: must not appear within its non-recursive term
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 UNION ALL SELECT n + 1 FROM r WHERE n < 2
) SELECT n FROM r ORDER BY n;

-- ============================================================================
-- 2. Not inside a sub-select of an expression
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within a subquery
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM cte_t WHERE n IN (SELECT n FROM r) AND n < 3
) SELECT n FROM r ORDER BY n;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: must not appear within a subquery
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM cte_t WHERE EXISTS (SELECT 1 FROM r) AND n < 3
) SELECT n FROM r ORDER BY n;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: must not appear within a subquery
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL
  SELECT n + 1 FROM cte_t WHERE NOT EXISTS (SELECT 1 FROM r WHERE r.n = cte_t.n) AND n < 3
) SELECT n FROM r ORDER BY n;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: must not appear within a subquery
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT (SELECT max(n) FROM r) + 1 FROM cte_t WHERE n < 2
) SELECT n FROM r ORDER BY n;

-- a FROM-clause subquery over the recursive name is not a subquery reference
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT q.n + 1 FROM (SELECT n FROM r) q WHERE q.n < 3
) SELECT n FROM r ORDER BY n;

-- nor is one that wraps it in a WITH clause of its own
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL
  SELECT n + 1 FROM (WITH q AS (SELECT n FROM r) SELECT n FROM q) z WHERE n < 3
) SELECT n FROM r ORDER BY n;

-- ============================================================================
-- 3. Not on a side an outer join may null-extend
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive reference to query "r" must not appear within an outer join
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL
  SELECT t.n + 1 FROM cte_t x LEFT JOIN r t ON true WHERE t.n < 3 AND x.n = 1
) SELECT n FROM r ORDER BY n;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: must not appear within an outer join
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT t.n + 1 FROM r t RIGHT JOIN cte_t x ON x.n = t.n WHERE t.n < 3
) SELECT n FROM r ORDER BY n;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: must not appear within an outer join
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT t.n + 1 FROM r t FULL JOIN cte_t x ON x.n = t.n WHERE t.n < 3
) SELECT n FROM r ORDER BY n;

-- a subquery over the recursive name on the nullable side counts too
-- begin-expected-error
-- sqlstate: 42P19
-- message-like: must not appear within an outer join
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL
  SELECT t.n + 1 FROM cte_t x LEFT JOIN (SELECT n FROM r) t ON true WHERE t.n < 3 AND x.n = 1
) SELECT n FROM r ORDER BY n;

-- the preserved side is ordinary SQL
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT t.n + 1 FROM r t LEFT JOIN cte_t x ON x.n = t.n WHERE t.n < 3
) SELECT n FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT t.n + 1 FROM cte_t x RIGHT JOIN r t ON x.n = t.n WHERE t.n < 3
) SELECT n FROM r ORDER BY n;

-- an outer join whose nullable side names something else entirely
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL
  SELECT r.n + 1 FROM r, (SELECT t.n FROM cte_t t LEFT JOIN cte_t u ON t.n = u.n) q
  WHERE r.n < 2 AND q.n = 1
) SELECT n FROM r ORDER BY n;

-- ============================================================================
-- 4. No aggregate at the recursive term's own query level
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: aggregate functions are not allowed in a recursive query's recursive term
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT max(n) + 1 FROM r WHERE n < 3
) SELECT n FROM r ORDER BY n;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: aggregate functions are not allowed
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT count(*)::int + n FROM r WHERE n < 3 GROUP BY n
) SELECT n FROM r ORDER BY n;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: aggregate functions are not allowed
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 GROUP BY n HAVING count(*) > 0
) SELECT n FROM r ORDER BY n;

-- one query level down it is over that query, not this one
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT r.n + 1 FROM r, (SELECT max(n) AS m FROM cte_t) q WHERE r.n < q.m
) SELECT n FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < (SELECT max(n) FROM cte_t)
) SELECT n FROM r ORDER BY n;

-- and the non-recursive term may aggregate freely
-- begin-expected
-- columns: n
-- row: 3
-- row: 4
-- row: 5
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT max(n) FROM cte_t UNION ALL SELECT n + 1 FROM r WHERE n < 5
) SELECT n FROM r ORDER BY n;

-- a window function in the recursive term is allowed
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + (row_number() OVER ())::int FROM r WHERE n < 3
) SELECT n FROM r ORDER BY n;

-- ============================================================================
-- 5. Clauses PostgreSQL has not implemented for a recursive query
-- ============================================================================

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ORDER BY in a recursive query is not implemented
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 ORDER BY n
) SELECT n FROM r ORDER BY n;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: OFFSET in a recursive query is not implemented
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 OFFSET 0
) SELECT n FROM r ORDER BY n;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: LIMIT in a recursive query is not implemented
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 LIMIT 10
) SELECT n FROM r ORDER BY n;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE/SHARE in a recursive query is not implemented
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 FOR UPDATE
) SELECT n FROM r ORDER BY n;

-- the same words inside a parenthesised arm belong to that arm
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  (SELECT 1 LIMIT 1) UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SELECT n FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL (SELECT n + 1 FROM r WHERE n < 3 OFFSET 0)
) SELECT n FROM r ORDER BY n;

-- or to a subquery of the recursive term
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT r.n + 1 FROM r, (SELECT n FROM cte_t LIMIT 1) q WHERE r.n < 3
) SELECT n FROM r ORDER BY n;

-- and LIMIT on the query that reads the WITH item is untouched
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5
) SELECT n FROM r ORDER BY n LIMIT 2;

-- ============================================================================
-- 6. The union form, and column types across the two terms
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive query "r" does not have the form non-recursive-term UNION [ALL] recursive-term
-- end-expected-error
WITH RECURSIVE r(n) AS (SELECT 1 EXCEPT SELECT n FROM r) SELECT n FROM r;

-- begin-expected-error
-- sqlstate: 42P19
-- message-like: does not have the form non-recursive-term UNION [ALL] recursive-term
-- end-expected-error
WITH RECURSIVE r(n) AS (SELECT 1 INTERSECT SELECT n FROM r) SELECT n FROM r;

-- a RECURSIVE item that names itself outside a UNION is the same refusal, even
-- when a table of that name exists: with RECURSIVE the name is the WITH item
-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive query "cte_t" does not have the form
-- end-expected-error
WITH RECURSIVE cte_t AS (SELECT n FROM cte_t) SELECT n FROM cte_t ORDER BY n;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: recursive query "r" column 1 has type integer in non-recursive term but type bigint overall
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1::int UNION SELECT (n + 1)::bigint FROM r WHERE n < 3
) SELECT n FROM r ORDER BY n;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: has type integer in non-recursive term but type numeric overall
-- end-expected-error
WITH RECURSIVE r(n) AS (
  SELECT 1::int UNION ALL SELECT (n + 1)::numeric FROM r WHERE n < 3
) SELECT n FROM r ORDER BY n;

-- narrowing back to the seed's type is fine
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1::bigint UNION ALL SELECT (n + 1)::int FROM r WHERE n < 3
) SELECT n FROM r ORDER BY n;

-- and so is varchar against text, in either order
-- begin-expected
-- columns: s
-- row: a
-- row: aa
-- row: aaa
-- end-expected
WITH RECURSIVE r(s) AS (
  SELECT 'a'::varchar UNION ALL SELECT (s || 'a')::text FROM r WHERE length(s) < 3
) SELECT s FROM r ORDER BY s;

-- ============================================================================
-- 7. RECURSIVE without a self-reference is an ordinary query
-- ============================================================================

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- end-expected
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 2) SELECT n FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3)
SELECT n FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
WITH RECURSIVE r(n) AS (SELECT 1 EXCEPT SELECT 2) SELECT n FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- end-expected
WITH RECURSIVE r(n) AS (VALUES (1),(2)) SELECT n FROM r ORDER BY n;

-- SEARCH orders a recursion; there is none here
-- begin-expected-error
-- sqlstate: 42601
-- message-like: WITH query is not recursive
-- end-expected-error
WITH RECURSIVE r AS (SELECT 1 AS n) SEARCH DEPTH FIRST BY n SET ord SELECT n FROM r;

-- ============================================================================
-- 8. SEARCH and CYCLE bind to their own WITH item
-- ============================================================================

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET ord, s AS (SELECT 9 AS m)
SELECT n FROM r ORDER BY ord;

-- begin-expected
-- columns: m
-- row: 9
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH BREADTH FIRST BY n SET ord, s AS (SELECT 9 AS m)
SELECT m FROM s;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) CYCLE n SET is_cycle USING path, s AS (SELECT 9 AS m)
SELECT n FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET ord CYCLE n SET is_cycle USING path, s AS (SELECT 9 AS m)
SELECT n FROM r ORDER BY ord;

-- begin-expected
-- columns: m|k
-- row: 9|8
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET ord, s AS (SELECT 9 AS m), u AS (SELECT 8 AS k)
SELECT m, k FROM s, u;

-- begin-expected
-- columns: p|m
-- row: 7|9
-- end-expected
WITH RECURSIVE a AS (SELECT 7 AS p), r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH BREADTH FIRST BY n SET ord, s AS (SELECT 9 AS m)
SELECT p, m FROM a, s;

-- two WITH items may each carry a SEARCH clause
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET ord, q(m) AS (
  SELECT 1 UNION ALL SELECT m + 1 FROM q WHERE m < 2
) SEARCH DEPTH FIRST BY m SET ord2
SELECT n FROM r ORDER BY ord;

-- SEARCH as the last thing in the WITH clause still works
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET ord
SELECT n FROM r ORDER BY ord;

-- ============================================================================
-- 9. A plain WITH item sees only the items written before it
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "y" does not exist
-- end-expected-error
WITH x AS (SELECT n FROM y), y AS (SELECT 2 AS n) SELECT n FROM x;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "y" does not exist
-- end-expected-error
WITH x AS (SELECT (SELECT n FROM y) AS n), y AS (SELECT 2 AS n) SELECT n FROM x;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "z" does not exist
-- end-expected-error
WITH x AS (SELECT 1 AS n), y AS (SELECT n FROM z), z AS (SELECT 3 AS n) SELECT n FROM y;

-- an item does not see its own name either
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "x" does not exist
-- end-expected-error
WITH x AS (SELECT n FROM x) SELECT n FROM x;

-- a backward reference is what the clause is for
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
WITH y AS (SELECT 2 AS n), x AS (SELECT n FROM y) SELECT n FROM x;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
WITH x AS (SELECT 1 AS n), y AS (SELECT n FROM x), z AS (SELECT n FROM y) SELECT n FROM z;

-- the query itself sees every item, in any order
-- begin-expected
-- columns: a|b
-- row: 2|1
-- end-expected
WITH x AS (SELECT 1 AS n), y AS (SELECT 2 AS n)
SELECT (SELECT n FROM y) AS a, (SELECT n FROM x) AS b;

-- with RECURSIVE the whole list is visible to every item
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
WITH RECURSIVE x AS (SELECT n FROM y), y AS (SELECT 2 AS n) SELECT n FROM x;

-- a hidden WITH name falls through to the stored relation of that name
-- begin-expected
-- columns: n
-- row: 99
-- end-expected
WITH cte_t AS (SELECT 99 AS n) SELECT n FROM cte_t;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH x AS (SELECT n FROM cte_t), cte_t AS (SELECT 99 AS n) SELECT n FROM x ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH cte_t AS (SELECT n FROM cte_t) SELECT n FROM cte_t ORDER BY n;

-- an inner WITH shadows an outer one, and can still read it
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
WITH x AS (SELECT 1 AS n) SELECT n FROM (WITH x AS (SELECT 2 AS n) SELECT n FROM x) z;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
WITH x AS (SELECT 1 AS n) SELECT n FROM (WITH y AS (SELECT n FROM x) SELECT n FROM y) z;

-- two WITH items may not share a name
-- begin-expected-error
-- sqlstate: 42712
-- message-like: WITH query name "x" specified more than once
-- end-expected-error
WITH x AS (SELECT 1 AS n), x AS (SELECT 2 AS n) SELECT n FROM x;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: WITH query name "x" specified more than once
-- end-expected-error
WITH RECURSIVE x AS (SELECT 1 AS n), x AS (SELECT 2 AS n) SELECT n FROM x;

-- two items that read each other are not implemented
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: mutual recursion between WITH items is not implemented
-- end-expected-error
WITH RECURSIVE x AS (SELECT n FROM y), y AS (SELECT n FROM x) SELECT n FROM x;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: mutual recursion between WITH items is not implemented
-- end-expected-error
WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM y WHERE n < 3),
               y(n) AS (SELECT n FROM x)
SELECT n FROM x ORDER BY n;

-- ============================================================================
-- 10. Ordinary recursion is untouched
-- ============================================================================

-- begin-expected
-- columns: a|b
-- row: 1|2
-- row: 2|3
-- row: 3|4
-- end-expected
WITH RECURSIVE p(a, b) AS (
  SELECT a, b FROM cte_edge WHERE a = 1
  UNION ALL
  SELECT e.a, e.b FROM cte_edge e JOIN p ON e.a = p.b
) SELECT a, b FROM p ORDER BY a, b;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT r.n + 1 FROM r JOIN cte_t t ON t.n = r.n WHERE r.n < 3
) SELECT n FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3)
SELECT n FROM r ORDER BY n;

-- begin-expected
-- columns: s
-- row: a
-- row: aa
-- row: aaa
-- end-expected
WITH RECURSIVE r(s) AS (
  SELECT 'a'::text UNION ALL SELECT s || 'a' FROM r WHERE length(s) < 3
) SELECT s FROM r ORDER BY s;

-- DISTINCT and GROUP BY in the recursive term are allowed
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT DISTINCT n + 1 FROM r WHERE n < 3
) SELECT n FROM r ORDER BY n;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 GROUP BY n
) SELECT n FROM r ORDER BY n;

-- a second WITH item may read a recursive one
-- begin-expected
-- columns: m
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH RECURSIVE r(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
), s(m) AS (SELECT n FROM r) SELECT m FROM s ORDER BY m;

-- two independent recursions in one clause
-- begin-expected
-- columns: n|m
-- row: 1|1
-- row: 1|2
-- row: 2|1
-- row: 2|2
-- row: 3|1
-- row: 3|2
-- end-expected
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3),
               q(m) AS (SELECT 1 UNION ALL SELECT m + 1 FROM q WHERE m < 2)
SELECT r.n, q.m FROM r, q ORDER BY 1, 2;

-- a plain WITH clause with two items joined together
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
WITH a AS (SELECT n FROM cte_t), b AS (SELECT n FROM cte_t)
SELECT a.n FROM a JOIN b ON a.n = b.n ORDER BY 1;

-- and LIMIT inside a plain WITH item
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- end-expected
WITH x AS (SELECT n FROM cte_t ORDER BY n LIMIT 2) SELECT n FROM x ORDER BY n;

DROP TABLE cte_edge;
DROP TABLE cte_t;
