-- What a join's names resolve to, and which joins may be asked at all.
--
-- Five things, all measured against PostgreSQL 18.
--
-- 1. An outer join names the side that contributed nothing. The whole purpose of
--    t1 RIGHT JOIN t2 is to answer with NULLs where t1 has no match, and those rows still have to
--    answer to t1's name. The shape of a side was taken from its first row, which works only while
--    it has one; when the side was empty the padded rows carried no binding for it and every
--    reference to its alias was 42P01 missing FROM-clause entry. It takes a side that produces no
--    rows at all -- an empty relation, a subquery or view that filters everything away, a WITH
--    query -- which is why the two queries in the original finding already worked.
--
-- 2. A FULL JOIN may only be asked what PostgreSQL can answer. It has no nested-loop plan for one:
--    it must merge or hash the two sides, and both need an equality between one side's value and
--    the other's. Anything else is refused where the query is planned. What it accepts was
--    measured across forty conditions rather than guessed: one AND-ed clause being a cross-side
--    equality is enough however unmergeable the rest are, a condition that folds to a constant is
--    always fine, and a WHERE that reads either side lifts the restriction entirely because the
--    join is then planned as an inner one. A view body is only analysed and not planned, so a view
--    over such a join is still created -- and fails when it is read.
--
-- 3. A NATURAL join merges the columns both sides share, exactly as a USING clause naming them
--    would. Only the USING half was known to the ambiguity check. The merges are counted rather
--    than remembered, so a relation joined on afterwards makes the name ambiguous again.
--
-- 4. A schema may be written in front of a column's relation. public.t.c resolves against an
--    unaliased FROM t -- but only when the schema really holds that relation: a WITH query, a
--    subquery alias and a relation of another schema are an invalid reference, not a missing one.
--
-- 5. A duplicate FROM name is refused wherever it is written, including in a view body, which the
--    view path swallowed. The one exemption SQL grants -- two relations of the same name from
--    different schemas, both written without an alias -- has to survive that.
--
-- The last section is ordinary SQL, which has to keep working: the cost of a rule that reaches too
-- far is a refused valid statement.

-- setup
DROP VIEW IF EXISTS jr_empty_v CASCADE;
DROP VIEW IF EXISTS jr_v CASCADE;
DROP TABLE IF EXISTS jr_t1 CASCADE;
DROP TABLE IF EXISTS jr_t2 CASCADE;
DROP TABLE IF EXISTS jr_t3 CASCADE;
DROP TABLE IF EXISTS jr_e1 CASCADE;
DROP TABLE IF EXISTS jr_e2 CASCADE;
DROP TABLE IF EXISTS jr_m1 CASCADE;
DROP TABLE IF EXISTS jr_m2 CASCADE;
DROP SCHEMA IF EXISTS jr_s CASCADE;

CREATE TABLE jr_t1 (i int, s text);
CREATE TABLE jr_t2 (j int, s text);
CREATE TABLE jr_t3 (k int, s text);
CREATE TABLE jr_e1 (i int);
CREATE TABLE jr_e2 (j int);
CREATE TABLE jr_m1 (i int, s text);
CREATE TABLE jr_m2 (j int, s text);
INSERT INTO jr_t1 VALUES (1,'a'),(2,'b'),(3,'c');
INSERT INTO jr_t2 VALUES (2,'x'),(3,'y'),(4,'z');
INSERT INTO jr_t3 VALUES (3,'p'),(5,'q');
INSERT INTO jr_m1 VALUES (1,'a'),(2,'b');
INSERT INTO jr_m2 VALUES (5,'a'),(6,'c');
CREATE VIEW jr_empty_v AS SELECT i, s FROM jr_t1 WHERE false;
CREATE VIEW jr_v AS SELECT i, s FROM jr_t1;
CREATE SCHEMA jr_s;
CREATE TABLE jr_s.jr_t1 (i int, z text);
INSERT INTO jr_s.jr_t1 VALUES (7,'s');

-- ============================================================================
-- 1. An outer join names the side that contributed nothing
-- ============================================================================

-- note: the left relation is empty, so every row is padded and every a.i is NULL
-- begin-expected
-- columns: i | j
-- row: NULL, 2
-- row: NULL, 3
-- row: NULL, 4
-- end-expected
SELECT a.i, b.j FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j ORDER BY b.j;

-- note: with rows on both sides the padding was already right, which is why the finding's own
-- note: two queries passed before anything was changed
-- begin-expected
-- columns: i | j
-- row: 2, 2
-- row: 3, 3
-- row: NULL, 4
-- row: 1, NULL
-- end-expected
SELECT a.i, b.j FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = b.j ORDER BY b.j, a.i;

-- begin-expected
-- columns: text | j
-- row: true, 2
-- row: true, 3
-- row: true, 4
-- end-expected
SELECT (a.i IS NULL)::text, b.j FROM jr_t1 a RIGHT JOIN jr_t2 b ON false ORDER BY b.j;

-- note: both sides empty is no rows, not an error
-- begin-expected
-- columns: i | j
-- end-expected
SELECT a.i, b.j FROM jr_e1 a FULL JOIN jr_e2 b ON a.i = b.j;

-- note: a chain of outer joins keeps every alias down the chain
-- begin-expected
-- columns: i | j | k
-- row: NULL, 3, 3
-- row: NULL, NULL, 5
-- end-expected
SELECT a.i, b.j, c.k FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j
    RIGHT JOIN jr_t3 c ON b.j = c.k ORDER BY 1, 2, 3;

-- note: an empty subquery, view and WITH query each still answer to their name
-- begin-expected
-- columns: i | j
-- row: NULL, 2
-- row: NULL, 3
-- row: NULL, 4
-- end-expected
SELECT a.i, b.j FROM (SELECT i FROM jr_t1 WHERE false) a FULL JOIN jr_t2 b ON a.i = b.j
    ORDER BY b.j;

-- begin-expected
-- columns: i | j
-- row: NULL, 2
-- row: NULL, 3
-- row: NULL, 4
-- end-expected
SELECT a.i, b.j FROM jr_empty_v a FULL JOIN jr_t2 b ON a.i = b.j ORDER BY b.j;

-- begin-expected
-- columns: i | j
-- row: NULL, 2
-- row: NULL, 3
-- row: NULL, 4
-- end-expected
WITH c AS (SELECT i FROM jr_t1 WHERE false)
SELECT c.i, b.j FROM c FULL JOIN jr_t2 b ON c.i = b.j ORDER BY b.j;

-- note: a LATERAL over an empty side is describable and empty rather than unresolvable
-- begin-expected
-- columns: i | j
-- end-expected
SELECT a.i, b.j FROM jr_e1 a LEFT JOIN LATERAL (SELECT j FROM jr_t2 WHERE j = a.i) b ON true
    ORDER BY 1;

-- note: a self-join of an empty relation names both of its aliases
-- begin-expected
-- columns: i | i
-- end-expected
SELECT a.i, b.i FROM jr_e1 a FULL JOIN jr_e1 b ON a.i = b.i;

-- note: the alias resolves in WHERE
-- begin-expected
-- columns: j
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT b.j FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j WHERE a.i IS NULL ORDER BY b.j;

-- note: in GROUP BY
-- begin-expected
-- columns: i | count
-- row: NULL, 3
-- end-expected
SELECT a.i, count(*) FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j GROUP BY a.i ORDER BY 1;

-- note: in HAVING
-- begin-expected
-- columns: j
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT b.j FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j GROUP BY b.j HAVING count(a.i) = 0
    ORDER BY 1;

-- note: in ORDER BY alone
-- begin-expected
-- columns: j
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT b.j FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j ORDER BY a.i, b.j;

-- note: inside a subquery
-- begin-expected
-- columns: j | count
-- row: 2, 0
-- row: 3, 0
-- row: 4, 0
-- end-expected
SELECT b.j, (SELECT count(*) FROM jr_t3 c WHERE c.k = a.i)
    FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j ORDER BY b.j;

-- note: and in a window partition
-- begin-expected
-- columns: j | count
-- row: 2, 3
-- row: 3, 3
-- row: 4, 3
-- end-expected
SELECT b.j, count(*) OVER (PARTITION BY a.i) FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j
    ORDER BY b.j;

-- note: a name no arm of the join answers to is still missing
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "c"
-- end-expected-error
SELECT c.i FROM jr_t1 a RIGHT JOIN jr_t2 b ON a.i = b.j;

-- note: and a column the aliased relation has not is still 42703, named in full
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column a.nosuch does not exist
-- end-expected-error
SELECT a.nosuch FROM jr_t1 a RIGHT JOIN jr_t2 b ON a.i = b.j;

-- ============================================================================
-- 2. Which conditions a FULL JOIN may carry
-- ============================================================================

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i < b.j;

-- note: an OR of two equalities is not an equality
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = b.j OR a.s = b.s;

-- note: an equality naming only one side is no use for merging or hashing either
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = 1;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i IS NOT DISTINCT FROM b.j;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i <> b.j;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON NOT (a.i = b.j);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON (a.i = b.j) IS TRUE;

-- note: an equality one of whose sides reads both relations cannot be split across them
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON (a.i + b.j) = 5;

-- note: a constant that folds to true disappears and does not carry the clause beside it
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON 1=1 AND a.i < b.j;

-- note: each join in a chain is judged on its own condition
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = b.j FULL JOIN jr_t3 c ON b.j < c.k;

-- note: a WITH query is a side like any other
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
WITH q AS (SELECT i FROM jr_t1) SELECT count(*) FROM q FULL JOIN jr_t2 b ON q.i < b.j;

-- note: the refusal comes from planning the query, so no LIMIT can get past it
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i < b.j LIMIT 0;

-- note: a plain equality, and an equality between expressions, are both fine
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = b.j;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON coalesce(a.i,0) = coalesce(b.j,0);

-- note: one equality carries any number of unmergeable clauses beside it
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = b.j AND a.i > 1 AND a.s < b.s;

-- note: a condition that folds to a constant is always allowed
-- begin-expected
-- columns: count
-- row: 9
-- end-expected
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON 1=1;

-- begin-expected
-- columns: count
-- row: 6
-- end-expected
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON false AND a.i < b.j;

-- begin-expected
-- columns: count
-- row: 6
-- end-expected
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON NULL AND a.i < b.j;

-- note: an alternative that always holds makes the whole condition true
-- begin-expected
-- columns: count
-- row: 9
-- end-expected
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i < b.j OR true;

-- note: an alternative that never holds drops out and leaves the equality standing
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = b.j OR false;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = b.j OR NULL;

-- note: USING and NATURAL join on equality by construction
-- begin-expected
-- columns: count
-- row: 6
-- end-expected
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b USING (s);

-- begin-expected
-- columns: count
-- row: 6
-- end-expected
SELECT count(*) FROM jr_t1 a NATURAL FULL JOIN jr_t2 b;

-- note: a WHERE that reads either side discards the padded rows, and the join is then planned as
-- note: an inner one, which asks nothing of its condition
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i > b.j WHERE a.i = b.j;

-- note: only a full join is restricted; the other three have a nested-loop plan
-- begin-expected
-- columns: count
-- row: 6
-- end-expected
SELECT count(*) FROM jr_t1 a LEFT JOIN jr_t2 b ON a.i < b.j;

-- begin-expected
-- columns: count
-- row: 6
-- end-expected
SELECT count(*) FROM jr_t1 a RIGHT JOIN jr_t2 b ON a.i < b.j;

-- begin-expected
-- columns: count
-- row: 6
-- end-expected
SELECT count(*) FROM jr_t1 a JOIN jr_t2 b ON a.i < b.j;

-- note: a name resolved before the join is planned is reported before it too
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "s" is ambiguous
-- end-expected-error
SELECT count(*) FROM jr_t1 FULL JOIN jr_t2 ON s = s;

-- ============================================================================
-- 3. A NATURAL join merges the columns both sides share
-- ============================================================================

-- begin-expected
-- columns: s
-- row: a
-- end-expected
SELECT s FROM jr_m1 NATURAL JOIN jr_m2 ORDER BY 1;

-- begin-expected
-- columns: s
-- row: a
-- end-expected
SELECT s FROM jr_m1 NATURAL JOIN jr_m2 WHERE s = 'a';

-- begin-expected
-- columns: s
-- row: a
-- end-expected
SELECT s FROM jr_m1 NATURAL JOIN jr_m2 GROUP BY s ORDER BY 1;

-- begin-expected
-- columns: s
-- row: a
-- end-expected
SELECT s FROM jr_m1 NATURAL JOIN jr_m2 ORDER BY s;

-- note: the merge holds when the join answers with no rows at all
-- begin-expected
-- columns: s
-- end-expected
SELECT s FROM jr_t1 NATURAL JOIN jr_t2;

-- begin-expected
-- columns: s
-- end-expected
SELECT s FROM jr_t1 NATURAL JOIN jr_t2 WHERE s = 'x';

-- note: every outer flavour merges the same way
-- begin-expected
-- columns: s
-- row: a
-- row: b
-- end-expected
SELECT s FROM jr_m1 NATURAL LEFT JOIN jr_m2 ORDER BY 1;

-- begin-expected
-- columns: s
-- row: a
-- row: c
-- end-expected
SELECT s FROM jr_m1 NATURAL RIGHT JOIN jr_m2 ORDER BY 1;

-- begin-expected
-- columns: s
-- row: a
-- row: b
-- row: c
-- end-expected
SELECT s FROM jr_m1 NATURAL FULL JOIN jr_m2 ORDER BY 1;

-- note: chained natural joins merge once each
-- begin-expected
-- columns: s
-- end-expected
SELECT s FROM jr_t1 NATURAL JOIN jr_t2 NATURAL JOIN jr_t3;

-- note: a merged column is still readable through either relation it came from
-- begin-expected
-- columns: s | s
-- row: a, a
-- end-expected
SELECT jr_m1.s, jr_m2.s FROM jr_m1 NATURAL JOIN jr_m2;

-- begin-expected
-- columns: s
-- end-expected
SELECT jr_t2.s FROM jr_t1 JOIN jr_t2 USING (s);

-- note: a relation joined on afterwards was merged into nothing, so the name is ambiguous again
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "s" is ambiguous
-- end-expected-error
SELECT s FROM jr_t1 NATURAL JOIN jr_t2 JOIN jr_t3 ON true;

-- note: and a shared name no join merged is ambiguous as it always was
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "s" is ambiguous
-- end-expected-error
SELECT s FROM jr_t1 JOIN jr_t2 ON jr_t1.i = jr_t2.j;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "s" is ambiguous
-- end-expected-error
SELECT s FROM jr_t1, jr_t2;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "s" is ambiguous
-- end-expected-error
SELECT s FROM jr_t1 CROSS JOIN jr_t2;

-- ============================================================================
-- 4. A schema written in front of a column's relation
-- ============================================================================

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT public.jr_t1.i FROM jr_t1 ORDER BY 1;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT public.jr_t1.i FROM public.jr_t1 ORDER BY 1;

-- begin-expected
-- columns: i
-- row: 7
-- end-expected
SELECT jr_s.jr_t1.i FROM jr_s.jr_t1;

-- note: it resolves in WHERE and ORDER BY
-- begin-expected
-- columns: i
-- row: 2
-- row: 3
-- end-expected
SELECT public.jr_t1.i FROM jr_t1 WHERE public.jr_t1.i > 1 ORDER BY public.jr_t1.i;

-- note: under an aggregate
-- begin-expected
-- columns: sum
-- row: 6
-- end-expected
SELECT sum(public.jr_t1.i) FROM jr_t1;

-- note: in GROUP BY and HAVING
-- begin-expected
-- columns: i
-- row: 2
-- row: 3
-- end-expected
SELECT public.jr_t1.i FROM jr_t1 GROUP BY public.jr_t1.i HAVING public.jr_t1.i > 1 ORDER BY 1;

-- note: and in a join condition
-- begin-expected
-- columns: i
-- row: 2
-- row: 3
-- end-expected
SELECT public.jr_t1.i FROM jr_t1 JOIN jr_t2 ON public.jr_t1.i = jr_t2.j ORDER BY 1;

-- note: a view is a relation a schema holds too
-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT public.jr_v.i FROM jr_v ORDER BY 1;

-- note: two schemas holding one name are each reachable by writing the schema
-- begin-expected
-- columns: i | i
-- row: 1, 7
-- row: 2, 7
-- row: 3, 7
-- end-expected
SELECT public.jr_t1.i, jr_s.jr_t1.i FROM jr_t1, jr_s.jr_t1 ORDER BY 1;

-- note: a star may be schema-qualified as well
-- begin-expected
-- columns: i | s
-- row: 1, a
-- row: 2, b
-- row: 3, c
-- end-expected
SELECT public.jr_t1.* FROM jr_t1 ORDER BY 1;

-- note: the wrong schema is an invalid reference rather than a missing one
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "jr_t1"
-- end-expected-error
SELECT public.jr_t1.i FROM jr_s.jr_t1;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "jr_v"
-- end-expected-error
SELECT jr_s.jr_v.i FROM jr_v;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "jr_t1"
-- end-expected-error
SELECT nosuch.jr_t1.i FROM jr_t1;

-- note: a WITH query lives in the query, not in a schema
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "jr_t1"
-- end-expected-error
WITH jr_t1 AS (SELECT 9 AS i) SELECT public.jr_t1.i FROM jr_t1;

-- note: and neither does a subquery alias
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "s"
-- end-expected-error
SELECT public.s.i FROM (SELECT 1 i) s;

-- note: an alias still hides the relation's own name, schema or no schema
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "jr_t1"
-- end-expected-error
SELECT public.jr_t1.i FROM jr_t1 a;

-- note: a name no FROM entry answers to is missing rather than invalid
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "jr_nosuch"
-- end-expected-error
SELECT public.jr_nosuch.i FROM jr_t1;

-- ============================================================================
-- 5. A duplicate FROM name, wherever it is written
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "x" specified more than once
-- end-expected-error
SELECT * FROM jr_t1 x JOIN jr_t2 x ON true;

-- note: a view body is checked for one too
-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "x" specified more than once
-- end-expected-error
CREATE VIEW jr_dupv AS SELECT 1 AS z FROM jr_t1 x JOIN jr_t2 x ON true;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "jr_t1" specified more than once
-- end-expected-error
SELECT 1 AS c FROM jr_t1, jr_t1;

-- note: a subquery has only the name it is given, so sharing it is a clash
-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "jr_t1" specified more than once
-- end-expected-error
SELECT 1 AS c FROM jr_t1, (SELECT 1) jr_t1;

-- note: the one exemption: two relations of one name from different schemas, neither aliased,
-- note: because either can still be reached by writing its schema
-- begin-expected
-- columns: c
-- row: 1
-- row: 1
-- row: 1
-- end-expected
SELECT 1 AS c FROM public.jr_t1, jr_s.jr_t1;

-- begin-expected
-- columns: c
-- row: 1
-- row: 1
-- row: 1
-- end-expected
SELECT 1 AS c FROM jr_t1 JOIN jr_s.jr_t1 ON true;

CREATE VIEW jr_okv AS SELECT 1 AS z FROM public.jr_t1, jr_s.jr_t1;
DROP VIEW jr_okv;

-- note: give either of them an alias and the name is the only way to reach it again
-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "q" specified more than once
-- end-expected-error
SELECT 1 AS c FROM public.jr_t1 q, jr_s.jr_t1 q;

-- ============================================================================
-- 6. Ordinary join SQL, which has to keep working
-- ============================================================================

-- begin-expected
-- columns: i | j
-- row: 2, 2
-- row: 3, 3
-- end-expected
SELECT a.i, b.j FROM jr_t1 a JOIN jr_t2 b ON a.i = b.j ORDER BY 1;

-- begin-expected
-- columns: i | j
-- row: 1, NULL
-- row: 2, 2
-- row: 3, 3
-- end-expected
SELECT a.i, b.j FROM jr_t1 a LEFT JOIN jr_t2 b ON a.i = b.j ORDER BY 1;

-- begin-expected
-- columns: count
-- row: 9
-- end-expected
SELECT count(*) FROM jr_t1 a CROSS JOIN jr_t2 b;

-- begin-expected
-- columns: s
-- end-expected
SELECT s FROM jr_t1 JOIN jr_t2 USING (s);

-- begin-expected
-- columns: i | j
-- row: 1, NULL
-- row: 2, 2
-- row: 3, 3
-- end-expected
SELECT a.i, x.j FROM jr_t1 a LEFT JOIN LATERAL (SELECT j FROM jr_t2 WHERE jr_t2.j = a.i) x
    ON true ORDER BY 1;

-- begin-expected
-- columns: i
-- row: 2
-- row: 3
-- end-expected
SELECT a.i FROM jr_t1 a WHERE EXISTS (SELECT 1 FROM jr_t2 b WHERE b.j = a.i) ORDER BY 1;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT a.i FROM jr_t1 a LEFT JOIN jr_t2 b ON a.i = b.j ORDER BY 1;

-- begin-expected
-- columns: count | count
-- row: 3, 3
-- end-expected
SELECT count(a.i), count(b.j) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = b.j;

-- begin-expected
-- columns: i
-- row: 1
-- row: 1
-- row: 1
-- row: 2
-- row: 2
-- row: 2
-- row: 3
-- row: 3
-- row: 3
-- end-expected
SELECT z.i FROM jr_t1 a RIGHT JOIN jr_t2 b ON a.i = b.j, jr_t1 z ORDER BY 1;

-- cleanup
DROP VIEW IF EXISTS jr_empty_v CASCADE;
DROP VIEW IF EXISTS jr_v CASCADE;
DROP TABLE IF EXISTS jr_t1 CASCADE;
DROP TABLE IF EXISTS jr_t2 CASCADE;
DROP TABLE IF EXISTS jr_t3 CASCADE;
DROP TABLE IF EXISTS jr_e1 CASCADE;
DROP TABLE IF EXISTS jr_e2 CASCADE;
DROP TABLE IF EXISTS jr_m1 CASCADE;
DROP TABLE IF EXISTS jr_m2 CASCADE;
DROP SCHEMA IF EXISTS jr_s CASCADE;
