-- Which FULL JOINs PostgreSQL refuses, decided the way PostgreSQL decides it.
--
-- PostgreSQL has no nested-loop plan for a full join: it must merge or hash the two sides, and both
-- need an equality between one side's value and the other's, so a condition offering neither is
-- 0A000 FULL JOIN is only supported with merge-joinable or hash-joinable join conditions. Memgres
-- reproduces a limitation it does not have, so that an application cannot tell the two engines
-- apart -- which makes a refusal PostgreSQL would not have raised pure loss, and the rule is
-- written to accept whatever it cannot read.
--
-- 1. PostgreSQL does not read the condition as written. eval_const_expressions runs first: NOT is
--    eliminated by replacing an operator with its negator and by de Morgan, so NOT (a <> b) is an
--    equality; x = true folds to x; a cast that changes nothing disappears; a CASE whose conditions
--    are constant collapses to the arm that holds. prepqual then factors an OR whose arms share a
--    clause, so (A AND B) OR (A AND C) becomes A AND (B OR C) and A OR A becomes A. A single-element
--    IN never reaches the planner as one: the parser writes it as an equality, and a list whose
--    members are not all constants as an OR of them. Measured in both directions -- NOT (a = b),
--    (a = b) IS TRUE, a IN (b, 1) and a = (SELECT 1) are still refused.
--
-- 2. A full join above which a strict qual sits is not a full join. reduce_outer_joins downgrades
--    it to a left, right or inner join when a qual rejects the rows one side was padded with, and
--    the downgraded join is never asked the question. The qual may be in WHERE, in a HAVING clause
--    with no aggregate in it, or in the ON condition of an inner join above. Only a strict qual
--    counts: WHERE a.x IS NOT NULL downgrades the join and WHERE a.x IS NULL does not. It is
--    relations and not columns that a qual proves non-null, so an OR proves what every arm proves.
--
-- 2a. Only the query the client sent is judged: the qual that rescues a join may be a long way
--    from it, reaching down through a pulled-up subquery or a pushed-down qual, and deciding which
--    is reimplementing the optimiser. A join inside a subquery, a WITH query, a view, an arm of a
--    set operation, the source of a writing statement or a function body is therefore accepted
--    unread, which is a handful of statements PostgreSQL declines to plan and memgres answers.
--    Those are marked expected-divergence below; full-join-narrowing.sql has the whole argument.
--
-- 3. Analysis comes before planning. A view's body is analysed when the view is defined, so a view
--    over a refused join is stored and fails when it is read -- and the complaints that belong to
--    analysis, a column name the view would answer to twice among them, have to be raised at
--    definition time rather than short-circuited by the planner's refusal. A name that does not
--    resolve is reported before anything is planned too.
--
-- The last section is ordinary SQL, which has to keep working: the cost of a rule that reaches too
-- far is a refused valid statement.

-- setup
DROP VIEW IF EXISTS fja_fv CASCADE;
DROP TABLE IF EXISTS fja_a CASCADE;
DROP TABLE IF EXISTS fja_b CASCADE;
DROP TABLE IF EXISTS fja_c CASCADE;
DROP TABLE IF EXISTS fja_p CASCADE;
DROP TABLE IF EXISTS fja_q CASCADE;

CREATE TABLE fja_a (x int, t text);
CREATE TABLE fja_b (y int, t2 text);
CREATE TABLE fja_c (z int, t3 text);
CREATE TABLE fja_p (id int, av text);
CREATE TABLE fja_q (id int, bv text);
INSERT INTO fja_a VALUES (1,'a'),(2,'b'),(3,'c');
INSERT INTO fja_b VALUES (1,'a'),(9,'z');
INSERT INTO fja_c VALUES (1,'a'),(2,'b');
CREATE VIEW fja_fv AS SELECT a.x, b.y FROM fja_a a FULL JOIN fja_b b ON a.x < b.y;

-- ============================================================================
-- 1. The condition is normalised before it is judged
-- ============================================================================

-- note: NOT over an operator becomes that operator's negator, so this is an equality
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON NOT (a.x <> b.y);

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON NOT (a.x <> b.y) AND a.x > 0;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON NOT NOT (a.x = b.y);

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON NOT (NOT (NOT (a.x <> b.y)));

-- note: de Morgan, and the two inequalities under it become equalities
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON NOT (a.x <> b.y OR a.t <> b.t2);

-- note: an odd number of NOTs leaves an inequality
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON NOT NOT NOT (a.x = b.y);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON NOT (a.x = b.y);

-- note: de Morgan turns this into an OR of two equalities, which is still not one
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON NOT (a.x <> b.y AND a.t <> b.t2);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON NOT (a.x IS DISTINCT FROM b.y);

-- note: comparing a boolean with a true constant is the boolean itself
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON (a.x = b.y) = true;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON true = (a.x = b.y);

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON (a.x = b.y) <> false;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON (a.x = b.y) = (1=1);

-- note: with a false constant it is the negation, which is an inequality
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON (a.x = b.y) = false;

-- note: a boolean test is not a boolean equality and is not simplified away
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON (a.x = b.y) IS TRUE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON (a.x = b.y) IS NOT NULL;

-- note: a cast from boolean to boolean changes nothing and disappears
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON (a.x = b.y)::boolean;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON (a.x = b.y)::bool = true;

-- note: a single-element IN is written by the parser as an equality
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x IN (b.y);

-- note: a longer list becomes an OR of equalities, whose arms here are the same one
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x IN (b.y, b.y);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x IN (b.y, 1);

-- note: a list that is all constants becomes an array comparison, never a join clause
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x IN (1,2);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x NOT IN (b.y);

-- note: a subquery over one side keeps the clause a join clause of that side
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = (SELECT b.y);

-- note: one over neither side leaves an equality naming only the left relation
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = (SELECT 1);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = (SELECT max(y) FROM fja_b);

-- note: a clause every arm of an OR carries is lifted out of it
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = b.y OR a.x = b.y;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = b.y OR (a.x = b.y AND a.t = b.t2);

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON (a.x = b.y AND a.x > 0) OR (a.x = b.y AND b.y > 0);

-- begin-expected
-- columns: count
-- row: 5
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON ((a.x = b.y AND a.x > 0) OR (a.x = b.y AND b.y > 0)) AND a.t < b.t2;

-- note: factoring an inequality out of an OR still leaves an inequality
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON (a.x < b.y AND a.x > 0) OR (a.x < b.y AND b.y > 0);

-- note: arms with nothing in common stay an OR
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = b.y OR a.t = b.t2;

-- note: an operator may be written with the schema it lives in
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x operator(pg_catalog.=) b.y;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x OPERATOR(pg_catalog.<) b.y;

-- note: a CASE whose conditions are constant collapses to the arm that holds
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON CASE WHEN true THEN a.x = b.y ELSE false END;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON CASE WHEN a.x > 0 THEN a.x = b.y ELSE false END;

-- ============================================================================
-- 2. A strict qual above the join makes it an inner one
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE a.x IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE NOT (a.x IS NULL);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE upper(a.t) = 'A';

-- note: a test for null rejects nothing, so the join is still a full one
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE a.x IS NULL;

-- note: coalesce answers with a value of its own where its argument is null
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE coalesce(a.x,0) >= 0;

-- note: an OR only proves what both of its arms prove
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE a.x IS NOT NULL OR b.y IS NOT NULL;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE true;

-- note: a WHERE that can never hold leaves a plan with no join in it
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE false;

-- note: a HAVING clause with no aggregate in it is a WHERE clause
-- begin-expected
-- columns: count
-- row: 1
-- row: 1
-- row: 1
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y GROUP BY a.x HAVING a.x IS NOT NULL;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y GROUP BY a.x HAVING count(*) > 0;

-- note: an inner join above filters both of its arms
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y JOIN fja_c c ON c.z = a.x;

-- note: an outer join above only filters the arm it may pad away
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM fja_c c LEFT JOIN (fja_a a FULL JOIN fja_b b ON a.x < b.y) ON c.z = a.x;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y LEFT JOIN fja_c c ON c.z = a.x;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y CROSS JOIN fja_c c;

-- note: a qual of the query above reaches into a subquery it pulls up
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM (SELECT * FROM fja_a a FULL JOIN fja_b b ON a.x < b.y) s WHERE s.x IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM (SELECT a.x + 0 AS xx FROM fja_a a FULL JOIN fja_b b ON a.x < b.y) s WHERE s.xx IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
WITH s AS (SELECT * FROM fja_a a FULL JOIN fja_b b ON a.x < b.y) SELECT count(*) FROM s WHERE s.x IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM fja_fv WHERE x IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM fja_fv v JOIN fja_c c ON c.z = v.x;

-- note: a join inside a subquery, a WITH query or a view is not judged at all -- see
-- full-join-narrowing.sql for why. PostgreSQL refuses each of these three; memgres answers them.
-- expected-divergence: a join not in the outermost query is accepted rather than judged
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM (SELECT * FROM fja_a a FULL JOIN fja_b b ON a.x < b.y LIMIT 10) s WHERE s.x IS NOT NULL;

-- expected-divergence: a join not in the outermost query is accepted rather than judged
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_fv;

-- expected-divergence: a join not in the outermost query is accepted rather than judged
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT (SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y);

-- note: the refusal comes from planning the query, so no LIMIT can get past it
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y LIMIT 0;

-- ============================================================================
-- 3. What is reported before the plan is made
-- ============================================================================

-- note: the range table is built before anything is planned
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "c"
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE c.z = 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE nosuchcol = 1;

-- note: a view's body is analysed and not planned, so the view is stored
CREATE VIEW fja_v1 AS SELECT a.x, b.y FROM fja_a a FULL JOIN fja_b b ON a.x > b.y;

-- expected-divergence: a join not in the outermost query is accepted rather than judged
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_v1;

DROP VIEW fja_v1;

-- note: a name the view would answer to twice is an analysis error, and comes first
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "id" specified more than once
-- end-expected-error
CREATE VIEW fja_v2 AS SELECT * FROM fja_p a FULL JOIN fja_q b ON a.id > b.id;

-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "id" specified more than once
-- end-expected-error
CREATE VIEW fja_v2 AS SELECT * FROM fja_p a FULL JOIN fja_q b ON a.id = b.id;

-- note: a materialized view is filled when it is created, so PostgreSQL plans its query then and
-- refuses it; memgres does not judge a body it is only storing
-- expected-divergence: a join not in the outermost query is accepted rather than judged
CREATE MATERIALIZED VIEW fja_mv AS SELECT a.x, b.y FROM fja_a a FULL JOIN fja_b b ON a.x > b.y;
DROP MATERIALIZED VIEW fja_mv;

CREATE MATERIALIZED VIEW fja_mv AS SELECT a.x, b.y FROM fja_a a FULL JOIN fja_b b ON a.x > b.y WITH NO DATA;
DROP MATERIALIZED VIEW fja_mv;

-- note: the source of a writing statement is not the outermost query either
CREATE TABLE fja_w (z int, t3 text);

-- expected-divergence: a join not in the outermost query is accepted rather than judged
INSERT INTO fja_w SELECT a.x, b.t2 FROM fja_a a FULL JOIN fja_b b ON a.x < b.y;

DROP TABLE fja_w;

-- ============================================================================
-- 4. Ordinary SQL, which has to keep working
-- ============================================================================

-- begin-expected
-- columns: x | y
-- row: 1, 1
-- row: 2, NULL
-- row: 3, NULL
-- row: NULL, 9
-- end-expected
SELECT a.x, b.y FROM fja_a a FULL JOIN fja_b b ON a.x = b.y ORDER BY 1,2;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON coalesce(a.x,0) = coalesce(b.y,0);

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON (a.x, a.t) = (b.y, b.t2);

-- begin-expected
-- columns: count
-- row: 6
-- end-expected
SELECT count(*) FROM fja_a a NATURAL FULL JOIN fja_b b;

-- note: a mergeable full join answers whatever is asked of it
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = b.y WHERE a.x IS NULL;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = b.y WHERE coalesce(a.x,0) >= 0;

-- begin-expected
-- columns: count
-- row: 5
-- end-expected
SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = b.y FULL JOIN fja_c c ON b.y = c.z;

-- note: only a full join is restricted
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM fja_a a LEFT JOIN fja_b b ON a.x < b.y;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a RIGHT JOIN fja_b b ON a.x < b.y;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM fja_a a JOIN fja_b b ON a.x < b.y JOIN fja_c c ON c.z = a.x;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM fja_a a LEFT JOIN fja_b b ON a.x < b.y LEFT JOIN fja_c c ON c.z = b.y;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM fja_a a, fja_b b WHERE a.x = b.y;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fja_a a JOIN LATERAL (SELECT b.y FROM fja_b b WHERE b.y >= a.x) s ON true;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM fja_a a LEFT JOIN LATERAL (SELECT b.y FROM fja_b b WHERE b.y = a.x) s ON true;

-- note: the WHERE of a writing statement is a qual above its FROM clause
UPDATE fja_c SET t3 = t3 FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE fja_c.z = a.x;
DELETE FROM fja_c USING fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE fja_c.z = a.x AND fja_c.z = 99;

-- teardown
DROP VIEW IF EXISTS fja_fv CASCADE;
DROP TABLE IF EXISTS fja_a CASCADE;
DROP TABLE IF EXISTS fja_b CASCADE;
DROP TABLE IF EXISTS fja_c CASCADE;
DROP TABLE IF EXISTS fja_p CASCADE;
DROP TABLE IF EXISTS fja_q CASCADE;
