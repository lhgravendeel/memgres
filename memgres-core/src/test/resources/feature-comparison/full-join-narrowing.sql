-- Where the FULL JOIN restriction is asked, and where it is not asked at all.
--
-- PostgreSQL refuses a full join whose condition it can neither merge nor hash, and memgres
-- reproduces that refusal so an application cannot tell the two engines apart. But the refusal is
-- a limitation of PostgreSQL's planner, not a rule about what the query means: memgres can compute
-- any full join it is asked for. A refusal PostgreSQL would not have raised is therefore pure loss,
-- while an acceptance PostgreSQL would not have granted costs only that memgres answers a query
-- PostgreSQL declines to plan.
--
-- Two rounds of trying to decide the question everywhere got that trade the wrong way round. The
-- condition is judged after reduce_outer_joins has run, and reduce_outer_joins works on whatever
-- qual has arrived above the join by then -- which may have been written a long way from it.
-- pull_up_subqueries lifts a simple subquery into the query reading it; where it cannot,
-- subquery_is_pushdown_safe pushes the qual down instead, and it declines to do that only for
-- LIMIT and OFFSET. Reading DISTINCT, GROUP BY, HAVING, window definitions and aggregate targets as
-- barriers -- which they are to pull-up and are not to push-down -- refused thirty ordinary
-- reporting queries: a non-equi full join inside a grouped or DISTINCT subquery, CTE or view, with
-- an IS NOT NULL or equality filter outside it.
--
-- So the question is asked in one place: the query the client sent, when the join stands in that
-- query's own FROM clause and that query's own WHERE, HAVING and enclosing ON conditions do not
-- reduce it. As soon as the join sits inside a subquery, a WITH query, a view, an arm of a set
-- operation, the source of a writing statement or a function body, it is accepted unread, because
-- something above it may rescue it and nothing here can tell which.
--
-- Section 1 is what the rule still refuses, measured against PostgreSQL 18. Section 2 is what a
-- qual of that one query proves, counted over relations the way find_nonnullable_rels counts it.
-- Section 3 is the ordinary reporting SQL that two rounds of this refused and that must never be
-- refused again. Section 4 is the reach given up, marked expected-divergence: PostgreSQL refuses
-- each of those and memgres answers it.

-- setup
DROP VIEW IF EXISTS fjn_vv CASCADE;
DROP TABLE IF EXISTS fjn_a CASCADE;
DROP TABLE IF EXISTS fjn_b CASCADE;
DROP TABLE IF EXISTS fjn_c CASCADE;
DROP TABLE IF EXISTS fjn_tgt CASCADE;

CREATE TABLE fjn_a (x int, t text, n numeric);
CREATE TABLE fjn_b (y int, t text, n numeric);
CREATE TABLE fjn_c (z int PRIMARY KEY);
CREATE TABLE fjn_tgt (id int, k int);
INSERT INTO fjn_a VALUES (1,'a',1),(2,'b',2);
INSERT INTO fjn_b VALUES (1,'a',1),(3,'c',3);
INSERT INTO fjn_c VALUES (1),(3);
CREATE VIEW fjn_vv AS SELECT a.x AS ax, a.t AS at, b.y AS by_ FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y;

-- ============================================================================
-- 1. What the outermost query is still refused for
-- ============================================================================

-- note: the shape the whole rule exists for
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x > b.y;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x <> b.y;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x IS NOT DISTINCT FROM b.y;

-- note: an ORDER BY, a LIMIT or a GROUPING SETS clause does not get past the refusal
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y ORDER BY 1;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y LIMIT 0;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y GROUP BY GROUPING SETS ((a.x),(b.y));

-- note: nor does a join above whose condition rejects nothing on either side
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y CROSS JOIN fjn_c c;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y LEFT JOIN fjn_c c ON c.z = a.x;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y FULL JOIN fjn_c c ON c.z = a.x;

-- ============================================================================
-- 2. What a qual of that query proves, counted over relations
-- ============================================================================

-- note: a qual naming a third relation proves nothing about either side of this join
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y JOIN fjn_c c ON c.z > 0;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y, fjn_c c WHERE c.z > 0;

-- note: an OR proves what every arm of it proves. Both arms here reject a null row of a, whichever
-- of its columns each one reads, so the join is a left join and is never asked the question
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y WHERE a.x = 1 OR a.t = 'b';

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y WHERE a.x IS NOT NULL OR a.t IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y WHERE a.x < 9 OR a.n < 9;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y WHERE b.y = 3 OR b.t = 'c';

-- note: de Morgan first, and then both arms are strict on a
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y WHERE NOT (a.x > 0 AND a.t > '');

-- note: one arm rejecting a null a and the other a null b proves neither
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y WHERE a.x IS NOT NULL OR b.y IS NOT NULL;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y WHERE a.x IS NOT NULL OR a.t IS NULL;

-- note: a HAVING clause with no aggregate in it is read the same way
-- begin-expected
-- columns: x | t
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT a.x, a.t FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y GROUP BY a.x, a.t HAVING a.x > 0 OR a.t > '' ORDER BY 1;

-- note: and so is the ON condition of an inner join above
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM (fjn_a a FULL JOIN fjn_b b ON a.x < b.y) JOIN fjn_c c ON a.x > 0 OR a.t > '';

-- note: a name no relation of the query answers to is reported before anything is planned
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "c"
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y WHERE c.z = 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column a.nosuch does not exist
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y WHERE a.nosuch = 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column c.nosuch does not exist
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y JOIN fjn_c c ON c.nosuch = 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y, fjn_c c WHERE nosuchcol = 1;

-- ============================================================================
-- 3. The reporting SQL two rounds of this refused
-- ============================================================================

-- note: a non-equi full join inside a DISTINCT subquery with a filter outside it
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT DISTINCT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) s WHERE s.ax IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT DISTINCT ON (a.x) a.x AS ax, b.y AS by_ FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y ORDER BY a.x) s WHERE s.ax IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y GROUP BY 1) s WHERE s.ax IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y GROUP BY 1 HAVING count(*) > 0) s WHERE s.ax IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT sum(a.n) AS s, a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y GROUP BY a.x) t WHERE t.ax IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT a.x AS ax, row_number() OVER (PARTITION BY a.x) AS rn FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) t WHERE t.ax IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM (SELECT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y UNION ALL SELECT 1) s WHERE s.ax IS NOT NULL;

-- note: through two levels neither of which could be pulled up
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT DISTINCT ax FROM (SELECT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y GROUP BY 1) u) t WHERE t.ax IS NOT NULL;

-- note: the same subquery under a WITH, a view and a LATERAL
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH t AS (SELECT DISTINCT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) SELECT count(*) FROM t WHERE t.ax IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM fjn_vv WHERE ax > 0 OR at > '';

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM fjn_c z, LATERAL (SELECT DISTINCT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) t WHERE t.ax IS NOT NULL;

-- note: an ordinary equality filter reduces the join just as IS NOT NULL does
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (SELECT DISTINCT a.x AS ax, b.y AS by_ FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) t WHERE t.ax = 1 AND t.by_ = 3;

-- note: the filter may arrive from a join's ON clause rather than a WHERE
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM fjn_c z JOIN (SELECT DISTINCT a.x AS ax, b.y AS by_ FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) t ON t.ax = z.z AND t.by_ = z.z;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM fjn_c z LEFT JOIN (SELECT DISTINCT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) t ON t.ax = z.z;

-- note: a sublink is pulled up to a semijoin, which memgres never saw at all
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM fjn_c c WHERE c.z IN (SELECT b.y FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM fjn_c z WHERE EXISTS (SELECT 1 FROM (SELECT DISTINCT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) t WHERE t.ax IS NOT NULL AND t.ax = z.z);

-- note: and the same sublink under a writing statement
UPDATE fjn_c SET z = z WHERE z IN (SELECT b.y FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y);
DELETE FROM fjn_c WHERE z IN (SELECT b.y FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) AND false;

INSERT INTO fjn_tgt(id, k) SELECT t.ax, 1 FROM (SELECT DISTINCT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) t WHERE t.ax IS NOT NULL;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM fjn_tgt;

-- ============================================================================
-- 4. The reach given up: PostgreSQL refuses these and memgres answers them
-- ============================================================================

-- expected-divergence: a join not in the outermost query is accepted rather than judged
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y LIMIT 10) s WHERE s.ax IS NOT NULL;

-- expected-divergence: a join not in the outermost query is accepted rather than judged
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y OFFSET 0) s WHERE s.ax IS NOT NULL;

-- note: a MATERIALIZED WITH query is neither pulled up nor pushed into, so PostgreSQL judges the
-- join on its own; memgres does not look inside a WITH query at all
-- expected-divergence: a join not in the outermost query is accepted rather than judged
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH q AS MATERIALIZED (SELECT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) SELECT count(*) FROM q WHERE ax IS NOT NULL;

-- note: nor is a WITH query that is referenced twice
-- expected-divergence: a join not in the outermost query is accepted rather than judged
-- begin-expected
-- columns: count
-- row: 6
-- end-expected
WITH q AS (SELECT a.x AS ax, b.y AS by_ FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) SELECT count(*) FROM q p, q r WHERE p.ax IS NOT NULL AND r.by_ IS NOT NULL;

-- expected-divergence: a join not in the outermost query is accepted rather than judged
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT (SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y);

-- expected-divergence: a join not in the outermost query is accepted rather than judged
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM fjn_vv;

-- note: an arm of a set operation is not the outermost query either
-- expected-divergence: a join not in the outermost query is accepted rather than judged
-- begin-expected
-- columns: count
-- row: 1
-- row: 3
-- end-expected
SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y UNION ALL SELECT 1 ORDER BY 1;

-- expected-divergence: a join not in the outermost query is accepted rather than judged
INSERT INTO fjn_tgt(id, k) SELECT a.x, 2 FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y;

-- teardown
DROP VIEW IF EXISTS fjn_vv CASCADE;
DROP TABLE IF EXISTS fjn_a CASCADE;
DROP TABLE IF EXISTS fjn_b CASCADE;
DROP TABLE IF EXISTS fjn_c CASCADE;
DROP TABLE IF EXISTS fjn_tgt CASCADE;
