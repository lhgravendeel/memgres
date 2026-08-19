-- ============================================================================
-- which query level an aggregate belongs to
--
-- An aggregate belongs to the query level its argument variables come from, which is not
-- always the level it is written at. One written inside a sub-select but naming only an
-- enclosing query's columns is answered over that query's groups, once, and the sub-select
-- reads the answer.
--
-- ============================================================================

-- setup
CREATE TABLE zz_ql1 (id int, g int, v int);
INSERT INTO zz_ql1 VALUES (1,1,10),(2,1,20),(3,2,70);
CREATE TABLE zz_ql2 (id int, w int);
INSERT INTO zz_ql2 VALUES (1,100),(2,200),(3,300);

-- ============================================================================
-- an aggregate over an outer column is answered at the outer level
-- ============================================================================
-- begin-expected
-- columns: s
-- row: 100
-- end-expected
SELECT (SELECT sum(a.v)) AS s FROM zz_ql1 a;
-- and so the query it belongs to is a grouped one, answering once
-- begin-expected
-- columns: c
-- row: 3
-- end-expected
SELECT (SELECT count(a.v)) AS c FROM zz_ql1 a;
-- begin-expected
-- columns: m | n
-- row: 70 | 10
-- end-expected
SELECT (SELECT max(a.v)) AS m, (SELECT min(a.v)) AS n FROM zz_ql1 a;

-- ============================================================================
-- it is answered over each group, not over the whole relation
-- ============================================================================
-- begin-expected
-- columns: g | s
-- row: 1 | 30
-- row: 2 | 70
-- end-expected
SELECT a.g, (SELECT sum(a.v)) AS s FROM zz_ql1 a GROUP BY a.g ORDER BY a.g;
-- begin-expected
-- columns: g | s
-- row: 1 | 30
-- row: 2 | 140
-- end-expected
SELECT a.g, (SELECT max(a.v) + min(a.v)) AS s FROM zz_ql1 a GROUP BY a.g ORDER BY a.g;

-- ============================================================================
-- an aggregate naming the sub-select's own columns stays there
-- ============================================================================
-- begin-expected
-- columns: id | s
-- row: 1 | 100
-- row: 2 | 100
-- row: 3 | 100
-- end-expected
SELECT a.id, (SELECT sum(b.v) FROM zz_ql1 b) AS s FROM zz_ql1 a ORDER BY a.id;
-- and a bare star names no relation, so it counts the rows it is written over
-- begin-expected
-- columns: c
-- row: 3
-- row: 3
-- row: 3
-- end-expected
SELECT (SELECT count(*) FROM zz_ql2 b) AS c FROM zz_ql1 a ORDER BY 1;
-- begin-expected
-- columns: c
-- row: 3
-- row: 3
-- row: 3
-- end-expected
SELECT (SELECT count(1) FROM zz_ql2 b) AS c FROM zz_ql1 a ORDER BY 1;

-- ============================================================================
-- a qualified star names a relation, and is judged by which one
-- ============================================================================
-- begin-expected
-- columns: c
-- row: 3
-- end-expected
SELECT (SELECT count(a.*) FROM zz_ql2 b WHERE b.id = 1) AS c FROM zz_ql1 a;
-- begin-expected
-- columns: c
-- row: 3
-- row: 3
-- row: 3
-- end-expected
SELECT (SELECT count(b.*) FROM zz_ql2 b) AS c FROM zz_ql1 a ORDER BY 1;

-- ============================================================================
-- the sub-select's own FROM is what it may still name
-- ============================================================================
-- begin-expected
-- columns: s
-- row: 30
-- row: 70
-- end-expected
SELECT (SELECT sum(a.v) FROM zz_ql2 b WHERE b.id = 1) AS s FROM zz_ql1 a GROUP BY a.g ORDER BY 1;
-- begin-expected
-- columns: g | s
-- row: 1 | 33
-- row: 2 | 73
-- end-expected
SELECT a.g, (SELECT sum(a.v) + count(b.id) FROM zz_ql2 b) AS s FROM zz_ql1 a GROUP BY a.g ORDER BY a.g;

-- ============================================================================
-- a column the outer query did not group by is still ungrouped inside a sub-select
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT (SELECT max(a.v) FROM zz_ql1 b WHERE b.id = a.id) AS m FROM zz_ql1 a;

-- ============================================================================
-- an aggregate of this query may not stand in this query's WHERE, wherever it is written
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT count(*) AS c FROM zz_ql1 a WHERE (SELECT sum(a.v)) > 0;

-- ============================================================================
-- but it may stand in HAVING and in ORDER BY, which are read after the groups exist
-- ============================================================================
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT a.g AS g FROM zz_ql1 a GROUP BY a.g HAVING (SELECT sum(a.v)) > 40 ORDER BY 1;
-- begin-expected
-- columns: g
-- row: 1
-- row: 2
-- end-expected
SELECT a.g AS g FROM zz_ql1 a GROUP BY a.g ORDER BY (SELECT sum(a.v));
-- begin-expected
-- columns: g
-- row: 2
-- row: 1
-- end-expected
SELECT a.g AS g FROM zz_ql1 a GROUP BY a.g ORDER BY (SELECT sum(a.v)) DESC;

-- ============================================================================
-- a sub-select answering more than one row is still refused
-- ============================================================================
-- begin-expected-error
-- sqlstate: 21000
-- end-expected-error
SELECT (SELECT a.v FROM zz_ql2 b) AS v FROM zz_ql1 a;

-- ============================================================================
-- the answer has the type the aggregate gives it, not the type of what it reads
-- ============================================================================
-- begin-expected
-- columns: t
-- row: bigint
-- end-expected
SELECT pg_typeof((SELECT sum(a.v))) AS t FROM zz_ql1 a;
-- begin-expected
-- columns: t
-- row: bigint
-- end-expected
SELECT pg_typeof((SELECT count(a.v))) AS t FROM zz_ql1 a;
-- begin-expected
-- columns: t
-- row: numeric
-- end-expected
SELECT pg_typeof((SELECT avg(a.v))) AS t FROM zz_ql1 a;
-- and it is a value of that type where an operator has to be resolved
-- begin-expected
-- columns: s
-- row: 101
-- end-expected
SELECT (SELECT sum(a.v)) + 1 AS s FROM zz_ql1 a;
-- begin-expected
-- columns: b
-- row: t
-- end-expected
SELECT (SELECT max(a.v)) > 5 AS b FROM zz_ql1 a;
-- begin-expected
-- columns: c
-- row: 3
-- end-expected
SELECT (SELECT count(a.v))::text AS c FROM zz_ql1 a;

-- ============================================================================
-- every kind of aggregate is placed the same way
-- ============================================================================
-- begin-expected
-- columns: b
-- row: t
-- end-expected
SELECT (SELECT bool_and(a.v > 5)) AS b FROM zz_ql1 a;
-- begin-expected
-- columns: s
-- row: 10,20,70
-- end-expected
SELECT (SELECT string_agg(a.v::text, ',' ORDER BY a.v)) AS s FROM zz_ql1 a;
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT (SELECT count(DISTINCT a.g)) AS c FROM zz_ql1 a;
-- begin-expected
-- columns: p
-- row: 20
-- end-expected
SELECT (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a.v)) AS p FROM zz_ql1 a;
-- begin-expected
-- columns: a
-- row: {10,20,70}
-- end-expected
SELECT (SELECT array_agg(a.v ORDER BY a.v)) AS a FROM zz_ql1 a;

-- ============================================================================
-- a window call written over an outer column is not an aggregate of the outer query
-- ============================================================================
-- begin-expected
-- columns: s
-- row: 10
-- row: 20
-- row: 70
-- end-expected
SELECT (SELECT sum(a.v) OVER ()) AS s FROM zz_ql1 a ORDER BY 1;

-- ============================================================================
-- the rule reaches as far down as the sub-selects go
-- ============================================================================
-- begin-expected
-- columns: s
-- row: 100
-- end-expected
SELECT (SELECT (SELECT sum(a.v))) AS s FROM zz_ql1 a;
-- begin-expected
-- columns: s
-- row: 100
-- end-expected
SELECT (SELECT sum(a.v) FROM (SELECT 1 AS x) c) AS s FROM zz_ql1 a;
-- begin-expected
-- columns: s
-- row: 100
-- end-expected
SELECT (WITH cte AS (SELECT 1 AS x) SELECT sum(a.v) FROM cte) AS s FROM zz_ql1 a;

-- ============================================================================
-- a relation an inner FROM shadows is that FROM's, and is out of reach
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT (SELECT sum(a.v) FROM zz_ql2 a) AS s FROM zz_ql1 a ORDER BY 1;

-- ============================================================================
-- two aggregates of one level cannot be written one inside the other
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT sum(count(a.v)) AS s FROM zz_ql1 a;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT sum((SELECT count(a.v))) AS s FROM zz_ql1 a;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT percentile_cont((SELECT min(a.v) / 100.0)) WITHIN GROUP (ORDER BY a.v) AS p FROM zz_ql1 a;
-- an aggregate the sub-select owns is a value by the time the outer one reads it
-- begin-expected
-- columns: s
-- row: 9
-- end-expected
SELECT sum((SELECT count(b.v) FROM zz_ql1 b)) AS s FROM zz_ql1 a;
-- begin-expected
-- columns: s
-- row: 103
-- end-expected
SELECT count(*) + (SELECT sum(a.v)) AS s FROM zz_ql1 a;

-- ============================================================================
-- a direct argument is settled once for the group, so it reads only grouped columns
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT percentile_cont(a.v / 100.0) WITHIN GROUP (ORDER BY a.v) AS p FROM zz_ql1 a;
-- begin-expected
-- columns: g | p
-- row: 1 | 15
-- row: 2 | 70
-- end-expected
SELECT a.g, percentile_cont(0.5) WITHIN GROUP (ORDER BY a.v) AS p FROM zz_ql1 a GROUP BY a.g ORDER BY a.g;

-- ============================================================================
-- a writing statement is a query level too
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
UPDATE zz_ql2 SET w = (SELECT max(w)) WHERE id = 1;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
UPDATE zz_ql2 a SET w = (SELECT max(a.w)) WHERE a.id = 1;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
DELETE FROM zz_ql2 WHERE w > (SELECT max(w));
-- what the sub-select's own FROM supplies is the sub-select's, here as anywhere
UPDATE zz_ql2 SET w = (SELECT max(b.w) FROM zz_ql2 b) WHERE id = 1;
DELETE FROM zz_ql2 WHERE w > (SELECT max(w) FROM zz_ql2 b);
-- and a bare star still counts the rows of the query it is written in
UPDATE zz_ql2 SET w = (SELECT count(*)) WHERE id = 2;
-- begin-expected
-- columns: id | w
-- row: 1 | 300
-- row: 2 | 1
-- row: 3 | 300
-- end-expected
SELECT id, w AS w FROM zz_ql2 ORDER BY id;


-- teardown
DROP TABLE zz_ql1;
DROP TABLE zz_ql2;
