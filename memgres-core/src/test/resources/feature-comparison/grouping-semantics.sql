-- Grouping semantics: how a grouped query is formed, measured against PostgreSQL 18.
--
-- Three rules, all about the shape of the grouping rather than what it licenses:
--
--  1. HAVING with no GROUP BY forms exactly one group over the whole table, so the query
--     answers at most one row. WHERE does not take the group away: the group is there even
--     when no row reaches it, so "WHERE false HAVING true" still answers one row.
--  2. Several grouping elements are cross-multiplied. GROUP BY ROLLUP(a), ROLLUP(b) is the
--     Cartesian product of the two lists of grouping sets, not the first list alone, and the
--     same holds for CUBE, for a grouping set beside a plain column, and for two GROUPING SETS
--     clauses. GROUP BY DISTINCT then drops the sets the product produces more than once and
--     GROUP BY ALL keeps every one.
--  3. GROUPING() takes an expression the query groups by -- under a plain GROUP BY as much as
--     under GROUPING SETS, where it always answers 0 -- and anything else is an error. Its
--     result is int4.

-- setup
DROP VIEW IF EXISTS gsm_v CASCADE;
DROP TABLE IF EXISTS gsm_nokey CASCADE;
DROP TABLE IF EXISTS gsm_other CASCADE;
DROP TABLE IF EXISTS gsm_t CASCADE;
DROP TABLE IF EXISTS gsm_child CASCADE;
DROP TABLE IF EXISTS gsm_empty CASCADE;

CREATE TABLE gsm_nokey (a int, b text);
INSERT INTO gsm_nokey VALUES (1, 'p'), (1, 'q'), (2, 'r');

CREATE TABLE gsm_other (x int);
INSERT INTO gsm_other VALUES (1);

CREATE TABLE gsm_t (id int PRIMARY KEY, a int, b text, n int);
INSERT INTO gsm_t VALUES (1, 10, 'x', 5), (2, 20, 'y', 6), (3, 10, 'z', 7);

CREATE TABLE gsm_child (cid int PRIMARY KEY, tid int, amt int);
INSERT INTO gsm_child VALUES (1, 1, 5), (2, 1, 6), (3, 2, 7);

CREATE TABLE gsm_empty (a int, b text);

CREATE VIEW gsm_v AS SELECT a, count(*) AS c FROM gsm_nokey GROUP BY a;

-- 1: HAVING with no GROUP BY groups the whole table

-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT 1 FROM gsm_nokey HAVING true;

-- begin-expected
-- columns: ?column?
-- end-expected
SELECT 1 FROM gsm_nokey HAVING false;

-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT 1 FROM gsm_nokey WHERE false HAVING true;

-- begin-expected
-- columns: ?column?
-- end-expected
SELECT 1 FROM gsm_nokey HAVING 1 > (SELECT count(*) FROM gsm_other);

-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT 1 FROM gsm_nokey, gsm_other HAVING true;

-- begin-expected
-- columns: one
-- row: 1
-- end-expected
SELECT 1 AS one FROM gsm_nokey HAVING true LIMIT 5;

-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT 1 FROM gsm_empty HAVING true;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (SELECT 1 FROM gsm_nokey HAVING true) s;

-- the one group is judged like any other, so an ungrouped column in it is an error

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT a FROM gsm_nokey HAVING true;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT 1 FROM gsm_nokey HAVING a > 0;

-- 2: several grouping elements are cross-multiplied

-- begin-expected
-- columns: count
-- row: 1
-- row: 1
-- row: 1
-- row: 1
-- row: 1
-- row: 1
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT count(*) FROM gsm_nokey GROUP BY ROLLUP(a), ROLLUP(b) ORDER BY 1;

-- begin-expected
-- columns: count
-- row: 1
-- row: 1
-- row: 1
-- row: 1
-- row: 1
-- row: 1
-- end-expected
SELECT count(*) FROM gsm_nokey GROUP BY ROLLUP(a), b ORDER BY 1;

-- begin-expected
-- columns: count
-- row: 1
-- row: 1
-- row: 1
-- end-expected
SELECT count(*) FROM gsm_nokey GROUP BY GROUPING SETS ((a)), GROUPING SETS ((b)) ORDER BY 1;

-- begin-expected
-- columns: a | b | count
-- row: 1, p, 1
-- row: 1, q, 1
-- row: 1, NULL, 2
-- row: 2, r, 1
-- row: 2, NULL, 1
-- row: NULL, p, 1
-- row: NULL, q, 1
-- row: NULL, r, 1
-- row: NULL, NULL, 3
-- end-expected
SELECT a, b, count(*) FROM gsm_nokey GROUP BY CUBE(a), CUBE(b) ORDER BY 1, 2;

-- begin-expected
-- columns: a | b | count
-- row: 1, p, 1
-- row: 1, q, 1
-- row: 2, r, 1
-- row: NULL, p, 1
-- row: NULL, q, 1
-- row: NULL, r, 1
-- end-expected
SELECT a, b, count(*) FROM gsm_nokey GROUP BY GROUPING SETS ((a), ()), b ORDER BY 1, 2;

-- GROUP BY DISTINCT drops the sets the product repeats; GROUP BY ALL keeps them

-- begin-expected
-- columns: a | b | count
-- row: 1, p, 1
-- row: 1, q, 1
-- row: 1, NULL, 2
-- row: 2, r, 1
-- row: 2, NULL, 1
-- row: NULL, NULL, 3
-- end-expected
SELECT a, b, count(*) FROM gsm_nokey GROUP BY DISTINCT ROLLUP(a), ROLLUP(a, b) ORDER BY 1, 2;

-- begin-expected
-- columns: a | b | count
-- row: 1, p, 1
-- row: 1, p, 1
-- row: 1, q, 1
-- row: 1, q, 1
-- row: 1, NULL, 2
-- row: 1, NULL, 2
-- row: 1, NULL, 2
-- row: 2, r, 1
-- row: 2, r, 1
-- row: 2, NULL, 1
-- row: 2, NULL, 1
-- row: 2, NULL, 1
-- row: NULL, NULL, 3
-- end-expected
SELECT a, b, count(*) FROM gsm_nokey GROUP BY ALL ROLLUP(a), ROLLUP(a, b) ORDER BY 1, 2;

-- begin-expected
-- columns: a | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a, count(*) FROM gsm_nokey GROUP BY ALL a ORDER BY 1;

-- begin-expected
-- columns: a | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a, count(*) FROM gsm_nokey GROUP BY DISTINCT a, a ORDER BY 1;

-- a plain column beside a grouping set still multiplies into it, not beside it

-- begin-expected
-- columns: a | b | count
-- row: 1, p, 1
-- row: 1, q, 1
-- row: 1, NULL, 2
-- row: 2, r, 1
-- row: 2, NULL, 1
-- end-expected
SELECT a, b, count(*) FROM gsm_nokey GROUP BY a, GROUPING SETS ((b), ()) ORDER BY 1, 2;

-- 3: GROUPING() answers for a plain GROUP BY as well as for a grouping set

-- begin-expected
-- columns: a | grouping
-- row: 1, 0
-- row: 2, 0
-- end-expected
SELECT a, grouping(a) FROM gsm_nokey GROUP BY a ORDER BY 1;

-- begin-expected
-- columns: id | grouping
-- row: 1, 0
-- row: 2, 0
-- row: 3, 0
-- end-expected
SELECT id, grouping(id) FROM gsm_t GROUP BY id ORDER BY 1;

-- begin-expected
-- columns: ?column? | grouping
-- row: 2, 0
-- row: 3, 0
-- end-expected
SELECT a + 1, grouping(a + 1) FROM gsm_nokey GROUP BY a + 1 ORDER BY 1;

-- begin-expected
-- columns: a | grouping
-- row: 1, 0
-- row: 2, 0
-- end-expected
SELECT a, grouping(gsm_nokey.a) FROM gsm_nokey GROUP BY a ORDER BY 1;

-- begin-expected
-- columns: a | grouping
-- row: 1, 0
-- row: 2, 0
-- end-expected
SELECT a, grouping(a) FROM gsm_nokey GROUP BY 1 ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM gsm_nokey GROUP BY a HAVING grouping(a) = 0 ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM gsm_nokey GROUP BY a ORDER BY grouping(a), a;

-- begin-expected
-- columns: a | grouping
-- row: 1, 0
-- row: 2, 0
-- row: NULL, 1
-- end-expected
SELECT a, grouping(a) FROM gsm_nokey GROUP BY ROLLUP (a) ORDER BY 1, 2;

-- the result is int4, so it adds and casts as an integer

-- begin-expected
-- columns: pg_typeof
-- row: integer
-- end-expected
SELECT pg_typeof(grouping(a)) FROM gsm_nokey GROUP BY a LIMIT 1;

-- begin-expected
-- columns: a | ?column?
-- row: 1, 1
-- row: 2, 1
-- end-expected
SELECT a, grouping(a) + 1 FROM gsm_nokey GROUP BY a ORDER BY 1;

-- an argument the query does not group by has no answer

-- begin-expected-error
-- sqlstate: 42803
-- message-like: arguments to GROUPING must be grouping expressions of the associated query level
-- end-expected-error
SELECT a, grouping(b) FROM gsm_nokey GROUP BY ROLLUP (a) ORDER BY 1;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: arguments to GROUPING must be grouping expressions of the associated query level
-- end-expected-error
SELECT grouping(a) FROM gsm_nokey;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: arguments to GROUPING must be grouping expressions of the associated query level
-- end-expected-error
SELECT count(*) FROM gsm_nokey GROUP BY a HAVING grouping(b) = 0;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: grouping operations are not allowed in WHERE
-- end-expected-error
SELECT count(*) FROM gsm_nokey WHERE grouping(a) = 0 GROUP BY a;

-- 4: what has to keep working

-- begin-expected
-- columns: a | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a, count(*) FROM gsm_nokey GROUP BY a ORDER BY 1;

-- begin-expected
-- columns: k | c
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a AS k, count(*) AS c FROM gsm_nokey GROUP BY a ORDER BY k;

-- begin-expected
-- columns: a | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a, count(*) FROM gsm_nokey GROUP BY a ORDER BY count(*) DESC, a;

-- begin-expected
-- columns: a | count
-- row: 1, 2
-- end-expected
SELECT a, count(*) FROM gsm_nokey GROUP BY a HAVING count(*) > 1 ORDER BY 1;

-- begin-expected
-- columns: id | sum
-- row: 1, 11
-- row: 2, 7
-- end-expected
SELECT t.id, sum(c.amt) FROM gsm_t t JOIN gsm_child c ON c.tid = t.id GROUP BY t.id ORDER BY 1;

-- begin-expected
-- columns: a | c
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT * FROM gsm_v ORDER BY 1;

-- begin-expected
-- columns: a | c
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT * FROM (SELECT a, count(*) c FROM gsm_nokey GROUP BY a) s WHERE s.c >= 1 ORDER BY 1;

-- begin-expected
-- columns: a | c
-- row: 1, 2
-- row: 2, 1
-- end-expected
WITH w AS (SELECT a, count(*) c FROM gsm_nokey GROUP BY a) SELECT * FROM w ORDER BY 1;

-- begin-expected
-- columns: rn
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT sub.rn FROM (SELECT row_number() OVER (ORDER BY a) rn FROM gsm_nokey) sub
  WHERE sub.rn >= 1 ORDER BY 1;

-- begin-expected
-- columns: id | c
-- row: 1, 2
-- row: 2, 1
-- row: 3, 0
-- end-expected
SELECT t.id, x.c FROM gsm_t t,
  LATERAL (SELECT count(*) c FROM gsm_child ch WHERE ch.tid = t.id) x ORDER BY 1;

-- begin-expected
-- columns: id | a
-- row: 1, 10
-- row: 2, 20
-- row: 3, 10
-- end-expected
SELECT id, a FROM gsm_t GROUP BY id ORDER BY 1;

-- begin-expected
-- columns: a | row_number
-- row: 1, 1
-- row: 1, 2
-- row: 2, 3
-- end-expected
SELECT a, row_number() OVER (ORDER BY a) FROM gsm_nokey ORDER BY 1, 2;

-- begin-expected
-- columns: sum
-- row: 18
-- row: 18
-- row: 18
-- end-expected
SELECT sum(n) OVER () FROM gsm_t GROUP BY n ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT DISTINCT a FROM gsm_nokey ORDER BY 1;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM gsm_nokey WHERE false;
