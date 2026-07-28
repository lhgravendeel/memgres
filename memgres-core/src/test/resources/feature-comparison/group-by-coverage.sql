-- Where the grouping check reaches, and which of a query's errors comes first.
--
-- All measured against PostgreSQL 18.
--
--  1. A nested query is not out of reach: an ungrouped column of the outer query read inside a
--     scalar subquery, an EXISTS or a HAVING sub-select is "subquery uses ungrouped column".
--     The DISTINCT ON list is judged like the select list, and CREATE VIEW refuses a grouped
--     query PostgreSQL refuses rather than storing it and returning an arbitrary row per read.
--  2. Order. PostgreSQL transforms the sort clause, then the grouping, then HAVING, and checks
--     for ungrouped columns last. So an unresolvable GROUP BY item (42703 / 42P01), a sort
--     position outside the select list (42P10) and a HAVING that does not type check (42883 /
--     22P02) each come before "must appear in the GROUP BY clause".
--  3. ORDER BY reads a constant as an output-column position exactly as GROUP BY does.
--  4. A window frame offset's type is resolved before the offset is asked to be constant: ROWS
--     and GROUPS count in bigint, RANGE counts in the ordering column's own type.

-- setup
DROP VIEW IF EXISTS vcv_view CASCADE;
DROP TABLE IF EXISTS vcv_nokey CASCADE;
DROP TABLE IF EXISTS vcv_other CASCADE;
DROP TABLE IF EXISTS vcv_pk CASCADE;
DROP TABLE IF EXISTS vcv_uq CASCADE;
DROP TABLE IF EXISTS vcv_t CASCADE;
DROP TABLE IF EXISTS vcv_num CASCADE;

CREATE TABLE vcv_nokey (a int, b text);
INSERT INTO vcv_nokey VALUES (1, 'x'), (1, 'y'), (2, 'z');

CREATE TABLE vcv_other (k int, s text);
INSERT INTO vcv_other VALUES (1, 'x'), (2, 'q');

CREATE TABLE vcv_pk (id int PRIMARY KEY, other text, n int);
INSERT INTO vcv_pk VALUES (1, 'a', 5), (2, 'b', 6);

CREATE TABLE vcv_uq (id int PRIMARY KEY, uq text NOT NULL UNIQUE, n int);
INSERT INTO vcv_uq VALUES (1, 'p', 1), (2, 'q', 2);

CREATE TABLE vcv_t (id int, a int);
INSERT INTO vcv_t VALUES (1, 10), (2, 20), (3, 30);

CREATE TABLE vcv_num (id int, a int, nn numeric, bi bigint, t text);
INSERT INTO vcv_num VALUES (1, 10, 1.5, 1, 'x'), (2, 20, 2.5, 2, 'y');

CREATE VIEW vcv_view AS SELECT a, b FROM vcv_nokey;

-- 1: an ungrouped column read inside a nested query

-- begin-expected-error
-- sqlstate: 42803
-- message-like: subquery uses ungrouped column "vcv_nokey.b" from outer query
-- end-expected-error
SELECT a, (SELECT s FROM vcv_other WHERE vcv_other.s = vcv_nokey.b) FROM vcv_nokey GROUP BY a;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: subquery uses ungrouped column "vcv_nokey.b" from outer query
-- end-expected-error
SELECT a, EXISTS (SELECT 1 FROM vcv_other WHERE s = vcv_nokey.b) FROM vcv_nokey GROUP BY a;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: subquery uses ungrouped column "vcv_nokey.b" from outer query
-- end-expected-error
SELECT a, CASE WHEN true THEN (SELECT s FROM vcv_other WHERE s = vcv_nokey.b) END
FROM vcv_nokey GROUP BY a;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: subquery uses ungrouped column "vcv_nokey.b" from outer query
-- end-expected-error
SELECT a FROM vcv_nokey GROUP BY a
HAVING (SELECT count(*) FROM vcv_other WHERE s = vcv_nokey.b) > 0;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: subquery uses ungrouped column "vcv_nokey.b" from outer query
-- end-expected-error
SELECT a FROM vcv_nokey GROUP BY a HAVING a IN (SELECT k FROM vcv_other WHERE s = vcv_nokey.b);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: subquery uses ungrouped column "vcv_nokey.b" from outer query
-- end-expected-error
SELECT a, (SELECT count(*) FROM vcv_other GROUP BY vcv_nokey.b) FROM vcv_nokey GROUP BY a;

-- the column is named through the alias the query gave it

-- begin-expected-error
-- sqlstate: 42803
-- message-like: subquery uses ungrouped column "x.b" from outer query
-- end-expected-error
SELECT x.a FROM vcv_nokey x GROUP BY x.a
HAVING EXISTS (SELECT 1 FROM vcv_nokey y WHERE y.b = x.b);

-- a UNIQUE NOT NULL column determines nothing; only a PRIMARY KEY does

-- begin-expected-error
-- sqlstate: 42803
-- message-like: subquery uses ungrouped column "u.id" from outer query
-- end-expected-error
SELECT u.uq, EXISTS (SELECT 1 FROM vcv_other o WHERE o.k = u.id) FROM vcv_uq u GROUP BY u.uq;

-- begin-expected
-- columns: other | exists
-- row: a, t
-- row: b, t
-- end-expected
SELECT u.other, EXISTS (SELECT 1 FROM vcv_other o WHERE o.k = u.id)
FROM vcv_pk u GROUP BY u.id ORDER BY 1;

-- a nested query that reads nothing ungrouped is untouched

-- begin-expected
-- columns: a | count
-- row: 1, 1
-- row: 2, 1
-- end-expected
SELECT a, (SELECT count(*) FROM vcv_other WHERE vcv_other.k = vcv_nokey.a)
FROM vcv_nokey GROUP BY a ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM vcv_nokey x GROUP BY a HAVING a IN (SELECT k FROM vcv_other WHERE k = x.a) ORDER BY 1;

-- begin-expected
-- columns: count
-- row: 2
-- row: 2
-- row: 2
-- end-expected
SELECT (SELECT count(*) FROM vcv_other GROUP BY vcv_nokey.b) FROM vcv_nokey ORDER BY 1;

-- 2: DISTINCT ON

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "vcv_nokey.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT DISTINCT ON (b) a FROM vcv_nokey GROUP BY a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT DISTINCT ON (sum(a)) a FROM vcv_nokey GROUP BY a ORDER BY sum(a), a;

-- begin-expected
-- columns: a
-- row: 1
-- row: 1
-- row: 2
-- end-expected
SELECT DISTINCT ON (row_number() OVER (ORDER BY a)) a FROM vcv_nokey
ORDER BY row_number() OVER (ORDER BY a);

-- begin-expected
-- columns: a | b
-- row: 1, x
-- row: 2, z
-- end-expected
SELECT DISTINCT ON (a) a, b FROM vcv_nokey ORDER BY a, b;

-- begin-expected
-- columns: a | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT DISTINCT ON (a) a, count(*) FROM vcv_nokey GROUP BY a ORDER BY a;

-- 3: SELECT DISTINCT sorts by what it selects

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: for SELECT DISTINCT, ORDER BY expressions must appear in select list
-- end-expected-error
SELECT DISTINCT a FROM vcv_nokey GROUP BY a ORDER BY count(*);

-- begin-expected
-- columns: a | sum
-- row: 1, 2
-- row: 2, 2
-- end-expected
SELECT DISTINCT a, sum(a) FROM vcv_nokey GROUP BY a ORDER BY sum(a), a;

-- 4: which of a query's errors comes first

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT b FROM vcv_nokey GROUP BY nosuchcol;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "vcv_other"
-- end-expected-error
SELECT b FROM vcv_nokey GROUP BY vcv_other.k;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column vcv_nokey.nosuchcol does not exist
-- end-expected-error
SELECT b FROM vcv_nokey GROUP BY vcv_nokey.nosuchcol;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 5 is not in select list
-- end-expected-error
SELECT a, b FROM vcv_nokey GROUP BY a ORDER BY 5;

-- the sort clause is transformed first, so it even beats an unresolvable grouping item

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 9 is not in select list
-- end-expected-error
SELECT a, b FROM vcv_nokey GROUP BY nosuchcol ORDER BY 9;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sum(text) does not exist
-- end-expected-error
SELECT a, b FROM vcv_nokey GROUP BY a HAVING sum(b) > 1;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: text > integer
-- end-expected-error
SELECT a, b FROM vcv_nokey GROUP BY a HAVING b > 1;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "zz"
-- end-expected-error
SELECT a, b FROM vcv_nokey GROUP BY a HAVING a > 'zz';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT a, b FROM vcv_nokey GROUP BY a HAVING count(*) > 'x';

-- 5: constants in ORDER BY

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position -1 is not in select list
-- end-expected-error
SELECT a FROM vcv_nokey GROUP BY a ORDER BY -1;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position -1 is not in select list
-- end-expected-error
SELECT a FROM vcv_nokey ORDER BY -1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT a, count(*) FROM vcv_nokey GROUP BY a ORDER BY 2.0;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT a FROM vcv_nokey ORDER BY 'x';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT a FROM vcv_nokey ORDER BY NULL;

-- an expression that merely looks constant is an ordinary sort key

-- begin-expected
-- columns: a
-- row: 1
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM vcv_nokey ORDER BY 1 + 0;

-- begin-expected
-- columns: array_agg
-- row: {1,1,2}
-- end-expected
SELECT array_agg(a ORDER BY 1) FROM vcv_nokey;

-- 6: CREATE VIEW is refused what PostgreSQL refuses

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "vcv_nokey.b" must appear in the GROUP BY clause
-- end-expected-error
CREATE VIEW vcv_bad AS SELECT a, b FROM vcv_nokey GROUP BY a;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 7 is not in select list
-- end-expected-error
CREATE VIEW vcv_bad AS SELECT a, b FROM vcv_nokey GROUP BY a ORDER BY 7;

CREATE VIEW vcv_good AS SELECT a, count(*) AS c FROM vcv_nokey GROUP BY a;

-- begin-expected
-- columns: a | c
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a, c FROM vcv_good ORDER BY 1;

DROP VIEW vcv_good;

-- 7: window frame offsets

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT sum(a) OVER (ORDER BY id RANGE BETWEEN 'x' PRECEDING AND CURRENT ROW) FROM vcv_t;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type numeric: "x"
-- end-expected-error
SELECT sum(a) OVER (ORDER BY nn RANGE BETWEEN 'x' PRECEDING AND CURRENT ROW) FROM vcv_num;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT sum(a) OVER (ORDER BY id ROWS BETWEEN 'x' PRECEDING AND CURRENT ROW) FROM vcv_t;

-- the offset's type is settled before any row is read

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT sum(a) OVER (ORDER BY id RANGE BETWEEN 'x' PRECEDING AND CURRENT ROW)
FROM vcv_t WHERE false;

-- a quoted offset that reads as a number still frames

-- begin-expected
-- columns: sum
-- row: 10
-- row: 30
-- row: 50
-- end-expected
SELECT sum(a) OVER (ORDER BY id RANGE BETWEEN '1' PRECEDING AND CURRENT ROW) FROM vcv_t ORDER BY 1;

-- an offset of the wrong type is that error, not the constant-offset one

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of ROWS must be type bigint, not type text
-- end-expected-error
SELECT a, count(*) OVER (ORDER BY a ROWS BETWEEN b PRECEDING AND CURRENT ROW)
FROM vcv_nokey GROUP BY a;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of GROUPS must be type bigint, not type text
-- end-expected-error
SELECT sum(a) OVER (ORDER BY id GROUPS BETWEEN t PRECEDING AND CURRENT ROW) FROM vcv_num;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: RANGE with offset PRECEDING/FOLLOWING is not supported for column type integer and offset type text
-- end-expected-error
SELECT sum(a) OVER (ORDER BY id RANGE BETWEEN t PRECEDING AND CURRENT ROW) FROM vcv_num;

-- an offset of a type the frame counts in is still refused for reading a row

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: argument of ROWS must not contain variables
-- end-expected-error
SELECT sum(a) OVER (ORDER BY id ROWS BETWEEN bi PRECEDING AND CURRENT ROW) FROM vcv_num;

-- 8: the shapes every real query is made of

-- begin-expected
-- columns: a | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a, count(*) FROM vcv_nokey GROUP BY a ORDER BY count(*) DESC, a;

-- begin-expected
-- columns: id | other
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT id, other FROM vcv_pk GROUP BY id ORDER BY 1;

-- begin-expected
-- columns: x | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT s.x, count(*) FROM (SELECT a AS x, b AS y FROM vcv_nokey) s GROUP BY s.x ORDER BY 1;

-- begin-expected
-- columns: a | c
-- row: 1, 1
-- row: 2, 1
-- end-expected
SELECT n.a, x.c FROM vcv_nokey n,
  LATERAL (SELECT count(*) AS c FROM vcv_other o WHERE o.k = n.a) x
GROUP BY n.a, x.c ORDER BY 1;

-- begin-expected
-- columns: rn
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT sub.rn FROM (SELECT row_number() OVER (ORDER BY a) AS rn FROM vcv_nokey) sub
WHERE sub.rn >= 1 ORDER BY 1;

-- begin-expected
-- columns: rn
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT s.rn FROM (SELECT row_number() OVER () AS rn FROM vcv_nokey) s
GROUP BY s.rn HAVING s.rn >= 1 ORDER BY 1;

-- teardown
DROP VIEW IF EXISTS vcv_view CASCADE;
DROP TABLE IF EXISTS vcv_num CASCADE;
DROP TABLE IF EXISTS vcv_t CASCADE;
DROP TABLE IF EXISTS vcv_uq CASCADE;
DROP TABLE IF EXISTS vcv_pk CASCADE;
DROP TABLE IF EXISTS vcv_other CASCADE;
DROP TABLE IF EXISTS vcv_nokey CASCADE;
