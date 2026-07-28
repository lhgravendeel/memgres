-- Where an aggregate or a window function may not appear.
--
-- An aggregate has a value only once a group of rows has been collected; a window call only once
-- the result rows exist to be numbered against one another. A clause read before either has
-- happened cannot hold one, and PostgreSQL names the clause rather than evaluating something
-- arbitrary: WHERE, a JOIN condition, LIMIT, OFFSET, a VALUES row, the SET list of an UPDATE, a
-- CHECK constraint, an index expression, a DEFAULT, a generation expression.
--
-- Three things the scan has to get right, all measured against PostgreSQL 18:
--
--  1. It is complete. An IN list, a BETWEEN bound and ANY(ARRAY[...]) are as much part of WHERE
--     as a bare comparison is.
--  2. It stops at a nested query. WHERE a > (SELECT count(*) FROM u) is ordinary SQL -- that
--     aggregate belongs to the sub-select.
--  3. Except that an aggregate whose arguments name only columns of the enclosing relation
--     belongs to the enclosing query after all, so UPDATE t SET c = (SELECT max(c) FROM other),
--     where c is a column of t and not of other, is an aggregate in the UPDATE.
--
-- Also: a window function written without OVER is not a missing function (42809, not 42883); a
-- row lock cannot be combined with anything that collapses rows into one (0A000); and a view
-- body is analysed when the view is defined, not deferred to every read of it.

-- setup
DROP VIEW IF EXISTS plv_okview CASCADE;
DROP TABLE IF EXISTS plv_key CASCADE;
DROP TABLE IF EXISTS plv_other CASCADE;
DROP TABLE IF EXISTS plv_nokey CASCADE;

CREATE TABLE plv_nokey (a int, b text);
INSERT INTO plv_nokey VALUES (1, 'p'), (1, 'q'), (2, 'r');

CREATE TABLE plv_other (k int, s text);
INSERT INTO plv_other VALUES (1, 'x'), (2, 'y');

CREATE TABLE plv_key (id int PRIMARY KEY, o text);
INSERT INTO plv_key VALUES (1, 'm'), (2, 'n');

-- 1: an aggregate in WHERE, however deeply the clause buries it

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
SELECT * FROM plv_nokey WHERE count(*) > 1;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
SELECT * FROM plv_nokey WHERE a IN (1, count(*)::int);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
SELECT * FROM plv_nokey WHERE a BETWEEN 0 AND count(*)::int;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
SELECT * FROM plv_nokey WHERE a = ANY (ARRAY[count(*)::int]);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
SELECT * FROM plv_nokey WHERE count(*)::int = ANY (SELECT k FROM plv_other);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in JOIN conditions
-- end-expected-error
SELECT a FROM plv_nokey JOIN plv_other ON plv_nokey.a IN (plv_other.k, count(*)::int);

-- 2: a sub-select in WHERE keeps its own scope

-- begin-expected
-- columns: a | b
-- row: 1, p
-- row: 1, q
-- row: 2, r
-- end-expected
SELECT * FROM plv_nokey WHERE a > (SELECT min(k) FROM plv_other) - 1 ORDER BY a, b;

-- begin-expected
-- columns: a | b
-- row: 2, r
-- end-expected
SELECT * FROM plv_nokey WHERE a IN (SELECT max(k) FROM plv_other) ORDER BY a, b;

-- begin-expected
-- columns: a | b
-- row: 1, p
-- row: 1, q
-- row: 2, r
-- end-expected
SELECT * FROM plv_nokey WHERE EXISTS (SELECT count(*) FROM plv_other) ORDER BY a, b;

-- 3: LIMIT and OFFSET are read once, before there is a group

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in LIMIT
-- end-expected-error
SELECT count(*) FROM plv_nokey LIMIT count(*);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in OFFSET
-- end-expected-error
SELECT count(*) FROM plv_nokey OFFSET count(*);

-- begin-expected
-- columns: a
-- row: 1
-- row: 1
-- end-expected
SELECT a FROM plv_nokey ORDER BY a LIMIT (SELECT max(k) FROM plv_other);

-- 4: an UPDATE and a DELETE read one row at a time

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
DELETE FROM plv_other WHERE count(*) > 100;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
UPDATE plv_other SET s = 'z' WHERE sum(k) > 1000;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in UPDATE
-- end-expected-error
UPDATE plv_other SET k = count(*) WHERE false;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in UPDATE
-- end-expected-error
UPDATE plv_other SET k = 1 + max(k) WHERE false;

-- an aggregate over a column the sub-select does not supply belongs to the UPDATE

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in UPDATE
-- end-expected-error
UPDATE plv_other SET s = (SELECT max(s) FROM plv_nokey) WHERE false;

-- and one over a column it does supply does not

-- begin-expected
-- columns: k | s
-- row: 2, r
-- end-expected
UPDATE plv_other SET s = (SELECT max(b) FROM plv_nokey) WHERE k = 2 RETURNING k, s;

-- begin-expected
-- columns: k | s
-- row: 1, q
-- end-expected
UPDATE plv_other SET s = (SELECT max(b) FROM plv_nokey WHERE a = plv_other.k)
  WHERE k = 1 RETURNING k, s;

-- begin-expected
-- columns: k
-- row: 2
-- end-expected
DELETE FROM plv_other WHERE k > (SELECT max(a) FROM plv_nokey) - 1 RETURNING k;

-- 5: a VALUES row is written out, not read from anything

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in VALUES
-- end-expected-error
INSERT INTO plv_other VALUES (count(*), 'x');

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in VALUES
-- end-expected-error
INSERT INTO plv_other VALUES (1, 'x'), (count(*)::int, 'y');

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in VALUES
-- end-expected-error
SELECT * FROM (VALUES (count(*))) v;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in VALUES
-- end-expected-error
VALUES (count(*));

-- a sub-select in a VALUES row is a query of its own

-- begin-expected
-- columns: x
-- row: 3
-- end-expected
SELECT * FROM (VALUES ((SELECT count(*) FROM plv_nokey))) v(x);

-- begin-expected
-- columns: sum
-- row: 3
-- end-expected
SELECT sum(x) FROM (VALUES (1), (2)) v(x);

-- 6: a CHECK, an index key, a DEFAULT and a generation expression see one row

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in check constraints
-- end-expected-error
CREATE TABLE plv_chk (x int CHECK (count(x) > 0));

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in check constraints
-- end-expected-error
CREATE TABLE plv_chk (x int, CHECK (sum(x) > 0));

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in check constraints
-- end-expected-error
ALTER TABLE plv_other ADD CONSTRAINT plv_c1 CHECK (count(k) > 0);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in index expressions
-- end-expected-error
CREATE INDEX plv_i1 ON plv_nokey ((count(a)));

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in DEFAULT expressions
-- end-expected-error
CREATE TABLE plv_chk (x int DEFAULT count(*));

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in column generation expressions
-- end-expected-error
CREATE TABLE plv_chk (x int GENERATED ALWAYS AS (count(x)) STORED);

-- 7: a window call is refused in the same places, with its own sqlstate

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
SELECT * FROM plv_nokey WHERE a IN (1, row_number() OVER ()::int);

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
SELECT * FROM plv_nokey WHERE a BETWEEN 0 AND rank() OVER ()::int;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
DELETE FROM plv_other WHERE row_number() OVER () > 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in UPDATE
-- end-expected-error
UPDATE plv_nokey SET a = row_number() OVER () WHERE false;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in VALUES
-- end-expected-error
INSERT INTO plv_other VALUES (row_number() OVER ()::int, 'x');

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in VALUES
-- end-expected-error
SELECT * FROM (VALUES (row_number() OVER ())) v;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in check constraints
-- end-expected-error
CREATE TABLE plv_chk (x int CHECK (row_number() OVER () > 0));

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in index expressions
-- end-expected-error
CREATE INDEX plv_i2 ON plv_nokey ((rank() OVER ()));

-- 8: a window function with no OVER clause is missing a clause, not missing

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function rank requires an OVER clause
-- end-expected-error
SELECT rank() FROM plv_nokey;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
SELECT row_number() FROM plv_nokey;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function lag requires an OVER clause
-- end-expected-error
SELECT lag(a) FROM plv_nokey;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function ntile requires an OVER clause
-- end-expected-error
SELECT ntile(2) FROM plv_nokey;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function first_value requires an OVER clause
-- end-expected-error
SELECT first_value(a) FROM plv_nokey;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function dense_rank requires an OVER clause
-- end-expected-error
SELECT dense_rank() FROM plv_nokey;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function percent_rank requires an OVER clause
-- end-expected-error
SELECT percent_rank() FROM plv_nokey;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function cume_dist requires an OVER clause
-- end-expected-error
SELECT cume_dist() FROM plv_nokey;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function rank requires an OVER clause
-- end-expected-error
SELECT * FROM plv_nokey WHERE a = rank();

-- the four hypothetical-set names are ordered-set aggregates too: with arguments what is
-- missing is WITHIN GROUP, not OVER

-- begin-expected-error
-- sqlstate: 42809
-- message-like: WITHIN GROUP is required for ordered-set aggregate rank
-- end-expected-error
SELECT rank(a) FROM plv_nokey;

-- begin-expected
-- columns: rank
-- row: 1
-- end-expected
SELECT rank(1) WITHIN GROUP (ORDER BY a) FROM plv_nokey;

-- begin-expected
-- columns: rank
-- row: 1
-- row: 1
-- row: 3
-- end-expected
SELECT rank() OVER (ORDER BY a) FROM plv_nokey ORDER BY 1;

-- 9: a row lock needs a row to point at

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE is not allowed with GROUP BY clause
-- end-expected-error
SELECT a FROM plv_nokey GROUP BY a FOR UPDATE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR SHARE is not allowed with GROUP BY clause
-- end-expected-error
SELECT a FROM plv_nokey GROUP BY a FOR SHARE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR NO KEY UPDATE is not allowed with GROUP BY clause
-- end-expected-error
SELECT a FROM plv_nokey GROUP BY a FOR NO KEY UPDATE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE is not allowed with DISTINCT clause
-- end-expected-error
SELECT DISTINCT a FROM plv_nokey FOR UPDATE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE is not allowed with HAVING clause
-- end-expected-error
SELECT a FROM plv_nokey HAVING count(*) > 0 FOR UPDATE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE is not allowed with aggregate functions
-- end-expected-error
SELECT count(*) FROM plv_nokey FOR UPDATE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE is not allowed with window functions
-- end-expected-error
SELECT a, rank() OVER (ORDER BY a) FROM plv_nokey FOR UPDATE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT
-- end-expected-error
SELECT a FROM plv_nokey UNION SELECT k FROM plv_other FOR UPDATE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE is not allowed with GROUP BY clause
-- end-expected-error
SELECT * FROM (SELECT a FROM plv_nokey GROUP BY a) q FOR UPDATE;

-- a lock over rows that were never collapsed is fine

-- begin-expected
-- columns: a
-- row: 1
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM plv_nokey ORDER BY a FOR UPDATE;

-- begin-expected
-- columns: a
-- row: 1
-- row: 1
-- end-expected
SELECT a FROM plv_nokey WHERE a = 1 ORDER BY a FOR UPDATE OF plv_nokey;

-- begin-expected
-- columns: a
-- row: 1
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM plv_nokey n WHERE n.a IN (SELECT max(k) FROM plv_other) OR true
  ORDER BY a FOR UPDATE;

-- 10: a view body is judged when the view is written

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "plv_nokey.b" must appear in the GROUP BY clause
-- end-expected-error
CREATE VIEW plv_badview AS SELECT a, b FROM plv_nokey GROUP BY a;

CREATE VIEW plv_okview AS SELECT a, count(*) n FROM plv_nokey GROUP BY a;

-- begin-expected
-- columns: a | n
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a, n FROM plv_okview ORDER BY a;

-- 11: nothing above narrows an ordinary grouped, windowed or joined query

-- begin-expected
-- columns: a | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a, count(*) FROM plv_nokey GROUP BY a ORDER BY a;

-- begin-expected
-- columns: a | c
-- row: 1, 2
-- end-expected
SELECT a, count(*) c FROM plv_nokey GROUP BY a HAVING count(*) > 1 ORDER BY c;

-- begin-expected
-- columns: id | o | count
-- row: 1, m, 1
-- row: 2, n, 1
-- end-expected
SELECT id, o, count(*) FROM plv_key GROUP BY id ORDER BY id;

-- begin-expected
-- columns: a | sum
-- row: 1, 3
-- row: 2, 3
-- end-expected
SELECT a, sum(count(*)) OVER () FROM plv_nokey GROUP BY a ORDER BY a;

-- begin-expected
-- columns: rn
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT sub.rn FROM (SELECT row_number() OVER (ORDER BY a) rn FROM plv_nokey) sub
  WHERE sub.rn >= 1 ORDER BY 1;

-- begin-expected
-- columns: a | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
WITH c AS (SELECT a, count(*) FROM plv_nokey GROUP BY a) SELECT * FROM c ORDER BY a;

-- begin-expected
-- columns: a | m
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT k.id AS a, l.m FROM plv_key k,
  LATERAL (SELECT max(a) m FROM plv_nokey WHERE a = k.id) l ORDER BY 1;
