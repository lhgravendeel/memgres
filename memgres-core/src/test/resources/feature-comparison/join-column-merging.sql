-- How the columns a join merges are matched, ordered and named.
--
-- Five things, all measured against PostgreSQL 18.
--
-- 1. A chain of USING or NATURAL joins was a cross product. A join's USING column was looked for
--    by scanning the whole row for two relations holding a column of that name, which is right for
--    one join and wrong for every one after it: a JOIN b USING (id) JOIN c USING (id) found a.id
--    and b.id, compared them -- the first join had already made them equal -- and never looked at
--    c at all, so the third relation was cross-joined in silently, on ordinary SQL, with no error.
--    A join's sides are now described by the columns they expose, so the second clause equates the
--    first join's merged id with c's, and a column an earlier join merged answers with whichever
--    relation behind it is not null.
--
-- 2. SELECT * lists the merged columns first. PostgreSQL's join output is every merged column, in
--    the order USING names them or a NATURAL join finds them, then what is left of the left side
--    and then of the right side. u1(id,s,p) JOIN u2(id,s,q) USING (s) is s, id, p, id, q -- not
--    the left side with a column crossed out. The third relation of a chain keeps its own id,
--    which a merge two joins earlier used to swallow, and an ordinal counts these columns.
--
-- 3. A name a USING clause gives must be one column of each side. Two of them on the left is
--    42702 common column name "x" appears more than once in left table, which is what
--    (a JOIN b ON true) JOIN c USING (id) writes; none is 42703 column "x" specified in USING
--    clause does not exist in left table, naming the side that actually lacks it rather than the
--    other one. The name is matched as written, so USING ("ID") does not find a column named id.
--
-- 4. A function in FROM is not always the right side of a lateral join. Running it once per left
--    row answers an INNER or LEFT join on a condition and nothing else: it has nowhere to put the
--    rows the right side kept to itself, so a FULL JOIN against one dropped every unmatched row on
--    both sides, and it never saw the columns a USING or NATURAL clause named.
--
-- 5. USING will not equate a number with a string. PostgreSQL has no such = operator and refuses
--    all twenty-four combinations of its six numeric and four string types; memgres compared them
--    as text and joined whatever happened to spell the same. Only that pair is refused, and only
--    between declared columns -- a relation a function produced carries no type worth judging --
--    because refusing valid SQL costs more than the permissiveness left behind.
--
-- The last section is ordinary SQL, which has to keep working: the cost of a rule that reaches too
-- far is a refused valid statement.

-- setup
DROP VIEW IF EXISTS jcm_v1 CASCADE;
DROP TABLE IF EXISTS jcm_a CASCADE;
DROP TABLE IF EXISTS jcm_b CASCADE;
DROP TABLE IF EXISTS jcm_c CASCADE;
DROP TABLE IF EXISTS jcm_d CASCADE;
DROP TABLE IF EXISTS jcm_e CASCADE;
DROP TABLE IF EXISTS jcm_f CASCADE;
DROP TABLE IF EXISTS jcm_u1 CASCADE;
DROP TABLE IF EXISTS jcm_u2 CASCADE;
DROP TABLE IF EXISTS jcm_u3 CASCADE;
DROP TABLE IF EXISTS jcm_u4 CASCADE;
DROP TABLE IF EXISTS jcm_emp CASCADE;
DROP TABLE IF EXISTS jcm_dpt CASCADE;
CREATE TABLE jcm_a (id int, av text);
CREATE TABLE jcm_b (id int, bv text);
CREATE TABLE jcm_c (id int, cv text);
CREATE TABLE jcm_d (id int, dv text);
CREATE TABLE jcm_e (id int, ev text);
CREATE TABLE jcm_f (bv text, id int);
CREATE TABLE jcm_u1 (id int, s int, p text);
CREATE TABLE jcm_u2 (id int, s int, q text);
CREATE TABLE jcm_u3 (id int, s int, r text);
CREATE TABLE jcm_u4 (id text, w int);
CREATE TABLE jcm_emp (id int, name text, dept_id int, salary int);
CREATE TABLE jcm_dpt (id int, name text, budget int);
INSERT INTO jcm_a VALUES (1,'a1'),(2,'a2'),(3,'a3');
INSERT INTO jcm_b VALUES (1,'b1'),(2,'b2'),(4,'b4');
INSERT INTO jcm_c VALUES (1,'c1'),(3,'c3'),(4,'c4');
INSERT INTO jcm_d VALUES (1,'d1'),(5,'d5');
INSERT INTO jcm_f VALUES ('b1',1),('bz',9);
INSERT INTO jcm_u1 VALUES (1,10,'a'),(2,20,'b'),(3,30,'c');
INSERT INTO jcm_u2 VALUES (1,10,'x'),(2,99,'y'),(4,40,'z');
INSERT INTO jcm_u3 VALUES (1,10,'p'),(3,30,'q');
INSERT INTO jcm_u4 VALUES ('1',5);
INSERT INTO jcm_emp VALUES (1,'ann',10,100),(2,'bob',20,200),(3,'cy',30,300);
INSERT INTO jcm_dpt VALUES (1,'ann',999),(2,'zed',888);

-- 1. Chains of USING and NATURAL joins

-- stmt 1: the second USING was ignored and the third relation cross-joined
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM jcm_a JOIN jcm_b USING (id) JOIN jcm_c USING (id);

-- begin-expected
-- columns: id | av | bv | cv
-- row: 1, a1, b1, c1
-- end-expected
SELECT * FROM jcm_a JOIN jcm_b USING (id) JOIN jcm_c USING (id);

-- stmt 2: four relations chained on the same name
-- begin-expected
-- columns: id | av | bv | cv | dv
-- row: 1, a1, b1, c1, d1
-- end-expected
SELECT * FROM jcm_a JOIN jcm_b USING (id) JOIN jcm_c USING (id) JOIN jcm_d USING (id);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM jcm_a a JOIN jcm_b b USING (id) JOIN jcm_c c USING (id) JOIN jcm_d d USING (id);

-- stmt 3: a NATURAL chain finds its own columns and keeps joining on them
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM jcm_u1 NATURAL JOIN jcm_u2 NATURAL JOIN jcm_u3;

-- begin-expected
-- columns: id | s | p | q | r
-- row: 1, 10, a, x, p
-- end-expected
SELECT * FROM jcm_u1 NATURAL JOIN jcm_u2 NATURAL JOIN jcm_u3;

-- begin-expected
-- columns: id | av | bv | cv | dv
-- row: 1, a1, b1, c1, d1
-- end-expected
SELECT * FROM jcm_a NATURAL JOIN jcm_b NATURAL JOIN jcm_c NATURAL JOIN jcm_d;

-- stmt 4: an outer chain matches on whichever side of the merge is not null
-- begin-expected
-- columns: id | av | bv | cv
-- row: 1, a1, b1, c1
-- row: 2, a2, b2, NULL
-- row: 3, a3, NULL, c3
-- end-expected
SELECT * FROM jcm_a LEFT JOIN jcm_b USING (id) LEFT JOIN jcm_c USING (id) ORDER BY 1;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM jcm_u1 LEFT JOIN jcm_u2 USING (id) LEFT JOIN jcm_u3 USING (id);

-- begin-expected
-- columns: id | s | p | q | r
-- row: 1, 10, a, x, p
-- row: 2, 20, b, NULL, NULL
-- row: 3, 30, c, NULL, q
-- end-expected
SELECT * FROM jcm_u1 NATURAL LEFT JOIN jcm_u2 NATURAL LEFT JOIN jcm_u3 ORDER BY 1;

-- stmt 5: a FULL chain keeps every side's unmatched rows once
-- begin-expected
-- columns: id | av | bv | cv
-- row: 1, a1, b1, c1
-- row: 2, a2, b2, NULL
-- row: 3, a3, NULL, c3
-- row: 4, NULL, b4, c4
-- end-expected
SELECT * FROM jcm_a FULL JOIN jcm_b USING (id) FULL JOIN jcm_c USING (id) ORDER BY 1;

-- begin-expected
-- columns: id | s | p | s | q | s | r
-- row: 1, 10, a, 10, x, 10, p
-- row: 2, 20, b, 99, y, NULL, NULL
-- row: 3, 30, c, NULL, NULL, 30, q
-- row: 4, NULL, NULL, 40, z, NULL, NULL
-- end-expected
SELECT * FROM jcm_u1 FULL JOIN jcm_u2 USING (id) FULL JOIN jcm_u3 USING (id) ORDER BY 1;

-- stmt 6: inner and outer joins mixed in one chain
-- begin-expected
-- columns: id | av | bv | cv
-- row: 1, a1, b1, c1
-- row: 3, a3, NULL, c3
-- end-expected
SELECT * FROM jcm_a LEFT JOIN jcm_b USING (id) JOIN jcm_c USING (id) ORDER BY 1;

-- begin-expected
-- columns: id | av | bv | cv
-- row: 1, a1, b1, c1
-- row: 2, a2, b2, NULL
-- end-expected
SELECT * FROM jcm_a JOIN jcm_b USING (id) LEFT JOIN jcm_c USING (id) ORDER BY 1;

-- begin-expected
-- columns: id | av | bv | cv
-- row: 1, a1, b1, c1
-- row: 3, NULL, NULL, c3
-- row: 4, NULL, b4, c4
-- end-expected
SELECT * FROM jcm_a RIGHT JOIN jcm_b USING (id) RIGHT JOIN jcm_c USING (id) ORDER BY 1;

-- stmt 7: USING and NATURAL mixed in one chain
-- begin-expected
-- columns: id | av | bv | cv
-- row: 1, a1, b1, c1
-- end-expected
SELECT * FROM jcm_a NATURAL JOIN jcm_b JOIN jcm_c USING (id);

-- begin-expected
-- columns: id | av | bv | cv
-- row: 1, a1, b1, c1
-- end-expected
SELECT * FROM jcm_a JOIN jcm_b USING (id) NATURAL JOIN jcm_c;

-- stmt 8: a merge mixed with an ON condition keeps the third relation's own column
-- begin-expected
-- columns: id | av | bv | id | cv
-- row: 1, a1, b1, 1, c1
-- end-expected
SELECT * FROM jcm_a JOIN jcm_b USING (id) JOIN jcm_c ON jcm_c.id = jcm_a.id;

-- stmt 9: a parenthesised grouping joins the group it is written as
-- begin-expected
-- columns: id | av | bv | cv
-- row: 1, a1, b1, c1
-- end-expected
SELECT * FROM (jcm_a JOIN jcm_b USING (id)) JOIN jcm_c USING (id);

-- begin-expected
-- columns: id | av | bv | cv
-- row: 1, a1, b1, c1
-- end-expected
SELECT * FROM jcm_a JOIN (jcm_b JOIN jcm_c USING (id)) USING (id);

-- begin-expected
-- columns: id | av | bv | cv | dv
-- row: 1, a1, b1, c1, d1
-- end-expected
SELECT * FROM (jcm_a NATURAL JOIN jcm_b) NATURAL JOIN (jcm_c NATURAL JOIN jcm_d);

-- begin-expected
-- columns: id | av | id | bv | cv
-- row: 1, a1, 1, b1, c1
-- row: 2, a2, NULL, NULL, NULL
-- row: 3, a3, NULL, NULL, NULL
-- end-expected
SELECT * FROM jcm_a a LEFT JOIN (jcm_b b JOIN jcm_c c USING (id)) ON a.id = b.id ORDER BY 1;

-- stmt 10: a relation joined on afterwards is not merged into the chain
-- begin-expected
-- columns: id | s | p | q | id | s | r
-- row: 1, 10, a, x, 1, 10, p
-- row: 1, 10, a, x, 3, 30, q
-- end-expected
SELECT * FROM jcm_u1 NATURAL JOIN jcm_u2 JOIN jcm_u3 ON true ORDER BY 5;

-- begin-expected
-- columns: id | s | p | q | id | s | r
-- row: 1, 10, a, x, 1, 10, p
-- row: 1, 10, a, x, 3, 30, q
-- end-expected
SELECT * FROM jcm_u1 NATURAL JOIN jcm_u2 CROSS JOIN jcm_u3 ORDER BY 5;

-- stmt 11: an empty side still describes the chain
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM jcm_e NATURAL JOIN jcm_a NATURAL JOIN jcm_b;

-- begin-expected
-- columns: id | av | ev
-- row: 1, a1, NULL
-- row: 2, a2, NULL
-- row: 3, a3, NULL
-- end-expected
SELECT * FROM jcm_a LEFT JOIN jcm_e USING (id) ORDER BY 1;

-- 2. What SELECT * lists, and in what order

-- stmt 12: the merged column comes first, not where the left side had it
-- begin-expected
-- columns: s | id | p | id | q
-- row: 10, 1, a, 1, x
-- end-expected
SELECT * FROM jcm_u1 JOIN jcm_u2 USING (s);

-- begin-expected
-- columns: s | id | p | q
-- row: 10, 1, a, x
-- end-expected
SELECT * FROM jcm_u1 JOIN jcm_u2 USING (s, id);

-- begin-expected
-- columns: id | s | p | q
-- row: 1, 10, a, x
-- end-expected
SELECT * FROM jcm_u1 JOIN jcm_u2 USING (id, s);

-- stmt 13: a NATURAL join finds the common columns in the left side's order
-- begin-expected
-- columns: id | name | dept_id | salary | budget
-- row: 1, ann, 10, 100, 999
-- end-expected
SELECT * FROM jcm_emp NATURAL JOIN jcm_dpt;

-- begin-expected
-- columns: id | bv | av
-- row: 1, b1, a1
-- end-expected
SELECT * FROM jcm_f NATURAL JOIN jcm_a;

-- begin-expected
-- columns: id | av | bv
-- row: 1, a1, b1
-- end-expected
SELECT * FROM jcm_a NATURAL JOIN jcm_f;

-- stmt 14: an outer join's merged column takes the side that is not null
-- begin-expected
-- columns: id | av | bv
-- row: 1, a1, b1
-- row: 2, a2, b2
-- row: 3, a3, NULL
-- row: 4, NULL, b4
-- end-expected
SELECT * FROM jcm_a FULL JOIN jcm_b USING (id) ORDER BY 1;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM jcm_a FULL JOIN jcm_b USING (id) ORDER BY 1;

-- begin-expected
-- columns: id | name | dept_id | salary | budget
-- row: 1, ann, 10, 100, 999
-- row: 2, bob, 20, 200, NULL
-- row: 2, zed, NULL, NULL, 888
-- row: 3, cy, 30, 300, NULL
-- end-expected
SELECT * FROM jcm_emp NATURAL FULL JOIN jcm_dpt ORDER BY 1, 2;

-- stmt 15: an ordinal counts the columns the join exposes
-- begin-expected
-- columns: id | av | bv
-- row: 4, NULL, b4
-- row: 3, a3, NULL
-- row: 2, a2, b2
-- row: 1, a1, b1
-- end-expected
SELECT * FROM jcm_a FULL JOIN jcm_b USING (id) ORDER BY 1 DESC;

-- begin-expected
-- columns: id | av | bv | id | cv
-- row: 1, a1, b1, 1, c1
-- row: 1, a1, b1, 3, c3
-- row: 1, a1, b1, 4, c4
-- row: 2, a2, b2, 1, c1
-- row: 2, a2, b2, 3, c3
-- row: 2, a2, b2, 4, c4
-- end-expected
SELECT * FROM jcm_a JOIN jcm_b USING (id) JOIN jcm_c ON true ORDER BY 1, 4;

-- stmt 16: grouping and windowing see the same columns
-- begin-expected
-- columns: id | av | bv
-- row: 1, a1, b1
-- row: 2, a2, b2
-- row: 3, a3, NULL
-- row: 4, NULL, b4
-- end-expected
SELECT * FROM jcm_a FULL JOIN jcm_b USING (id) GROUP BY 1, 2, 3 ORDER BY 1;

-- begin-expected
-- columns: id | av | bv | row_number
-- row: 1, a1, b1, 1
-- row: 2, a2, b2, 2
-- row: 3, a3, NULL, 3
-- row: 4, NULL, b4, 4
-- end-expected
SELECT *, row_number() OVER (ORDER BY id) FROM jcm_a FULL JOIN jcm_b USING (id) ORDER BY id;

-- stmt 17: a qualified star still lists that relation's own columns
-- begin-expected
-- columns: id | av | id | bv
-- row: 1, a1, 1, b1
-- row: 2, a2, 2, b2
-- end-expected
SELECT a.*, b.* FROM jcm_a a JOIN jcm_b b USING (id) ORDER BY 1;

-- 3. Names a merge makes, and the ones it refuses

-- stmt 18: a merged name resolves, and a third relation makes it ambiguous again
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM jcm_a JOIN jcm_b USING (id) JOIN jcm_c USING (id);

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "id" is ambiguous
-- end-expected-error
SELECT id FROM jcm_a JOIN jcm_b USING (id) JOIN jcm_c ON true;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "s" is ambiguous
-- end-expected-error
SELECT s FROM jcm_u1 JOIN jcm_u2 USING (id) JOIN jcm_u3 ON true;

-- stmt 19: a relation's own column is still readable through its name
-- begin-expected
-- columns: id | id
-- row: 1, 1
-- row: 2, 2
-- row: 3, NULL
-- row: NULL, 4
-- end-expected
SELECT a.id, b.id FROM jcm_a a FULL JOIN jcm_b b USING (id) ORDER BY 1, 2;

-- stmt 20: a USING name must be one column of each side
-- begin-expected-error
-- sqlstate: 42702
-- message-like: common column name "id" appears more than once in left table
-- end-expected-error
SELECT * FROM (jcm_u1 JOIN jcm_u2 ON true) JOIN jcm_u3 USING (id);

-- begin-expected-error
-- sqlstate: 42702
-- message-like: common column name "id" appears more than once in left table
-- end-expected-error
SELECT * FROM jcm_a JOIN jcm_b ON jcm_a.id = jcm_b.id JOIN jcm_c USING (id);

-- begin-expected-error
-- sqlstate: 42702
-- message-like: common column name "s" appears more than once in left table
-- end-expected-error
SELECT * FROM jcm_u1 JOIN jcm_u2 USING (id) NATURAL JOIN jcm_u3;

-- stmt 21: a missing USING name names the side that actually lacks it
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "w" specified in USING clause does not exist in left table
-- end-expected-error
SELECT * FROM jcm_u1 JOIN jcm_u4 USING (w);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "w" specified in USING clause does not exist in right table
-- end-expected-error
SELECT * FROM jcm_u4 JOIN jcm_u1 USING (w);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "av" specified in USING clause does not exist in right table
-- end-expected-error
SELECT * FROM jcm_a JOIN jcm_b USING (av);

-- stmt 22: a quoted USING name matches the case it is written in
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "ID" specified in USING clause does not exist in left table
-- end-expected-error
SELECT * FROM jcm_u1 JOIN jcm_u2 USING ("ID");

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM jcm_a JOIN jcm_b USING (ID);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM jcm_a JOIN jcm_b USING ("id");

-- stmt 23: a star qualified by a name no FROM item answers to
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "j"
-- end-expected-error
SELECT j.* FROM jcm_a JOIN jcm_b USING (id);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "j"
-- end-expected-error
SELECT j.* FROM jcm_a;

-- stmt 24: USING will not equate a number with a string
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = text
-- end-expected-error
SELECT count(*) FROM jcm_u1 JOIN jcm_u4 USING (id);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = text
-- end-expected-error
SELECT * FROM jcm_u1 NATURAL JOIN jcm_u4;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: text = integer
-- end-expected-error
SELECT count(*) FROM jcm_u4 JOIN jcm_u1 USING (id);

-- 4. A function in FROM as one side of a join

-- stmt 25: a FULL JOIN against one keeps both sides' unmatched rows
-- begin-expected
-- columns: id | av | id
-- row: 1, a1, NULL
-- row: 2, a2, 2
-- row: 3, a3, 3
-- row: NULL, NULL, 4
-- row: NULL, NULL, 5
-- end-expected
SELECT * FROM jcm_a FULL JOIN generate_series(2,5) g(id) ON jcm_a.id = g.id ORDER BY 1 NULLS LAST, 3;

-- begin-expected
-- columns: id | av
-- row: 1, a1
-- row: 2, a2
-- row: 3, a3
-- row: 4, NULL
-- row: 5, NULL
-- end-expected
SELECT * FROM jcm_a FULL JOIN generate_series(2,5) g(id) USING (id) ORDER BY 1;

-- stmt 26: a RIGHT JOIN against one keeps its rows
-- begin-expected
-- columns: id | av | id
-- row: 2, a2, 2
-- row: 3, a3, 3
-- row: NULL, NULL, 4
-- end-expected
SELECT * FROM jcm_a RIGHT JOIN generate_series(2,4) g(id) ON g.id = jcm_a.id ORDER BY 3;

-- stmt 27: a USING or NATURAL clause against one is honoured
-- begin-expected
-- columns: id | av
-- row: 1, a1
-- row: 2, a2
-- end-expected
SELECT * FROM jcm_a JOIN generate_series(1,2) g(id) USING (id) ORDER BY 1;

-- begin-expected
-- columns: id | av
-- row: 1, a1
-- row: 2, a2
-- end-expected
SELECT * FROM jcm_a NATURAL JOIN generate_series(1,2) g(id) ORDER BY 1;

-- stmt 28: a lateral function still reads the rows to its left
-- begin-expected
-- columns: id | av | n
-- row: 1, a1, 2
-- row: 2, a2, 3
-- row: 3, a3, 4
-- end-expected
SELECT * FROM jcm_a CROSS JOIN LATERAL (SELECT jcm_a.id + 1 AS n) l ORDER BY 1;

-- 5. Ordinary SQL, which has to keep working

-- stmt 29: plain joins are unchanged
-- begin-expected
-- columns: id | av | id | bv
-- row: 1, a1, 1, b1
-- row: 2, a2, 2, b2
-- end-expected
SELECT * FROM jcm_a a JOIN jcm_b b ON a.id = b.id ORDER BY 1;

-- begin-expected
-- columns: id | av | id | bv | id | cv
-- row: 1, a1, 1, b1, 1, c1
-- end-expected
SELECT * FROM jcm_a JOIN jcm_b ON jcm_a.id = jcm_b.id JOIN jcm_c ON jcm_b.id = jcm_c.id;

-- begin-expected
-- columns: count
-- row: 9
-- end-expected
SELECT count(*) FROM jcm_a CROSS JOIN jcm_b;

-- begin-expected
-- columns: count
-- row: 27
-- end-expected
SELECT count(*) FROM jcm_a, jcm_b, jcm_c;

-- stmt 30: a merged column reads from every clause
-- begin-expected
-- columns: id
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM jcm_a FULL JOIN jcm_b USING (id) WHERE id > 2 ORDER BY 1;

-- begin-expected
-- columns: max
-- row: 4
-- end-expected
SELECT max(id) FROM jcm_a FULL JOIN jcm_b USING (id);

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT DISTINCT id FROM jcm_a NATURAL FULL JOIN jcm_b ORDER BY 1;

-- stmt 31: derived relations merge like stored ones
-- begin-expected
-- columns: id | av | bv
-- row: 1, a1, b1
-- row: 2, a2, b2
-- end-expected
SELECT * FROM (SELECT id, av FROM jcm_a) s JOIN (SELECT id, bv FROM jcm_b) t USING (id) ORDER BY 1;

-- begin-expected
-- columns: id | av | bv | cv
-- row: 1, a1, b1, c1
-- end-expected
WITH w AS (SELECT * FROM jcm_a) SELECT * FROM w NATURAL JOIN jcm_b NATURAL JOIN jcm_c;

-- begin-expected
-- columns: id | av | bv
-- row: 1, a1, b1
-- row: 2, a2, b2
-- end-expected
SELECT * FROM (jcm_a JOIN jcm_b USING (id)) x ORDER BY 1;

-- stmt 32: wider numeric and wider string pairs are still equatable
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM jcm_a JOIN (SELECT id::bigint AS id FROM jcm_b) t USING (id);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM jcm_a JOIN (SELECT id::numeric AS id FROM jcm_b) t USING (id);

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM jcm_a a JOIN (SELECT av::varchar AS av FROM jcm_a) t USING (av);

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM jcm_a a NATURAL JOIN jcm_a b;

-- stmt 33: a view over a merge is readable and joinable again
CREATE VIEW jcm_v1 AS SELECT * FROM jcm_a JOIN jcm_b USING (id);

-- begin-expected
-- columns: id | av | bv
-- row: 1, a1, b1
-- row: 2, a2, b2
-- end-expected
SELECT * FROM jcm_v1 ORDER BY 1;

-- begin-expected
-- columns: id | av | bv | cv
-- row: 1, a1, b1, c1
-- end-expected
SELECT * FROM jcm_v1 NATURAL JOIN jcm_c;

-- stmt 34: a merge read from a subquery
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT (SELECT count(*) FROM jcm_a JOIN jcm_b USING (id)) AS count;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM jcm_a WHERE id IN (SELECT id FROM jcm_b NATURAL JOIN jcm_c) ORDER BY 1;

-- cleanup
DROP VIEW jcm_v1;
DROP TABLE jcm_a;
DROP TABLE jcm_b;
DROP TABLE jcm_c;
DROP TABLE jcm_d;
DROP TABLE jcm_e;
DROP TABLE jcm_f;
DROP TABLE jcm_u1;
DROP TABLE jcm_u2;
DROP TABLE jcm_u3;
DROP TABLE jcm_u4;
DROP TABLE jcm_emp;
DROP TABLE jcm_dpt;
