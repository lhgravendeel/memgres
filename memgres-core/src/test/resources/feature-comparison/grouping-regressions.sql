-- Grouped queries PostgreSQL runs, and the grouping rules that had begun to refuse them.
--
-- Three rules judged how a query was written rather than what it means, and each turned working
-- SQL into an error:
--
--  1. A DISTINCT query's sort key was matched against the select list by spelling, so
--     SELECT DISTINCT s, count(*) ... GROUP BY s ORDER BY d.s was refused although the qualified
--     d.s and the bare s are the same column. Matching is on the resolved column now; a sort key
--     naming a column the select list does not carry is still refused.
--
--  2. The check for a count compared against a non-integer ran on every binary expression, so
--     HAVING count(*) || 'x' = '2x' was read as a comparison and refused with 22P02. Only a
--     comparison and the four arithmetic operators read a bare string literal as the bigint
--     beside them; concatenation and everything else are left alone.
--
--  3. A cast to the column's own type was read as a coercion. PostgreSQL erases such a cast while
--     it analyses the query, so GROUP BY a::int over an int column is GROUP BY a: it licenses a
--     bare a, and a primary key grouped that way still determines its row. A cast to another type
--     -- b::varchar over text, n::numeric over numeric(10,2) -- is a real coercion and still
--     leaves the column ungrouped.
--
-- Everything here is measured against PostgreSQL 18.

-- setup
DROP VIEW IF EXISTS grr_v CASCADE;
DROP TABLE IF EXISTS grr_f CASCADE;
DROP TABLE IF EXISTS grr_t CASCADE;
DROP TABLE IF EXISTS grr_u CASCADE;
DROP TABLE IF EXISTS grr_d CASCADE;
DROP TABLE IF EXISTS grr_cust CASCADE;
DROP TABLE IF EXISTS grr_ord CASCADE;
DROP TABLE IF EXISTS grr_w CASCADE;
DROP TABLE IF EXISTS grr_e CASCADE;
DROP TYPE IF EXISTS grr_mood CASCADE;

CREATE TABLE grr_f (id int PRIMARY KEY, a int, b text, d date, n numeric(10,2));
INSERT INTO grr_f VALUES (1,10,'x',DATE '2020-01-01',1.5),(2,10,'y',DATE '2020-02-01',2.5),
                         (3,20,'z',DATE '2021-01-01',3.5);

CREATE TABLE grr_t (id int PRIMARY KEY, a int, b text);
INSERT INTO grr_t VALUES (1,10,'x'),(2,20,'y'),(3,10,'z');

CREATE TABLE grr_u (id int PRIMARY KEY, a int, c text);
INSERT INTO grr_u VALUES (1,10,'p'),(2,30,'q');

CREATE TABLE grr_d (id int PRIMARY KEY, s text, n numeric);
INSERT INTO grr_d VALUES (1,'a',1.5),(2,'a',2.5),(3,'b',3.5);

CREATE TABLE grr_cust (id int PRIMARY KEY, name text, region text);
INSERT INTO grr_cust VALUES (1,'Ann','EU'),(2,'Bob','US'),(3,'Cid','EU'),(4,'Dee','APAC');

CREATE TABLE grr_ord (id int PRIMARY KEY, cust_id int, total numeric(10,2));
INSERT INTO grr_ord VALUES (10,1,100.00),(11,1,50.50),(12,2,75.25),(13,3,10.00),(14,3,20.00);

CREATE TABLE grr_w (id serial PRIMARY KEY, v varchar(3), c char(2), ts timestamp);
INSERT INTO grr_w (v,c,ts) VALUES ('ab','pq',TIMESTAMP '2024-01-01 10:00:00'),
                                  ('cd','rs',TIMESTAMP '2024-01-02 10:00:00');

CREATE TYPE grr_mood AS ENUM ('ok','bad');
CREATE TABLE grr_e (id int PRIMARY KEY, m grr_mood);
INSERT INTO grr_e VALUES (1,'ok'),(2,'ok'),(3,'bad');

CREATE VIEW grr_v AS SELECT s, count(*) AS c FROM grr_d GROUP BY s;

-- 1: a DISTINCT query's sort key is the column it resolves to, not the way it was spelled

-- begin-expected
-- columns: s | count
-- row: a, 2
-- row: b, 1
-- end-expected
SELECT DISTINCT s, count(*) FROM grr_d d GROUP BY s ORDER BY d.s;

-- begin-expected
-- columns: s | count
-- row: b, 1
-- row: a, 2
-- end-expected
SELECT DISTINCT s, count(*) FROM grr_d d GROUP BY s ORDER BY d.s DESC;

-- the relation may be named by its own name as well as by an alias
-- begin-expected
-- columns: s | count
-- row: a, 2
-- row: b, 1
-- end-expected
SELECT DISTINCT s, count(*) FROM grr_d GROUP BY s ORDER BY grr_d.s;

-- an output alias does not hide the column the sort key names
-- begin-expected
-- columns: k | count
-- row: a, 2
-- row: b, 1
-- end-expected
SELECT DISTINCT s AS k, count(*) FROM grr_d d GROUP BY s ORDER BY d.s;

-- a primary key groups the whole row, and the sort key may qualify any column of it
-- begin-expected
-- columns: id | name
-- row: 1, Ann
-- row: 2, Bob
-- row: 3, Cid
-- row: 4, Dee
-- end-expected
SELECT DISTINCT id, name FROM grr_cust c GROUP BY 1 ORDER BY c.name;

-- a sub-select in FROM answers for its own alias
-- begin-expected
-- columns: rn | count
-- row: 1, 1
-- row: 2, 1
-- row: 3, 1
-- row: 4, 1
-- row: 5, 1
-- end-expected
SELECT DISTINCT rn, count(*) FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn FROM grr_ord) sub
WHERE rn >= 1 GROUP BY rn ORDER BY sub.rn;

-- whole expressions match the same way, once each side's columns are resolved
-- begin-expected
-- columns: upper | count
-- row: B, 1
-- row: A, 2
-- end-expected
SELECT DISTINCT upper(s), count(*) FROM grr_d d GROUP BY s ORDER BY upper(d.s) DESC;

-- 2: the rule that belongs -- a sort key the DISTINCT does not keep

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: for SELECT DISTINCT, ORDER BY expressions must appear in select list
-- end-expected-error
SELECT DISTINCT s, count(*) FROM grr_d d GROUP BY s ORDER BY d.n;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: for SELECT DISTINCT, ORDER BY expressions must appear in select list
-- end-expected-error
SELECT DISTINCT count(*) FROM grr_d d GROUP BY s ORDER BY d.s;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: for SELECT DISTINCT, ORDER BY expressions must appear in select list
-- end-expected-error
SELECT DISTINCT s, count(*) FROM grr_d d GROUP BY s ORDER BY d.s || 'x';

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: for SELECT DISTINCT, ORDER BY expressions must appear in select list
-- end-expected-error
SELECT DISTINCT s FROM grr_d GROUP BY s ORDER BY max(id);

-- another relation's column of the same name is a different column
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: for SELECT DISTINCT, ORDER BY expressions must appear in select list
-- end-expected-error
SELECT DISTINCT t.a, count(*) FROM grr_t t JOIN grr_u u ON t.a = u.a GROUP BY t.a ORDER BY u.a;

-- 3: a count concatenated with a string is ordinary SQL

-- begin-expected
-- columns: a
-- row: 20
-- end-expected
SELECT a FROM grr_t GROUP BY a HAVING count(*) || 'x' = '1x' ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 10
-- row: 20
-- end-expected
SELECT a FROM grr_t GROUP BY a HAVING length(count(*) || 'x') = 2 ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT a FROM grr_t GROUP BY a HAVING 'x' || count(*) = 'x2' ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT a FROM grr_t GROUP BY a HAVING count(a) || 'x' = '2x' ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 10
-- row: 20
-- end-expected
SELECT a FROM grr_t GROUP BY a HAVING (count(*) || 'x') IS NOT NULL ORDER BY 1;

-- begin-expected
-- columns: s | count
-- row: a, 2
-- end-expected
SELECT s, count(*) FROM grr_d GROUP BY s HAVING count(*) || 'x' = '2x';

-- begin-expected
-- columns: s | count
-- row: a, 2
-- end-expected
SELECT s, count(*) FROM grr_d GROUP BY s HAVING 'a' || count(*) || 'b' = 'a2b';

-- begin-expected
-- columns: s | count
-- row: a, 2
-- row: b, 1
-- end-expected
SELECT s, count(*) FROM grr_d GROUP BY s HAVING count(*) || '' <> '' ORDER BY 1;

-- 4: the rule that belongs -- a comparison, and arithmetic, do read the literal as a bigint

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT a FROM grr_t GROUP BY a HAVING count(*) > 'x';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT a FROM grr_t GROUP BY a HAVING count(*) = 'x';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT a FROM grr_t GROUP BY a HAVING 'x' = count(*);

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT a FROM grr_t GROUP BY a HAVING count(*) <> 'x';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT a FROM grr_t GROUP BY a HAVING count(*) + 'x' = 2;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT a FROM grr_t GROUP BY a HAVING count(*) * 'x' = 2;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "1.5"
-- end-expected-error
SELECT a FROM grr_t GROUP BY a HAVING count(*) - '1.5' = 2;

-- a literal that reads as an integer is one, whichever operator it stands beside
-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT a FROM grr_t GROUP BY a HAVING count(*) > '1' ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT a FROM grr_t GROUP BY a HAVING count(*) + '1' = 3 ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 20
-- end-expected
SELECT a FROM grr_t GROUP BY a HAVING count(*) * '2' = 2 ORDER BY 1;

-- the other type error HAVING carries is untouched
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sum(text) does not exist
-- end-expected-error
SELECT a FROM grr_t GROUP BY a HAVING sum(b) > 1;

-- 5: grouping by a column cast to its own type groups by the column

-- begin-expected
-- columns: a | count
-- row: 10, 2
-- row: 20, 1
-- end-expected
SELECT a, count(*) FROM grr_f GROUP BY a::int ORDER BY 1;

-- begin-expected
-- columns: a | count
-- row: 10, 2
-- row: 20, 1
-- end-expected
SELECT a, count(*) FROM grr_f GROUP BY a::int4 ORDER BY 1;

-- begin-expected
-- columns: a | count
-- row: 10, 2
-- row: 20, 1
-- end-expected
SELECT a, count(*) FROM grr_f GROUP BY CAST(a AS integer) ORDER BY 1;

-- begin-expected
-- columns: a | count
-- row: 10, 2
-- row: 20, 1
-- end-expected
SELECT f.a, count(*) FROM grr_f f GROUP BY f.a::int ORDER BY 1;

-- begin-expected
-- columns: b | count
-- row: x, 1
-- row: y, 1
-- row: z, 1
-- end-expected
SELECT b, count(*) FROM grr_f GROUP BY b::text ORDER BY 1;

-- begin-expected
-- columns: d | count
-- row: 2020-01-01, 1
-- row: 2020-02-01, 1
-- row: 2021-01-01, 1
-- end-expected
SELECT d, count(*) FROM grr_f GROUP BY d::date ORDER BY 1;

-- the length or precision is part of the type, and matches here
-- begin-expected
-- columns: n | count
-- row: 1.50, 1
-- row: 2.50, 1
-- row: 3.50, 1
-- end-expected
SELECT n, count(*) FROM grr_f GROUP BY n::numeric(10,2) ORDER BY 1;

-- begin-expected
-- columns: v | count
-- row: ab, 1
-- row: cd, 1
-- end-expected
SELECT v, count(*) FROM grr_w GROUP BY v::varchar(3) ORDER BY 1;

-- begin-expected
-- columns: c | count
-- row: pq, 1
-- row: rs, 1
-- end-expected
SELECT c, count(*) FROM grr_w GROUP BY c::char(2) ORDER BY 1;

-- begin-expected
-- columns: ts | count
-- row: 2024-01-01 10:00:00, 1
-- row: 2024-01-02 10:00:00, 1
-- end-expected
SELECT ts, count(*) FROM grr_w GROUP BY ts::timestamp ORDER BY 1;

-- an enum is a named type of its own, and naming it is the no-op cast
-- begin-expected
-- columns: m | count
-- row: ok, 2
-- row: bad, 1
-- end-expected
SELECT m, count(*) FROM grr_e GROUP BY m::grr_mood ORDER BY 1;

-- two casts are two no-ops, and one buried in an expression is erased where it stands
-- begin-expected
-- columns: a | count
-- row: 10, 2
-- row: 20, 1
-- end-expected
SELECT a, count(*) FROM grr_f GROUP BY a::int::int ORDER BY 1;

-- begin-expected
-- columns: ?column? | count
-- row: 11, 2
-- row: 21, 1
-- end-expected
SELECT a + 1, count(*) FROM grr_f GROUP BY a::int + 1 ORDER BY 1;

-- begin-expected
-- columns: ?column? | count
-- row: 11, 2
-- row: 21, 1
-- end-expected
SELECT a::int + 1, count(*) FROM grr_f GROUP BY a + 1 ORDER BY 1;

-- the key erases too, so the whole row it determines stays available
-- begin-expected
-- columns: id | b | count
-- row: 1, x, 1
-- row: 2, y, 1
-- row: 3, z, 1
-- end-expected
SELECT id, b, count(*) FROM grr_f GROUP BY id::int ORDER BY 1;

-- begin-expected
-- columns: id | v | count
-- row: 1, ab, 1
-- row: 2, cd, 1
-- end-expected
SELECT id, v, count(*) FROM grr_w GROUP BY id::int ORDER BY 1;

-- 6: the rule that belongs -- a cast to another type is a value of its own

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT b, count(*) FROM grr_f GROUP BY b::varchar;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT n, count(*) FROM grr_f GROUP BY n::numeric;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT a, count(*) FROM grr_f GROUP BY a::bigint;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT a, count(*) FROM grr_f GROUP BY a::text;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT v, count(*) FROM grr_w GROUP BY v::varchar(4);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT v, count(*) FROM grr_w GROUP BY v::text;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT c, count(*) FROM grr_w GROUP BY c::char;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT ts, count(*) FROM grr_w GROUP BY ts::date;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT m, count(*) FROM grr_e GROUP BY m::text;

-- a cast over anything but a column keeps the coercion it looks like
-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT a, count(*) FROM grr_f GROUP BY (a + 0)::int;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
SELECT count(*) FROM grr_f GROUP BY nosuch::int;

-- 7: the ordinary shapes around all three rules

-- begin-expected
-- columns: a | count
-- row: 10, 2
-- row: 20, 1
-- end-expected
SELECT a, count(*) FROM grr_t GROUP BY a ORDER BY 1;

-- begin-expected
-- columns: a | count
-- row: 10, 2
-- row: 20, 1
-- end-expected
SELECT a, count(*) FROM grr_t GROUP BY 1 ORDER BY 1;

-- begin-expected
-- columns: ?column? | count
-- row: 10, 2
-- row: 20, 1
-- end-expected
SELECT a + 0, count(*) FROM grr_t GROUP BY a + 0 ORDER BY 1;

-- begin-expected
-- columns: id | a | b
-- row: 1, 10, x
-- row: 2, 20, y
-- row: 3, 10, z
-- end-expected
SELECT id, a, b FROM grr_t GROUP BY id ORDER BY 1;

-- begin-expected
-- columns: a | c
-- row: 20, 1
-- row: 10, 2
-- end-expected
SELECT a, count(*) AS c FROM grr_t GROUP BY a ORDER BY c;

-- begin-expected
-- columns: a
-- row: 20
-- row: 10
-- end-expected
SELECT a FROM grr_t GROUP BY a ORDER BY count(*), a;

-- begin-expected
-- columns: a | count | row_number
-- row: 10, 2, 1
-- row: 20, 1, 2
-- end-expected
SELECT a, count(*), row_number() OVER (ORDER BY a) FROM grr_t GROUP BY a ORDER BY 1;

-- begin-expected
-- columns: a | count
-- row: 10, 1
-- row: 20, 1
-- end-expected
SELECT t.a, count(*) FROM grr_t t JOIN grr_u u ON t.id = u.id GROUP BY t.a ORDER BY 1;

-- begin-expected
-- columns: a | c
-- row: 10, 2
-- row: 20, 1
-- end-expected
SELECT t.a, x.c FROM (SELECT DISTINCT a FROM grr_t) t,
LATERAL (SELECT count(*) AS c FROM grr_t u WHERE u.a = t.a) x ORDER BY 1;

-- begin-expected
-- columns: s | c
-- row: a, 2
-- row: b, 1
-- end-expected
SELECT s, c FROM grr_v ORDER BY 1;

-- begin-expected
-- columns: a | c
-- row: 10, 2
-- row: 20, 1
-- end-expected
SELECT * FROM (SELECT a, count(*) c FROM grr_t GROUP BY a) q ORDER BY 1;

-- begin-expected
-- columns: a | c
-- row: 10, 2
-- row: 20, 1
-- end-expected
WITH g AS (SELECT a, count(*) c FROM grr_t GROUP BY a) SELECT * FROM g ORDER BY 1;

-- begin-expected
-- columns: a | count
-- row: 10, 1
-- row: 10, 2
-- row: 20, 1
-- row: 30, 1
-- end-expected
SELECT a, count(*) FROM grr_t GROUP BY a UNION SELECT a, count(*) FROM grr_u GROUP BY a ORDER BY 1, 2;

-- begin-expected
-- columns: rn
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- row: 5
-- end-expected
SELECT rn FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn FROM grr_ord) sub WHERE rn >= 1 ORDER BY 1;

-- begin-expected
-- columns: a | count
-- row: NULL, 3
-- row: 10, 2
-- row: 20, 1
-- end-expected
SELECT a, count(*) FROM grr_t GROUP BY ROLLUP (a) ORDER BY 1 NULLS FIRST;

-- begin-expected
-- columns: id | b
-- row: 1, x
-- row: 2, y
-- row: 3, z
-- end-expected
SELECT id, b FROM grr_t GROUP BY GROUPING SETS ((id)) ORDER BY 1;

-- begin-expected
-- columns: s | id
-- row: a, 1
-- row: a, 2
-- row: b, 3
-- end-expected
SELECT DISTINCT ON (s, id) s, id FROM grr_d d ORDER BY d.s, d.id;

-- begin-expected
-- columns: s | count
-- row: a, 2
-- row: b, 1
-- end-expected
SELECT DISTINCT s, count(*) FROM grr_d GROUP BY s ORDER BY 1;

-- begin-expected
-- columns: k | count
-- row: a, 2
-- row: b, 1
-- end-expected
SELECT DISTINCT s AS k, count(*) FROM grr_d GROUP BY s ORDER BY k;

-- 8: the grouping errors that belong, unchanged

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "grr_t.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT a, b FROM grr_t GROUP BY a;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "grr_t.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT a FROM grr_t GROUP BY a HAVING b > 'a';

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "grr_t.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT a, count(*) FROM grr_t GROUP BY a ORDER BY b;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "grr_t.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT id, b FROM grr_t GROUP BY ROLLUP (id);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "grr_t.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT a, row_number() OVER (ORDER BY b) FROM grr_t GROUP BY a;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "grr_t.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT a, count(*) OVER (PARTITION BY b) FROM grr_t GROUP BY a;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "grr_t.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT DISTINCT ON (b) a FROM grr_t GROUP BY a;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: arguments to GROUPING must be grouping expressions
-- end-expected-error
SELECT a, GROUPING(b) FROM grr_t GROUP BY a;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
SELECT count(*) FROM grr_t GROUP BY nosuch;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: GROUP BY position 9 is not in select list
-- end-expected-error
SELECT a FROM grr_t GROUP BY 9;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in GROUP BY
-- end-expected-error
SELECT count(*) FROM grr_t GROUP BY 'x';

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in GROUP BY
-- end-expected-error
SELECT a FROM grr_t GROUP BY count(*);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: ungrouped column "t.id" from outer query
-- end-expected-error
SELECT a, (SELECT count(*) FROM grr_u u WHERE u.a = t.id) FROM grr_t t GROUP BY a;

-- cleanup
DROP VIEW IF EXISTS grr_v CASCADE;
DROP TABLE IF EXISTS grr_f CASCADE;
DROP TABLE IF EXISTS grr_t CASCADE;
DROP TABLE IF EXISTS grr_u CASCADE;
DROP TABLE IF EXISTS grr_d CASCADE;
DROP TABLE IF EXISTS grr_cust CASCADE;
DROP TABLE IF EXISTS grr_ord CASCADE;
DROP TABLE IF EXISTS grr_w CASCADE;
DROP TABLE IF EXISTS grr_e CASCADE;
DROP TYPE IF EXISTS grr_mood CASCADE;
