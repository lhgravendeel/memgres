-- ============================================================================
-- What a WITH item the query keeps apart holds, and the order a qualification
-- is read in
--
-- An item PostgreSQL keeps apart from the query reading it -- written
-- MATERIALIZED, named twice, holding a volatile call -- is computed in full
-- before that query is planned. What it holds is its own select list, and a
-- star there stands for every column of the relation underneath, a VIRTUAL
-- generated one included, whether or not the query above ever names it. An item
-- PostgreSQL pulls up takes the reading query's demand instead, and so does a
-- chain of derived relations, however many levels deep and whether the bottom
-- of it is a relation or a view.
--
-- PostgreSQL also orders the parts of a qualification by what each costs to
-- evaluate and stops at the first that is false. A part holding a sub-query is
-- a plan of its own and costs more than any comparison of values the row
-- already carries, so the plain parts decide the row first and the sub-query
-- never runs for a row they reject.
--
-- The generation expression below is 10/a over a relation holding a row with
-- a = 0, so every statement here is really asking which rows it was worked out
-- for. Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE kap_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO kap_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE kap_o (a int, note text);
INSERT INTO kap_o VALUES (5,'x'),(0,'y');
CREATE TABLE kap_n (a int);
INSERT INTO kap_n VALUES (5),(0),(7),(9);
CREATE VIEW kap_v AS SELECT * FROM kap_g;
CREATE VIEW kap_v2 AS SELECT * FROM kap_v;

-- ============================================================================
-- An item kept apart works out what its own star exposes
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM kap_g) SELECT count(*) FROM c;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM kap_g) SELECT k FROM c ORDER BY k;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM kap_g) SELECT a FROM c ORDER BY a;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM kap_g) SELECT g FROM c WHERE a = 5;

-- a qualification written above an item kept apart reaches nothing below it
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM kap_g) SELECT count(*) FROM c WHERE a = 5;

-- the item's own column names change nothing about what it holds
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c(x,y,z) AS MATERIALIZED (SELECT * FROM kap_g) SELECT count(*) FROM c;

-- a derived table, a view and a further item under a kept-apart one are all
-- pulled up into it, so the star reaches through them
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM (SELECT * FROM kap_g) s) SELECT count(*) FROM c;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM kap_v) SELECT count(*) FROM c;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM kap_g), d AS (SELECT * FROM c) SELECT count(*) FROM d;

-- whatever the query above puts between itself and the item
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM kap_g) SELECT count(*) FROM (SELECT * FROM c) t;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM kap_g) SELECT count(*) FROM c LEFT JOIN kap_o o ON o.a = c.a;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM kap_g) SELECT count(*) FROM c UNION ALL SELECT count(*) FROM kap_o;

-- an arm of a set operation is part of the same query, so a star on one is the
-- whole operation's
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM kap_g WHERE a = 5 UNION ALL SELECT * FROM kap_g WHERE a = 0) SELECT count(*) FROM c;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM kap_g WHERE a = 5 UNION ALL SELECT * FROM kap_g WHERE a = 5) SELECT count(*) FROM c;

-- ============================================================================
-- What the item names, and what its own qualification keeps
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH c AS MATERIALIZED (SELECT a, k FROM kap_g) SELECT count(*) FROM c;

-- begin-expected
-- columns: k
-- row: five
-- row: zero
-- end-expected
WITH c AS MATERIALIZED (SELECT a, k FROM kap_g) SELECT k FROM c ORDER BY k;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
WITH c AS MATERIALIZED (SELECT a, k FROM kap_g) SELECT c.a FROM c WHERE c.a = 5;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH c AS MATERIALIZED (SELECT a, k FROM kap_g) SELECT count(*) FROM c WHERE a = 0;

-- an aggregate is one value and reads nothing of the row
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
WITH c AS MATERIALIZED (SELECT count(*) AS n FROM kap_g) SELECT n FROM c;

-- what the item does name, it works out
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT g FROM kap_g) SELECT count(*) FROM c;

-- the item's own qualification settles which rows it holds
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM kap_g WHERE a = 5) SELECT count(*) FROM c;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM kap_g WHERE a = 5) SELECT g FROM c;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH c AS MATERIALIZED (SELECT g FROM kap_g WHERE a = 5) SELECT count(*) FROM c;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH c AS MATERIALIZED (SELECT DISTINCT a, k, g FROM kap_g WHERE a = 5) SELECT count(*) FROM c;

-- the expression written out by hand is the item's own to work out, over its
-- own rows
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH c AS MATERIALIZED (SELECT a, k, 10/a AS h FROM kap_g WHERE a = 5) SELECT count(*) FROM c;

-- EXISTS asks for no column at all, so a star under it exposes nothing
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH c AS MATERIALIZED (SELECT a FROM kap_g WHERE EXISTS (SELECT * FROM kap_g z)) SELECT count(*) FROM c;

-- ============================================================================
-- An item the query pulls up takes the reading query's demand instead
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH c AS NOT MATERIALIZED (SELECT * FROM kap_g) SELECT count(*) FROM c;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH c AS (SELECT * FROM kap_g) SELECT count(*) FROM c;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH RECURSIVE c AS (SELECT * FROM kap_g) SELECT count(*) FROM c;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH c AS (SELECT * FROM kap_g) SELECT g FROM c WHERE a = 5;

-- named twice, PostgreSQL keeps it apart however it was written
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS (SELECT * FROM kap_g) SELECT count(*) FROM c, c c2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM kap_g) SELECT count(*) FROM c, c c2;

-- written NOT MATERIALIZED it is pulled into both places
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
WITH c AS NOT MATERIALIZED (SELECT * FROM kap_g) SELECT count(*) FROM c, c c2;

-- a volatile call keeps it apart however it is written
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS (SELECT random(), * FROM kap_g) SELECT count(*) FROM c;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS NOT MATERIALIZED (SELECT random(), * FROM kap_g) SELECT count(*) FROM c;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS (SELECT * FROM kap_g WHERE random() < 2) SELECT count(*) FROM c;

-- now() is stable rather than volatile, so the item is still pulled up
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH c AS (SELECT now()::text AS n, * FROM kap_g) SELECT g FROM c WHERE a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH c AS (SELECT * FROM kap_g), d AS (SELECT * FROM c) SELECT g FROM d WHERE a = 5;

-- an item nobody reads is not computed, and a relation with no generated column
-- is untouched by any of it
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM kap_g) SELECT count(*) FROM kap_o;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM kap_o) SELECT count(*) FROM c;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM kap_o) SELECT count(*) FROM c WHERE a = 5;

-- ============================================================================
-- A qualification reaches through a chain of derived relations
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT t.g FROM (SELECT * FROM (SELECT * FROM kap_g) s) t WHERE t.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT t.g FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM kap_g) r) s) t WHERE t.a = 5;

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT t.k FROM (SELECT * FROM (SELECT * FROM kap_g) s) t WHERE t.a = 5;

-- begin-expected
-- columns: a | g
-- row: 5 | 2
-- end-expected
SELECT t.a, t.g FROM (SELECT * FROM (SELECT * FROM kap_g) s) t WHERE t.k = 'five';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT t.g FROM (SELECT * FROM (SELECT * FROM kap_g) s) t WHERE t.a = 5 AND t.g = 2;

-- begin-expected
-- columns: max
-- row: 2
-- end-expected
SELECT max(t.g) FROM (SELECT * FROM (SELECT * FROM kap_g) s) t WHERE t.a = 5;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT * FROM (SELECT * FROM kap_g) s) t;

-- a qualification written at either level of the chain reaches the same scan
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT t.g FROM (SELECT * FROM (SELECT * FROM kap_g) s WHERE s.a = 5) t;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT t.g FROM (SELECT * FROM (SELECT * FROM kap_g) s WHERE s.k = 'five') t;

-- a derived table over a view, and a view over a view
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM kap_v) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM kap_v2) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM kap_v2 s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT v.g FROM kap_v2 v WHERE v.k = 'five';

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT s.k FROM (SELECT * FROM kap_v) s WHERE s.a = 5;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT * FROM kap_v) s;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM kap_v WHERE a = 5) s;

-- with nothing narrowing the chain, every row of it reaches the expression
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT t.g FROM (SELECT * FROM (SELECT * FROM kap_g) s) t;

-- a LIMIT settles which rows the relation has before the qualification above it
-- is read
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT t.g FROM (SELECT * FROM (SELECT * FROM kap_g LIMIT 2) s) t WHERE t.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT * FROM kap_v) s;

-- ============================================================================
-- A plain part of a qualification is read before a part holding a sub-query
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o WHERE EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o WHERE o.a = 5 AND EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2);

-- begin-expected
-- columns: a | note
-- row: 5 | x
-- end-expected
SELECT o.a, o.note FROM kap_o o WHERE EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o WHERE NOT EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 3) AND o.a = 5;

-- a sub-query standing as a value costs as much as one standing as a predicate
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o WHERE (SELECT max(s.g) FROM kap_g s WHERE s.a = o.a) = 2 AND o.a = 5;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o WHERE (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2) = 1 AND o.a = 5;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o WHERE (SELECT count(*) FROM kap_g s WHERE s.a = o.a AND s.g = 2) = 1 AND o.a = 5;

-- the reading is the same beside more parts, and above a grouping or a sort
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o WHERE EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5 AND o.note = 'x';

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o WHERE o.a = 5 AND o.note = 'x' AND EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2);

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o WHERE o.note = 'x' AND EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM kap_o o WHERE EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o WHERE EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5 GROUP BY o.a;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o WHERE EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5 ORDER BY o.a;

-- a second relation beside it, and a join, do not change the reading
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o, kap_o p WHERE EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5 AND p.a = 5;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM kap_o o JOIN kap_o p ON p.a = o.a WHERE EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5;

-- with nothing to reject the row first, the sub-query runs for every row
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM kap_o o WHERE EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2);

-- one side of an OR is not a part that has to hold, so it decides nothing first
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM kap_o o WHERE EXISTS (SELECT 1 FROM kap_g s WHERE s.a = o.a AND s.g = 2) OR o.a = 5;

-- the same reading holds where it is the sub-query itself that would raise
-- begin-expected
-- columns: a
-- end-expected
SELECT o.a FROM kap_o o WHERE EXISTS (SELECT 1/0) AND o.a = 99;

-- begin-expected
-- columns: a
-- end-expected
SELECT o.a FROM kap_o o WHERE o.a = 99 AND EXISTS (SELECT 1/0);

-- begin-expected
-- columns: a
-- row: 7
-- row: 9
-- end-expected
SELECT n.a FROM kap_n n WHERE EXISTS (SELECT 1/(n.a-5)) AND n.a > 5 ORDER BY n.a;

-- begin-expected
-- columns: a
-- row: 7
-- row: 9
-- end-expected
SELECT n.a FROM kap_n n WHERE n.a > 5 AND EXISTS (SELECT 1/(n.a-5)) ORDER BY n.a;

-- begin-expected
-- columns: a
-- row: 7
-- row: 9
-- end-expected
SELECT n.a FROM kap_n n WHERE EXISTS (SELECT 1/(n.a-5)) AND n.a IN (7,9) ORDER BY n.a;

-- begin-expected
-- columns: a
-- row: 7
-- row: 9
-- end-expected
SELECT n.a FROM kap_n n WHERE EXISTS (SELECT 1/(n.a-5)) AND n.a <> 5 AND n.a <> 0 ORDER BY n.a;

-- begin-expected
-- columns: a
-- row: 7
-- end-expected
SELECT n.a FROM kap_n n WHERE n.a > 5 AND EXISTS (SELECT 1/(n.a-5)) AND n.a < 9;

-- begin-expected
-- columns: a
-- end-expected
SELECT n.a FROM kap_n n WHERE NOT EXISTS (SELECT 1/(n.a-5)) AND n.a > 5 ORDER BY n.a;

-- and a part that raises is not made cheap by a sub-query standing beside it
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT n.a FROM kap_n n WHERE EXISTS (SELECT 1 FROM kap_o z WHERE z.a = n.a) AND 10/n.a = 2;

-- ============================================================================
-- None of it changes the value the column carries
--
-- The generation expression below cannot raise, so every row that comes back
-- has to carry the right one.
-- ============================================================================

CREATE TABLE kap_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL);
INSERT INTO kap_h VALUES (1,'one'),(2,'two'),(3,'three');
CREATE TABLE kap_q (a int, note text);
INSERT INTO kap_q VALUES (1,'p1'),(9,'p9');
CREATE VIEW kap_hv AS SELECT * FROM kap_h;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM kap_h) SELECT a, g FROM c ORDER BY a;

-- begin-expected
-- columns: a | g
-- row: 2 | 20
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM kap_h) SELECT a, g FROM c WHERE a = 2;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
WITH c AS NOT MATERIALIZED (SELECT * FROM kap_h) SELECT a, g FROM c ORDER BY a;

-- begin-expected
-- columns: x | z
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
WITH c(x,y,z) AS MATERIALIZED (SELECT * FROM kap_h) SELECT x, z FROM c ORDER BY x;

-- begin-expected
-- columns: count | sum
-- row: 3 | 60
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM kap_h) SELECT count(*), sum(g) FROM c;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM kap_h) SELECT c.a, c.g FROM c JOIN kap_q q ON q.a = c.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT t.a, t.g FROM (SELECT * FROM (SELECT * FROM kap_h) s) t ORDER BY t.a;

-- begin-expected
-- columns: a | g
-- row: 2 | 20
-- end-expected
SELECT t.a, t.g FROM (SELECT * FROM (SELECT * FROM kap_h) s) t WHERE t.a = 2;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT s.a, s.g FROM (SELECT * FROM kap_hv) s ORDER BY s.a;

-- begin-expected
-- columns: a | g
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT s.a, s.g FROM (SELECT * FROM kap_hv) s WHERE s.a > 1 ORDER BY s.a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT o.a FROM kap_q o WHERE EXISTS (SELECT 1 FROM kap_h s WHERE s.a = o.a AND s.g = 10) AND o.a = 1;

-- cleanup
DROP VIEW kap_hv;
DROP VIEW kap_v2;
DROP VIEW kap_v;
DROP TABLE kap_g;
DROP TABLE kap_o;
DROP TABLE kap_n;
DROP TABLE kap_h;
DROP TABLE kap_q;
