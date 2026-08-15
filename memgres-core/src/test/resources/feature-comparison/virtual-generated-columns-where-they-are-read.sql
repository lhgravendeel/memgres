-- ============================================================================
-- A VIRTUAL generated column is worked out where it is read
--
-- PostgreSQL pulls a derived table, a view body and an inlined WITH item up
-- into the query that reads them, so a qualification written in the enclosing
-- query becomes a scan qualification on the base relation and the generation
-- expression is never evaluated for a row that qualification discards. The
-- generation expression below is 10/a, which raises 22012 for the row a = 0,
-- so every statement here is really asking "was the expression evaluated for
-- that row".
--
-- Every value was measured against PostgreSQL 18.
-- ============================================================================

CREATE TABLE wvg_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO wvg_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE wvg_o (a int, note text);
INSERT INTO wvg_o VALUES (5,'x'),(0,'y');
CREATE VIEW wvg_v AS SELECT * FROM wvg_g;

-- ============================================================================
-- The enclosing query's qualification reaches the relation under it
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM wvg_g) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT v.g FROM wvg_v v WHERE v.a = 5;

-- begin-expected
-- columns: a | k | g
-- row: 5 | five | 2
-- end-expected
SELECT * FROM wvg_v WHERE a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH c AS (SELECT * FROM wvg_g) SELECT g FROM c WHERE a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT a,k,g FROM wvg_g) s WHERE s.a = 5;

-- a column the derived table renames still carries the qualification down
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT a AS aa, k, g FROM wvg_g) s WHERE s.aa = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM wvg_g) s WHERE s.a = 5 AND s.k = 'five';

-- begin-expected
-- columns: g | k
-- row: 2 | five
-- end-expected
SELECT s.g, s.k FROM (SELECT * FROM wvg_g) s WHERE s.a > 1;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM wvg_g) s JOIN wvg_o o ON o.a = s.a WHERE s.a = 5;

-- an inner join's ON condition qualifies the derived relation the same way
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM wvg_g) s JOIN wvg_o o ON o.a = s.a AND s.a = 5;

-- begin-expected
-- columns: max
-- row: 2
-- end-expected
SELECT max(s.g) FROM (SELECT * FROM wvg_g) s WHERE s.a = 5;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(s.g) FROM (SELECT * FROM wvg_g) s WHERE s.a = 5;

-- begin-expected
-- columns: sum
-- row: 2
-- end-expected
SELECT sum(s.g) FROM (SELECT * FROM wvg_g) s WHERE s.a = 5 GROUP BY s.k;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM wvg_g) s WHERE s.a = 5 ORDER BY s.g;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM wvg_g) s WHERE s.a = 5 LIMIT 1;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH c AS (SELECT * FROM wvg_g) SELECT c.g FROM c JOIN wvg_o o ON o.a = c.a WHERE c.a = 5;

-- DISTINCT and GROUP BY inside the derived table do not stop it
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT DISTINCT a,k,g FROM wvg_g) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT a,k,g FROM wvg_g GROUP BY a,k,g) s WHERE s.a = 5;

-- each arm of a set operation takes the qualification
-- begin-expected
-- columns: g
-- row: 2
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM wvg_g UNION ALL SELECT * FROM wvg_g) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM wvg_g ORDER BY a) s WHERE s.a = 5;

CREATE TABLE wvg_t2 (g int);
INSERT INTO wvg_t2 SELECT s.g FROM (SELECT * FROM wvg_g) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT g FROM wvg_t2;

-- ============================================================================
-- Where PostgreSQL evaluates the expression for every row, it must keep raising
-- ============================================================================

-- nothing qualifies the derived relation at all
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT * FROM wvg_g) s;

-- a qualification NAMING the virtual column is itself a scan qualification
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.k FROM (SELECT * FROM wvg_g) s WHERE s.g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT v.k FROM wvg_v v WHERE v.g = 2;

-- a LIMIT or an OFFSET inside the derived table keeps the qualification above it
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT * FROM wvg_g LIMIT 10) s WHERE s.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT * FROM wvg_g OFFSET 0) s WHERE s.a = 5;

-- ORDER BY ... LIMIT above the derived table qualifies nothing
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT * FROM wvg_g) s ORDER BY s.a LIMIT 1;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.note FROM wvg_o o WHERE EXISTS (SELECT 1 FROM (SELECT * FROM wvg_g) s WHERE s.a = o.a AND s.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.note FROM wvg_o o, LATERAL (SELECT * FROM wvg_g x WHERE x.a = o.a) s WHERE s.g = 2;

-- a column nobody names is never worked out, with or without a qualification
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT * FROM wvg_g) s;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM wvg_v;

-- begin-expected
-- columns: k
-- row: five
-- row: zero
-- end-expected
SELECT s.k FROM (SELECT * FROM wvg_g) s;

-- begin-expected
-- columns: note
-- row: x
-- end-expected
SELECT o.note FROM wvg_o o WHERE o.a IN (SELECT s.a FROM (SELECT * FROM wvg_g) s WHERE s.a = 5);

-- ============================================================================
-- The three write paths that bring in a second relation read only the columns
-- the statement names of it
-- ============================================================================

MERGE INTO wvg_o o USING wvg_g t ON o.a = t.a WHEN MATCHED THEN UPDATE SET note = t.k;

-- begin-expected
-- columns: a | note
-- row: 0 | zero
-- row: 5 | five
-- end-expected
SELECT a, note FROM wvg_o ORDER BY a;

DELETE FROM wvg_o;
INSERT INTO wvg_o VALUES (5,'x'),(0,'y');
UPDATE wvg_o o SET note = 'u' FROM wvg_g t WHERE t.a = o.a;

-- begin-expected
-- columns: a | note
-- row: 0 | u
-- row: 5 | u
-- end-expected
SELECT a, note FROM wvg_o ORDER BY a;

DELETE FROM wvg_o;
INSERT INTO wvg_o VALUES (5,'x'),(0,'y');
DELETE FROM wvg_o o USING wvg_g t WHERE t.a = o.a AND o.a = 99;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | x
-- end-expected
SELECT a, note FROM wvg_o ORDER BY a;

-- a write path that does name the virtual column still evaluates it
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
MERGE INTO wvg_o o USING wvg_g t ON o.a = t.a AND t.g = 2 WHEN MATCHED THEN UPDATE SET note = 'm2';

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
DELETE FROM wvg_o o USING wvg_g t WHERE t.a = o.a AND t.g = 2;

-- and neither of them wrote anything
-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | x
-- end-expected
SELECT a, note FROM wvg_o ORDER BY a;

DROP VIEW wvg_v;
DROP TABLE wvg_t2;
DROP TABLE wvg_o;
DROP TABLE wvg_g;

-- ============================================================================
-- Pushing a qualification down decides only WHICH rows the column is worked
-- out for; it never changes a value. This generation expression cannot raise,
-- so every row that comes back must carry the right one.
-- ============================================================================

CREATE TABLE wvg_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL);
INSERT INTO wvg_h VALUES (1,'one'),(2,'two'),(3,'three');
CREATE TABLE wvg_p (a int, note text);
INSERT INTO wvg_p VALUES (1,'p1'),(9,'p9');
CREATE VIEW wvg_hv AS SELECT * FROM wvg_h;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 2 | 20
-- end-expected
SELECT s.a, s.g FROM (SELECT * FROM wvg_h) s WHERE s.a = 1 OR s.k = 'two' ORDER BY s.a;

-- begin-expected
-- columns: a | g
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT s.a, s.g FROM (SELECT * FROM wvg_h) s WHERE NOT (s.a = 1) ORDER BY s.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 3 | 30
-- end-expected
SELECT s.a, s.g FROM (SELECT * FROM wvg_h) s WHERE s.a <> 2 ORDER BY s.a;

-- an outer join's null-padded row has no value to work out
-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 9 | NULL
-- end-expected
SELECT p.a, s.g FROM wvg_p p LEFT JOIN (SELECT * FROM wvg_h) s ON p.a = s.a ORDER BY p.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 9 | NULL
-- end-expected
SELECT p.a, s.g FROM wvg_p p LEFT JOIN (SELECT * FROM wvg_h) s ON p.a = s.a WHERE p.a >= 1 ORDER BY p.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: NULL | 20
-- row: NULL | 30
-- end-expected
SELECT p.a, s.g FROM wvg_p p RIGHT JOIN (SELECT * FROM wvg_h) s ON p.a = s.a ORDER BY s.a;

-- a qualification holding a function call drops no row, and the value stands
-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
SELECT s.a, s.g FROM (SELECT * FROM wvg_h) s WHERE s.a = abs(-1) ORDER BY s.a;

-- nor does one holding a subquery
-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
SELECT s.a, s.g FROM (SELECT * FROM wvg_h) s WHERE s.a = (SELECT min(a) FROM wvg_h) ORDER BY s.a;

-- a WITH item read twice answers each reference under its own qualification
-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 2 | 20
-- end-expected
WITH c AS (SELECT * FROM wvg_h) SELECT c.a, c.g FROM c WHERE c.a = 1 UNION ALL SELECT c.a, c.g FROM c WHERE c.a = 2;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
WITH c AS (SELECT * FROM wvg_h) SELECT c.a, c.g FROM c ORDER BY c.a;

-- begin-expected
-- columns: a | g
-- row: 2 | 20
-- end-expected
SELECT v.a, v.g FROM wvg_hv v WHERE v.a = 2;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT v.a, v.g FROM wvg_hv v ORDER BY v.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
SELECT s.a, s.g FROM (SELECT * FROM wvg_h) s, wvg_p p WHERE s.a = p.a ORDER BY s.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 3 | 30
-- end-expected
SELECT s.a, s.g FROM (SELECT * FROM wvg_h) s WHERE s.a IN (1,3) ORDER BY s.a;

-- begin-expected
-- columns: a | g
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT s.a, s.g FROM (SELECT * FROM wvg_h) s WHERE s.k LIKE 't%' ORDER BY s.a;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (SELECT * FROM wvg_h) s WHERE s.g = 20;

UPDATE wvg_p p SET note = t.k FROM wvg_h t WHERE t.a = p.a;

-- begin-expected
-- columns: a | note
-- row: 1 | one
-- row: 9 | p9
-- end-expected
SELECT a, note FROM wvg_p ORDER BY a;

MERGE INTO wvg_p p USING wvg_h t ON p.a = t.a WHEN MATCHED THEN UPDATE SET note = t.g::text;

-- begin-expected
-- columns: a | note
-- row: 1 | 10
-- row: 9 | p9
-- end-expected
SELECT a, note FROM wvg_p ORDER BY a;

DROP VIEW wvg_hv;
DROP TABLE wvg_p;
DROP TABLE wvg_h;
