-- ============================================================================
-- A qualification reaching down a chain of derived relations, an OR whose
-- parts say the same thing, and the rows a kept-apart item is ever asked for
--
-- PostgreSQL pulls a whole chain of derived tables, views and inlined WITH
-- items up into the query that reads them, so that query's qualification
-- becomes a qualification of the scan underneath and a VIRTUAL generated
-- column is worked out only for the rows it keeps. That holds however many
-- levels the chain has and however each level writes its select list: a bare
-- star, a star written with the relation before it, or the columns named one
-- by one.
--
-- What a level names inside a derived table of its own is that table's own
-- business. Read as the reading query's, a column named only there was worked
-- out for every row the scan under it passed over -- and a column the chain
-- does not expose is not a column of the relation above at all.
--
-- PostgreSQL factors an OR whose branches are the same comparison back into
-- that comparison, so it restricts exactly what one branch restricts, and a
-- restriction on the outer relation reaches a LATERAL item tied to it by an
-- equality.
--
-- An item PostgreSQL keeps apart is computed when the query above first asks
-- it for a row. A qualification that is false before any row is read, and a
-- LIMIT of none, mean it is never asked at all. A LIMIT stops the scan under
-- it in the same way, so a row past the limit is never produced -- unless
-- something that has to see every row first, a sort or a DISTINCT among them,
-- stands in between.
--
-- The generation expression below is 10/a over a relation holding a row with
-- a = 0, so every statement here is really asking which rows it was worked out
-- for. Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE qdc_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO qdc_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE VIEW qdc_v AS SELECT * FROM qdc_g;
CREATE TABLE qdc_o (a int);
INSERT INTO qdc_o VALUES (5);
CREATE TABLE qdc_p (a int, note text);
INSERT INTO qdc_p VALUES (5,'x'),(0,'y');

-- ============================================================================
-- A star written with the relation before it carries the qualification down
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT s2.* FROM qdc_g s2) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT s2.* FROM (SELECT * FROM qdc_g) s2) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT s2.* FROM (SELECT s3.* FROM qdc_g s3) s2) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT v.* FROM qdc_v v) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM (SELECT v.* FROM qdc_v v) s2) s WHERE s.a = 5;

-- whichever column the qualification names, and whatever stands above the chain
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT s2.* FROM qdc_g s2) s WHERE s.k = 'five';

-- begin-expected
-- columns: max
-- row: 2
-- end-expected
SELECT max(s.g) FROM (SELECT s2.* FROM qdc_g s2) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT s2.* FROM qdc_g s2) s WHERE s.a = 5 LIMIT 1;

-- a qualification written at the chain's own level reaches the same scan
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT s2.* FROM qdc_g s2 WHERE s2.a = 5) s;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT s2.* FROM qdc_g s2) s JOIN qdc_p o ON o.a = s.a WHERE s.a = 5;

-- ============================================================================
-- The columns named one by one carry it down too
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT a, k, g FROM qdc_g) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT s2.a, s2.k, s2.g FROM qdc_g s2) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM (SELECT a, k, g FROM qdc_g) s2) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT a, k, g FROM (SELECT * FROM qdc_g) s2) s WHERE s.a = 5;

-- the order the list writes the columns in is its own business
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM (SELECT a, g, k FROM qdc_g) s2) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT a, k, g FROM qdc_v) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT t.g FROM (SELECT s.* FROM (SELECT * FROM (SELECT a, k, g FROM qdc_g) r) s) t WHERE t.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT a, k, g FROM qdc_g) s JOIN qdc_p o ON o.a = s.a WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT a, k, g FROM qdc_g WHERE a = 5) s;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM (SELECT * FROM qdc_g) s2) s WHERE s.a = 5;

-- ============================================================================
-- What a level of the chain names is its own relation's columns
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 5
-- row: 0
-- end-expected
SELECT s.a FROM (SELECT a, k, g FROM qdc_g) s ORDER BY s.a DESC;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT a, k, g FROM qdc_g) s;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT * FROM (SELECT a, k, g FROM qdc_g) s2) s;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT s2.* FROM qdc_g s2) s;

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT s.k FROM (SELECT * FROM (SELECT a, k, g FROM qdc_g) s2) s WHERE s.a = 5;

-- a column the chain does not expose is not a column of the relation above
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.g does not exist
-- end-expected-error
SELECT s.g FROM (SELECT * FROM (SELECT a, k FROM qdc_g) s2) s WHERE s.a = 5;

-- ============================================================================
-- With nothing narrowing it every row of the chain is worked out
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT s2.* FROM qdc_g s2) s;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT a, k, g FROM qdc_g) s;

-- a qualification naming the generated column is a scan qualification, and
-- deciding it works the column out for every row scanned
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.k FROM (SELECT s2.* FROM qdc_g s2) s WHERE s.g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.k FROM (SELECT a, k, g FROM qdc_g) s WHERE s.g = 2;

-- one side of an OR does not decide the row, so neither is read first
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT s2.* FROM qdc_g s2) s WHERE s.a = 5 OR s.g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT a, k, g FROM qdc_g) s WHERE s.a = 5 OR s.g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT * FROM (SELECT * FROM qdc_g) s2) s WHERE s.a = 5 OR s.g = 2;

-- an OR of parts naming different constants restricts neither
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT a, k, g FROM qdc_g) s WHERE s.a = 5 OR s.a = 0;

-- a LIMIT settles which rows the relation has before the qualification above
-- is read
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT s2.* FROM qdc_g s2 LIMIT 2) s WHERE s.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT a, k, g FROM qdc_g LIMIT 2) s WHERE s.a = 5;

-- ============================================================================
-- An OR whose parts say the same thing restricts what one of them restricts
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT g FROM qdc_g WHERE a = 5 OR a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM qdc_g) s WHERE s.a = 5 OR s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT s2.* FROM qdc_g s2) s WHERE s.a = 5 OR s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT a, k, g FROM qdc_g) s WHERE s.a = 5 OR s.a = 5;

-- and a restriction on the outer relation reaches a LATERAL item tied to it
-- by an equality
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM qdc_o o LEFT JOIN LATERAL (SELECT * FROM qdc_g z WHERE z.a = o.a) s
  ON s.g = 2 WHERE o.a = 5 OR o.a = 5;

-- begin-expected
-- columns: a | g
-- row: 5 | 2
-- end-expected
SELECT o.a, s.g FROM qdc_o o LEFT JOIN LATERAL (SELECT * FROM qdc_g z WHERE z.a = o.a) s
  ON s.g = 2 WHERE o.a = 5 OR o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM qdc_o o JOIN LATERAL (SELECT * FROM qdc_g z WHERE z.a = o.a) s
  ON s.g = 2 WHERE o.a = 5 OR o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM qdc_o o, LATERAL (SELECT * FROM qdc_g z WHERE z.a = o.a) s
  WHERE (o.a = 5 OR o.a = 5) AND s.g = 2;

-- however many branches say it, and beside a part that says it again
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM qdc_o o LEFT JOIN LATERAL (SELECT * FROM qdc_g z WHERE z.a = o.a) s
  ON s.g = 2 WHERE o.a = 5 OR o.a = 5 OR o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM qdc_o o LEFT JOIN LATERAL (SELECT * FROM qdc_g z WHERE z.a = o.a) s
  ON s.g = 2 WHERE (o.a = 5 OR o.a = 5) AND o.a = 5;

-- the select list need not name the column the condition reads, and an item
-- writing its columns out is read the same way
-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT s.k FROM qdc_o o LEFT JOIN LATERAL (SELECT * FROM qdc_g z WHERE z.a = o.a) s
  ON s.g = 2 WHERE o.a = 5 OR o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM qdc_o o LEFT JOIN LATERAL (SELECT z.a, z.k, z.g FROM qdc_g z WHERE z.a = o.a) s
  ON s.g = 2 WHERE o.a = 5 OR o.a = 5;

-- ============================================================================
-- A kept-apart item the query above can answer without is never computed
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT count(*) FROM c WHERE false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT count(*) FROM c WHERE 1=0;

-- begin-expected
-- columns: count
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT count(*) FROM c LIMIT 0;

-- begin-expected
-- columns: count
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT count(*) FROM c LIMIT 0 OFFSET 0;

-- begin-expected
-- columns: k
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT c.k FROM c WHERE false;

-- begin-expected
-- columns: k
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT c.k FROM c WHERE 1=0 ORDER BY c.k;

-- begin-expected
-- columns: a
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT c.a FROM c WHERE false ORDER BY c.a;

-- begin-expected
-- columns: max
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT max(c.a) FROM c WHERE false LIMIT 0;

-- whatever the query puts between itself and the item
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT count(*) FROM c JOIN qdc_p o ON o.a = c.a WHERE false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT count(*) FROM c CROSS JOIN c c2 WHERE false;

-- a qualification decided row by row is a row asked for
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT count(*) FROM c WHERE c.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT count(*) FROM c;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT count(*) FROM c WHERE c.a = 5 OR c.g = 2;

-- a qualification that is NULL rather than false is still decided row by row
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g) SELECT count(*) FROM c WHERE null;

-- ============================================================================
-- A LIMIT stops the scan under it
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g LIMIT 1) SELECT count(*) FROM c;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g LIMIT 1 OFFSET 0) SELECT count(*) FROM c;

-- begin-expected
-- columns: a | k | g
-- row: 5 | five | 2
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g LIMIT 1) SELECT c.a, c.k, c.g FROM c;

-- begin-expected
-- columns: k
-- row: five
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g LIMIT 1) SELECT c.k FROM c;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_g LIMIT 1) SELECT count(*) FROM c WHERE c.a = 5;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (SELECT * FROM qdc_g LIMIT 1) s;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (SELECT * FROM qdc_g LIMIT 1) s WHERE s.a = 5;

-- the same limit read straight off the relation
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT g FROM qdc_g LIMIT 1;

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT k FROM qdc_g LIMIT 1;

-- begin-expected
-- columns: a | k | g
-- row: 5 | five | 2
-- end-expected
SELECT * FROM qdc_g LIMIT 1;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT g FROM qdc_g LIMIT 1 OFFSET 0;

-- a sort has to be given every row before the limit can pick one
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM qdc_g ORDER BY a LIMIT 1) SELECT count(*) FROM c;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT * FROM qdc_g ORDER BY a LIMIT 1;

-- and so does a DISTINCT
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT DISTINCT * FROM qdc_g LIMIT 1;

-- a limit that reaches the row is no limit at all
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT * FROM qdc_g LIMIT 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM qdc_g LIMIT 2) SELECT count(*) FROM c;

-- an OFFSET reads the rows it skips
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM qdc_g OFFSET 1) SELECT count(*) FROM c;

-- an item naming only the stored columns is untouched by any of it
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH c AS MATERIALIZED (SELECT a, k FROM qdc_g) SELECT count(*) FROM c;

-- ============================================================================
-- None of it changes a value
-- ============================================================================

-- setup
CREATE TABLE qdc_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL);
INSERT INTO qdc_h VALUES (1,'one'),(2,'two'),(3,'three');
CREATE TABLE qdc_q (a int, note text);
INSERT INTO qdc_q VALUES (1,'x'),(2,'y'),(3,'z');

-- begin-expected
-- columns: a | g
-- row: 2 | 20
-- end-expected
SELECT s.a, s.g FROM (SELECT s2.* FROM qdc_h s2) s WHERE s.a = 2;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT s.a, s.g FROM (SELECT a, k, g FROM qdc_h) s ORDER BY s.a;

-- begin-expected
-- columns: a | g
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT s.a, s.g FROM (SELECT * FROM (SELECT a, k, g FROM qdc_h) s2) s WHERE s.a > 1 ORDER BY s.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_h) SELECT c.a, c.g FROM c ORDER BY c.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM qdc_h LIMIT 1) SELECT c.a, c.g FROM c;

-- begin-expected
-- columns: a | g
-- row: 2 | 20
-- end-expected
SELECT o.a, s.g FROM qdc_q o LEFT JOIN LATERAL (SELECT * FROM qdc_h z WHERE z.a = o.a) s
  ON s.g = 20 WHERE o.a = 2 OR o.a = 2;

-- begin-expected
-- columns: g
-- row: 10
-- end-expected
SELECT g FROM qdc_h WHERE a = 1 OR a = 1;

-- begin-expected
-- columns: g
-- row: 20
-- end-expected
SELECT s.g FROM (SELECT * FROM qdc_h) s WHERE s.a = 2 OR s.a = 2;

-- cleanup
DROP TABLE qdc_q;
DROP TABLE qdc_h;
DROP TABLE qdc_p;
DROP TABLE qdc_o;
DROP VIEW qdc_v;
DROP TABLE qdc_g;
