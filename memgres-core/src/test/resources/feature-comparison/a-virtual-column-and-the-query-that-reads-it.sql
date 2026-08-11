-- ============================================================================
-- A VIRTUAL generated column is worked out where the reference stands
--
-- PostgreSQL pulls a WITH item, a derived table and a view body up into the
-- query that reads them, so a qualification written above becomes a scan
-- qualification and the rows that qualification discards never reach the
-- generation expression. An item written MATERIALIZED, named more than once,
-- or holding a volatile call is computed on its own instead, and every row of
-- it is reached.
--
-- The generation expression below is 10/a over a relation holding a row with
-- a = 0, so every statement here is really asking which rows it was worked out
-- for. Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE vqc_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO vqc_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE vqc_o (a int, note text);
INSERT INTO vqc_o VALUES (5,'x'),(0,'y');
CREATE VIEW vqc_v AS WITH c AS (SELECT * FROM vqc_g) SELECT * FROM c;

-- ============================================================================
-- A WITH item is pulled up when the query names it once and it is left alone
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH c AS (SELECT * FROM vqc_g) SELECT g FROM c WHERE a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH c AS NOT MATERIALIZED (SELECT * FROM vqc_g) SELECT g FROM c WHERE a = 5;

-- RECURSIVE written but never used leaves the item an ordinary one
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH RECURSIVE c AS (SELECT * FROM vqc_g) SELECT g FROM c WHERE a = 5;

-- the qualification reaches through one item into another
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH c AS (SELECT * FROM vqc_g), d AS (SELECT * FROM c) SELECT g FROM d WHERE a = 5;

-- now() is stable, not volatile, so the item is still pulled up
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH c AS (SELECT now()::text AS n, * FROM vqc_g) SELECT g FROM c WHERE a = 5;

-- one arm of a set operation naming the item is still naming it once
-- begin-expected
-- columns: g
-- row: 2
-- row: 1
-- end-expected
WITH c AS (SELECT * FROM vqc_g) SELECT g FROM c WHERE a = 5 UNION ALL SELECT 1;

-- written NOT MATERIALIZED it is pulled up into both references
-- begin-expected
-- columns: g
-- row: 2
-- row: 2
-- end-expected
WITH c AS NOT MATERIALIZED (SELECT * FROM vqc_g) SELECT g FROM c WHERE a = 5
UNION ALL SELECT g FROM c WHERE a = 5;

-- and a view whose body reads an item takes the qualification through both
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT g FROM vqc_v WHERE a = 5;

-- ============================================================================
-- An item the query keeps apart is worked out over every row of its own
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM vqc_g) SELECT g FROM c WHERE a = 5;

-- named twice, the item is computed on its own
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS (SELECT * FROM vqc_g) SELECT g FROM c WHERE a = 5
UNION ALL SELECT g FROM c WHERE a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM vqc_g) SELECT g FROM c WHERE a = 5
UNION ALL SELECT g FROM c WHERE a = 5;

-- a volatile call in the item's select list keeps it apart
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS (SELECT *, random() AS r FROM vqc_g) SELECT g FROM c WHERE a = 5;

-- and so does one in the item's own qualification
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS (SELECT * FROM vqc_g WHERE random() < 2) SELECT g FROM c WHERE a = 5;

-- NOT MATERIALIZED does not override a volatile call
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS NOT MATERIALIZED (SELECT *, random() AS r FROM vqc_g)
SELECT g FROM c WHERE a = 5;

-- an item nobody reads the generated column of is read whole
-- begin-expected
-- columns: k
-- row: five
-- row: zero
-- end-expected
WITH c AS (SELECT * FROM vqc_g) SELECT k FROM c ORDER BY k;

-- ============================================================================
-- The qualification's parts decide the row before a generation expression does
-- ============================================================================

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT k FROM vqc_g WHERE a = 5 AND g = 2;

-- written the other way round, the stored column still decides first
-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT k FROM vqc_g WHERE g = 2 AND a = 5;

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT k FROM vqc_g WHERE a = 5 AND g = 2 AND k = 'five';

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT k FROM vqc_g WHERE a <> 0 AND g = 2;

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT k FROM vqc_g WHERE k = 'five' AND g = 2;

-- the same qualification written out rather than through the column
-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT k FROM vqc_g WHERE a = 5 AND 10/a = 2;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM vqc_g WHERE a = 5 AND g = 2;

-- the column is read again above the qualification
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT a FROM vqc_g WHERE a = 5 AND g = 2 ORDER BY g;

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT s.k FROM (SELECT * FROM vqc_g) s WHERE s.a = 5 AND s.g = 2;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM vqc_g) s WHERE s.a = 5 AND s.g = 2;

-- one side of an OR decides nothing on its own
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT k FROM vqc_g WHERE a = 5 OR g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT k FROM vqc_g WHERE g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT * FROM vqc_g) s WHERE s.g = 2;

-- ============================================================================
-- The same on the write paths
-- ============================================================================

UPDATE vqc_g SET k = 'F' WHERE a = 5 AND g = 2;

-- begin-expected
-- columns: a | k
-- row: 0 | zero
-- row: 5 | F
-- end-expected
SELECT a, k FROM vqc_g ORDER BY a;

DELETE FROM vqc_g WHERE a = 99 AND g = 2;

-- begin-expected
-- columns: a | k
-- row: 0 | zero
-- row: 5 | F
-- end-expected
SELECT a, k FROM vqc_g ORDER BY a;

-- begin-expected
-- columns: a | k
-- row: 5 | five
-- end-expected
UPDATE vqc_g SET k = 'five' WHERE a = 5 AND g = 2 RETURNING a, k;

-- a generated column of the relation the statement brought in is worked out
-- above the join that kept its row
UPDATE vqc_o o SET note = t.g::text FROM vqc_g t WHERE t.a = o.a AND o.a = 5;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | 2
-- end-expected
SELECT a, note FROM vqc_o ORDER BY a;

-- with nothing narrowing the join, every paired row reaches it
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
UPDATE vqc_o o SET note = t.g::text FROM vqc_g t WHERE t.a = o.a;

-- and nothing was written
-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | 2
-- end-expected
SELECT a, note FROM vqc_o ORDER BY a;

-- cleanup
DROP VIEW vqc_v;
DROP TABLE vqc_g;
DROP TABLE vqc_o;

-- ============================================================================
-- Where the column is worked out decides only WHICH rows it is worked out for
--
-- The generation expression below cannot raise, so every reading of it has to
-- carry the same value however the query around it is written.
-- ============================================================================

-- setup
CREATE TABLE vqc_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL);
INSERT INTO vqc_h VALUES (1,'one'),(2,'two'),(3,'three');

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
WITH c AS (SELECT * FROM vqc_h) SELECT a, g FROM c WHERE a = 1;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM vqc_h) SELECT a, g FROM c WHERE a = 1;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
WITH c AS NOT MATERIALIZED (SELECT * FROM vqc_h) SELECT a, g FROM c WHERE a = 1;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
WITH c AS (SELECT *, random() AS r FROM vqc_h) SELECT a, g FROM c WHERE a = 1;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 2 | 20
-- end-expected
WITH c AS (SELECT * FROM vqc_h) SELECT a, g FROM c WHERE a = 1
UNION ALL SELECT a, g FROM c WHERE a = 2;

-- cleanup
DROP TABLE vqc_h;
