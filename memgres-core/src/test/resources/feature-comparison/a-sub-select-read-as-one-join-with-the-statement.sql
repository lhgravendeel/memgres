-- ============================================================================
-- A sub-select compared with IN or = ANY, and the restriction the rest of the
-- qualification puts on it
--
-- PostgreSQL pulls a sub-select written x IN (SELECT c FROM t) or
-- x = ANY (SELECT c FROM t) among the parts that must all hold up into the
-- statement holding it and reads the two as one join, which puts c and x in
-- one class. Beside x = 5 that says c = 5, and PostgreSQL puts that
-- restriction on t's own scan -- so a row of t the join could never have kept
-- is decided before anything costly is read of it. That is as true of the scan
-- a statement writes through as of one it only reads from, so an UPDATE and a
-- DELETE read their WHERE the same way a SELECT does.
--
-- A sub-select the statement does not read as a join is worked out in full:
-- one under NOT or under OR, one compared any other way, a scalar one, and one
-- whose own clauses settle which rows it answers with before the comparison is
-- made. Nothing is derived either where the rest of the qualification pins the
-- column to no constant at all.
--
-- The generation expression below is 10/a over a relation holding a row with
-- a = 0, so every statement here is really asking which rows it was worked out
-- for. Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE sjq_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO sjq_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE sjq_o (a int, note text);
INSERT INTO sjq_o VALUES (5,'x'),(0,'y');
CREATE TABLE sjq_p (a int);
INSERT INTO sjq_p VALUES (5);

-- ============================================================================
-- A sub-select compared with IN or = ANY takes the constant the rest of the
-- qualification pins the column to
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 5;

-- written the other way round it says the same thing
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o WHERE o.a = 5 AND o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2);

-- the constant may stand on either side of the comparison that pins the column
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND 5 = o.a;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 5 AND o.note = 'x';

-- = ANY and = SOME spell the same comparison as IN
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o WHERE o.a = ANY (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 5;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o WHERE o.a = SOME (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 5;

-- a name compared with another that is itself compared with a constant is
-- compared with that constant, so the restriction survives a join
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o, sjq_p p WHERE o.a = p.a AND o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND p.a = 5;

-- ordering the sub-select's rows does not choose between them
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2 ORDER BY s.a) AND o.a = 5;

-- nor does answering each of them once
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o WHERE o.a IN (SELECT DISTINCT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 5;

-- a grouped sub-select is named by what it grouped on
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2 GROUP BY s.a) AND o.a = 5;

-- the restriction is on what the sub-select answers with, whatever that is
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o WHERE o.a IN (SELECT s.a + 0 FROM sjq_g s WHERE s.g = 2) AND o.a = 5;

-- a sub-select that compares itself with the row above needs nothing derived
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sjq_o o WHERE o.a IN (VALUES (5),(6)) AND o.a = 5;

-- a part the plain comparisons never reach runs for no row at all
-- begin-expected
-- columns: a
-- end-expected
SELECT o.a FROM sjq_o o WHERE NOT EXISTS (SELECT 1 FROM sjq_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 99;

-- ============================================================================
-- Nothing is derived where the qualification pins the column to no constant
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sjq_o o WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sjq_o o WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a IN (5,6);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sjq_o o WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a > 4;

-- an aggregate answers for every row the sub-select read, so no restriction
-- reaches the scan under it
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sjq_o o WHERE o.a IN (SELECT max(s.a) FROM sjq_g s WHERE s.g = 2) AND o.a = 5;

-- ============================================================================
-- A sub-select the statement does not read as a join is worked out in full
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sjq_o o WHERE o.a NOT IN (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sjq_o o WHERE NOT (o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2)) AND o.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sjq_o o WHERE o.a < ANY (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 5;

-- a scalar sub-select is one value the statement works out whichever order the
-- two parts were written in
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sjq_o o WHERE (SELECT max(s.g) FROM sjq_g s) = 2 AND o.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sjq_o o WHERE o.a = 5 AND (SELECT max(s.g) FROM sjq_g s) = 2;

-- one side of an OR does not decide the row, so neither is read first
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sjq_o o WHERE o.a = 99 OR EXISTS (SELECT 1 FROM sjq_g s WHERE s.a = o.a AND s.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sjq_o o WHERE CASE WHEN o.a = 5 THEN (SELECT count(*) FROM sjq_g s WHERE s.g = 2) ELSE 0 END > 0 AND o.a = 5;

-- ============================================================================
-- A statement that writes reads its qualification the same way
-- ============================================================================

UPDATE sjq_o o SET note = 'z' WHERE EXISTS (SELECT 1 FROM sjq_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | z
-- end-expected
SELECT a, note FROM sjq_o ORDER BY a;

DELETE FROM sjq_o o WHERE EXISTS (SELECT 1 FROM sjq_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 99;

DELETE FROM sjq_o o WHERE NOT EXISTS (SELECT 1 FROM sjq_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 99;

UPDATE sjq_o o SET note = 'q' WHERE o.a = 99 AND EXISTS (SELECT 1 FROM sjq_g s WHERE s.a = o.a AND s.g = 2);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM sjq_o;

-- a write takes the restriction its own qualification derives for a sub-select
-- begin-expected
-- columns: a | note
-- row: 5 | p
-- end-expected
UPDATE sjq_o o SET note = 'p' WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 5 RETURNING o.a, o.note;

UPDATE sjq_o o SET note = 'n' WHERE 5 = o.a AND o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2);

UPDATE sjq_o o SET note = 'v' WHERE o.a = ANY (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 5;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | v
-- end-expected
SELECT a, note FROM sjq_o ORDER BY a;

DELETE FROM sjq_o o WHERE o.a = ANY (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 99;

-- the derived restriction is whatever the qualification says, so a statement
-- that asks for the row the expression raises for still raises
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
DELETE FROM sjq_o o WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 0;

-- and so does a write that pins the column to no constant at all
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
UPDATE sjq_o o SET note = 'r' WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
DELETE FROM sjq_o o WHERE EXISTS (SELECT 1 FROM sjq_g s WHERE s.a = o.a AND s.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
UPDATE sjq_o o SET note = 'e' WHERE o.a NOT IN (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
DELETE FROM sjq_o o WHERE (SELECT max(s.g) FROM sjq_g s) = 2 AND o.a = 5;

-- none of them wrote anything
-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | v
-- end-expected
SELECT a, note FROM sjq_o ORDER BY a;

-- begin-expected
-- columns: a | note
-- row: 5 | v
-- end-expected
DELETE FROM sjq_o o WHERE o.a IN (SELECT s.a FROM sjq_g s WHERE s.g = 2) AND o.a = 5 RETURNING o.a, o.note;

DELETE FROM sjq_o o WHERE o.a = 5 AND EXISTS (SELECT 1 FROM sjq_g s WHERE s.a = o.a AND s.g = 2);

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- end-expected
SELECT a, note FROM sjq_o ORDER BY a;

-- ============================================================================
-- Reading a sub-select as a join never changes a value
-- ============================================================================

-- setup
CREATE TABLE sjq_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL);
INSERT INTO sjq_h VALUES (1,'one'),(2,'two'),(3,'three');
CREATE TABLE sjq_q (a int, note text);
INSERT INTO sjq_q VALUES (1,'x'),(2,'y'),(3,'z');

-- begin-expected
-- columns: a | note
-- row: 2 | y
-- end-expected
SELECT o.a, o.note FROM sjq_q o WHERE o.a IN (SELECT s.a FROM sjq_h s WHERE s.g = 20) AND o.a = 2;

-- begin-expected
-- columns: a
-- row: 2
-- row: 3
-- end-expected
SELECT o.a FROM sjq_q o WHERE o.a IN (SELECT s.a FROM sjq_h s WHERE s.g > 10) ORDER BY o.a;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT o.a FROM sjq_q o WHERE o.a = ANY (SELECT s.a FROM sjq_h s) ORDER BY o.a;

-- a constant the sub-select can answer nothing for keeps no row
-- begin-expected
-- columns: a
-- end-expected
SELECT o.a FROM sjq_q o WHERE o.a IN (SELECT s.a FROM sjq_h s WHERE s.g = 20) AND o.a = 3;

-- cleanup
DROP TABLE sjq_q;
DROP TABLE sjq_h;
DROP TABLE sjq_p;
DROP TABLE sjq_o;
DROP TABLE sjq_g;
