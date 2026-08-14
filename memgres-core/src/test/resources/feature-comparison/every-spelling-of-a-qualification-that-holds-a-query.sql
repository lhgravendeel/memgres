-- ============================================================================
-- Every spelling of a qualification that holds a query
--
-- PostgreSQL orders the parts of a qualification by what each costs to work out
-- and stops at the first that is false, so a part holding a query -- EXISTS, IN,
-- = ANY, <> ALL, a scalar sub-select -- is read after every plain comparison
-- standing beside it, on a statement that writes as much as on one that reads.
--
-- It also stops reading the sub-select itself at the row that settles what it
-- was asked: EXISTS and NOT EXISTS at the first row answered at all, ALL at the
-- first row the comparison fails of. An ANY comparison -- which is what IN is,
-- and what NOT IN is under a NOT -- is not read that way: every row of the
-- sub-select is read into a table of values and the comparison is answered out
-- of that, so IN reaches a row EXISTS stopped in front of.
--
-- What the sub-select says about the row it is compared with is the condition
-- of the join the two are read as rather than a filter on the sub-select's own
-- scan, so a row that comparison would have rejected is reached just the same.
-- What does narrow that scan is the constant an equivalence class carries onto
-- it: o.a = 5 standing beside s.a = o.a says s.a = 5 as surely as it says
-- either of them, and it says it through a set operation and down through a
-- derived table too.
--
-- The generation expression below is 10/a over a relation holding a row where a
-- is 0, so every statement here is really asking which rows it was worked out
-- for. Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE sqq_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO sqq_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE sqq_o (a int, note text);
INSERT INTO sqq_o VALUES (5,'x'),(0,'y');
CREATE TABLE sqq_p (a int);
INSERT INTO sqq_p VALUES (5),(0);

-- ============================================================================
-- A sub-select is read no further than the row that settles what it was asked
-- ============================================================================

-- ALL stops at the first row the comparison fails of, so the row behind it is
-- never reached
-- begin-expected
-- columns: a
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a <> ALL (SELECT s.a FROM sqq_g s WHERE s.g = 2) AND o.a = 5;

-- begin-expected
-- columns: a
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a <> ALL (SELECT s.a FROM sqq_g s WHERE s.g = 2) AND o.a > 4;

-- begin-expected
-- columns: a
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a <> ALL (SELECT s.a FROM sqq_g s WHERE s.g = 2) AND o.a IN (5,6);

-- with no row of the statement rejected first the comparison holds of the row
-- in front, and the row behind it is read after all
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sqq_o o WHERE o.a <> ALL (SELECT s.a FROM sqq_g s WHERE s.g = 2);

-- an uncorrelated EXISTS is answered by the first row the sub-select holds
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a = 5 AND EXISTS (SELECT 1 FROM sqq_g s WHERE s.g = 2);

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE EXISTS (SELECT 1 FROM sqq_g s WHERE s.g = 2) AND o.a = 5;

-- what the sub-select's own select list names changes nothing
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a = 5 AND EXISTS (SELECT s.a FROM sqq_g s WHERE s.g = 2);

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a = 5 AND EXISTS (SELECT 1 FROM sqq_g s WHERE s.a > 0 AND s.g = 2);

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a = 5 AND EXISTS (SELECT 1 FROM sqq_g s WHERE s.g = 2 AND true);

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a = 5 AND EXISTS (SELECT 1 FROM sqq_g s WHERE s.g = 2 LIMIT 1);

-- and the sub-select is read no further whatever else the statement holds
-- begin-expected
-- columns: a
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a = 9 AND EXISTS (SELECT 1 FROM sqq_g s WHERE s.g = 2);

-- begin-expected
-- columns: a
-- row: 5
-- row: 0
-- end-expected
SELECT o.a FROM sqq_o o WHERE EXISTS (SELECT 1 FROM sqq_g s WHERE s.g = 2);

-- an ANY comparison is answered out of every row the sub-select holds, so it
-- reaches the row EXISTS stopped in front of
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sqq_o o WHERE o.a = 5 AND 5 IN (SELECT s.a FROM sqq_g s WHERE s.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sqq_o o WHERE o.a = 5 AND 5 = ANY (SELECT s.a FROM sqq_g s WHERE s.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sqq_o o WHERE o.a = 5 AND 1 IN (SELECT 1 FROM sqq_g s WHERE s.g = 2);

-- and so is NOT IN, which is that comparison standing under a NOT
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sqq_o o WHERE o.a NOT IN (SELECT s.a FROM sqq_g s WHERE s.g = 2) AND o.a = 5;

-- ============================================================================
-- The restriction an equivalence class carries into a sub-select
-- ============================================================================

-- through a set operation, which answers with the rows of both its queries, so
-- the restriction stands on each of their scans
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a IN (SELECT s.a FROM sqq_g s WHERE s.g = 2 UNION SELECT 7) AND o.a = 5;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a IN (SELECT s.a FROM sqq_g s WHERE s.g = 2 UNION ALL SELECT 7) AND o.a = 5;

-- with nothing pinning the column of the statement above, nothing is carried in
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sqq_o o WHERE o.a IN (SELECT s.a FROM sqq_g s WHERE s.g = 2 UNION SELECT 7);

-- and down through the derived table the sub-select itself reads
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a IN (SELECT t.a FROM (SELECT * FROM sqq_g) t WHERE t.g = 2) AND o.a = 5;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a = 5 AND o.a IN (SELECT t.a FROM (SELECT * FROM sqq_g) t WHERE t.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM sqq_o o WHERE o.a IN (SELECT t.a FROM (SELECT * FROM sqq_g) t WHERE t.g = 2);

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a = 5 AND EXISTS (SELECT 1 FROM sqq_g s WHERE s.a = o.a AND s.g = 2);

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE o.a = 5 AND EXISTS (SELECT 1 FROM (SELECT * FROM sqq_g) s WHERE s.a = o.a AND s.g = 2);

-- the relation on both sides of the comparison is no different
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT a FROM sqq_g t1 WHERE a = 5 AND EXISTS (SELECT 1 FROM sqq_g t2 WHERE t2.a = t1.a AND t2.g = 2);

-- a scalar sub-select is run once for each row of the statement above with that
-- row's values standing in it, so what it compares with them is a filter on its
-- own scan
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM sqq_o o WHERE (SELECT max(s.g) FROM sqq_g s WHERE s.a = o.a) = 2 AND o.a = 5;

-- ============================================================================
-- A comparison with the row the statement above holds is a join condition
--
-- The relation here holds a row (2,'two') so that no row of the statement above
-- selects the row where a is 0: what reaches the generation expression for that
-- row is the pairing, not the selection.
-- ============================================================================

-- setup
CREATE TABLE sqq_h (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO sqq_h (a,k) VALUES (5,'five'),(0,'zero'),(2,'two');
CREATE TABLE sqq_q (a int, note text);
INSERT INTO sqq_q VALUES (5,'x'),(2,'y');

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.note FROM sqq_q o WHERE EXISTS (SELECT 1 FROM (SELECT * FROM sqq_h) s WHERE s.a = o.a AND s.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.note FROM sqq_q o WHERE EXISTS (SELECT 1 FROM sqq_h s WHERE s.a = o.a AND s.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.note FROM sqq_q o WHERE NOT EXISTS (SELECT 1 FROM sqq_h s WHERE s.a = o.a AND s.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.note FROM sqq_q o WHERE o.a IN (SELECT s.a FROM sqq_h s WHERE s.a = o.a AND s.g = 2);

-- the order the two are written in says nothing
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.note FROM sqq_q o WHERE EXISTS (SELECT 1 FROM sqq_h s WHERE s.g = 2 AND s.a = o.a);

-- a plain comparison beside it is read first and settles the row
-- begin-expected
-- columns: note
-- row: x
-- end-expected
SELECT o.note FROM sqq_q o WHERE EXISTS (SELECT 1 FROM sqq_h s WHERE s.a = o.a AND s.g = 2) AND o.note = 'x';

-- begin-expected
-- columns: note
-- row: x
-- end-expected
SELECT o.note FROM sqq_q o WHERE o.a = 5 AND EXISTS (SELECT 1 FROM sqq_h s WHERE s.a = o.a AND s.g = 2);

-- begin-expected
-- columns: note
-- row: x
-- end-expected
SELECT o.note FROM sqq_q o WHERE o.a = 5 AND EXISTS (SELECT 1 FROM (SELECT * FROM sqq_h) s WHERE s.a = o.a AND s.g = 2);

-- begin-expected
-- columns: note
-- row: y
-- end-expected
SELECT o.note FROM sqq_q o WHERE o.a = 2 AND EXISTS (SELECT 1 FROM sqq_h s WHERE s.a = o.a AND s.g = 5);

-- an ALL correlated with the row above is narrowed by that correlation alone
-- begin-expected
-- columns: note
-- row: y
-- end-expected
SELECT o.note FROM sqq_q o WHERE o.a <> ALL (SELECT s.a FROM sqq_h s WHERE s.a = o.a AND s.g = 2);

-- begin-expected
-- columns: note
-- row: x
-- end-expected
SELECT o.note FROM sqq_q o WHERE (SELECT max(s.g) FROM sqq_h s WHERE s.a = o.a) = 2;

DROP TABLE sqq_q;
DROP TABLE sqq_h;

-- ============================================================================
-- The write paths read their qualification the same way
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH w AS (UPDATE sqq_o o SET note='z' FROM sqq_p p WHERE p.a = o.a AND EXISTS (SELECT 1 FROM sqq_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5 RETURNING o.a) SELECT count(*) AS count FROM w;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- row: 5, z
-- end-expected
SELECT a, note FROM sqq_o ORDER BY a;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH w AS (DELETE FROM sqq_o o USING sqq_p p WHERE p.a = o.a AND EXISTS (SELECT 1 FROM sqq_g s WHERE s.a = o.a AND s.g = 2) AND o.a = 5 RETURNING o.a) SELECT count(*) AS count FROM w;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- end-expected
SELECT a, note FROM sqq_o ORDER BY a;

INSERT INTO sqq_o VALUES (5,'x');

-- the same through a derived table the sub-select reads
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH w AS (UPDATE sqq_o o SET note='q' FROM sqq_p p WHERE p.a = o.a AND o.a IN (SELECT t.a FROM (SELECT * FROM sqq_g) t WHERE t.g = 2) AND o.a = 5 RETURNING o.a) SELECT count(*) AS count FROM w;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- row: 5, q
-- end-expected
SELECT a, note FROM sqq_o ORDER BY a;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH w AS (DELETE FROM sqq_o o USING sqq_p p WHERE p.a = o.a AND o.a IN (SELECT t.a FROM (SELECT * FROM sqq_g) t WHERE t.g = 2) AND o.a = 5 RETURNING o.a) SELECT count(*) AS count FROM w;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- end-expected
SELECT a, note FROM sqq_o ORDER BY a;

INSERT INTO sqq_o VALUES (5,'x');

-- and through a set operation
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH w AS (UPDATE sqq_o o SET note='r' FROM sqq_p p WHERE p.a = o.a AND o.a IN (SELECT s.a FROM sqq_g s WHERE s.g = 2 UNION SELECT 7) AND o.a = 5 RETURNING o.a) SELECT count(*) AS count FROM w;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- row: 5, r
-- end-expected
SELECT a, note FROM sqq_o ORDER BY a;

-- with nothing pinning the target's column the sub-select is read whole
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
UPDATE sqq_o o SET note='s' FROM sqq_p p WHERE p.a = o.a AND EXISTS (SELECT 1 FROM sqq_g s WHERE s.a = o.a AND s.g = 2);

-- ============================================================================
-- What an assignment reads is worked out for every pair the join answered with
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
UPDATE sqq_o o SET note = g.g::text FROM sqq_g g;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
UPDATE sqq_o o SET note = g.g::text FROM sqq_g g WHERE g.a = o.a;

-- and nothing was written by either
-- begin-expected
-- columns: a | note
-- row: 0, y
-- row: 5, r
-- end-expected
SELECT a, note FROM sqq_o ORDER BY a;

-- narrowed to the one pair, it is worked out for that pair alone
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH w AS (UPDATE sqq_o o SET note = g.g::text FROM sqq_g g WHERE g.a = o.a AND o.a = 5 RETURNING o.a) SELECT count(*) AS count FROM w;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- row: 5, 2
-- end-expected
SELECT a, note FROM sqq_o ORDER BY a;

-- a statement that names no VIRTUAL column of the relation it brought in works
-- none of them out, however many pairs the join answered with
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH w AS (UPDATE sqq_o o SET note = 'u' FROM sqq_g g WHERE g.a = o.a RETURNING o.a) SELECT count(*) AS count FROM w;

-- begin-expected
-- columns: a | note
-- row: 0, u
-- row: 5, u
-- end-expected
SELECT a, note FROM sqq_o ORDER BY a;

-- a DELETE naming it in its qualification is narrowed the same way
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
WITH w AS (DELETE FROM sqq_o o USING sqq_g g WHERE g.a = o.a AND o.a = 5 AND g.g = 2 RETURNING o.a) SELECT count(*) AS count FROM w;

-- begin-expected
-- columns: a | note
-- row: 0, u
-- end-expected
SELECT a, note FROM sqq_o ORDER BY a;

-- cleanup
DROP TABLE sqq_o;
DROP TABLE sqq_p;
DROP TABLE sqq_g;
