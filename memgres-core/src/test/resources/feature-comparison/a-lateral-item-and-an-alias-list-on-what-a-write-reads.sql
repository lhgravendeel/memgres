-- ============================================================================
-- A LATERAL item narrowed from above, and the alias list a writing statement's
-- relation may wear
--
-- PostgreSQL pulls a LATERAL item up into the query reading it, so a comparison
-- written inside the item stands beside that query's own: (SELECT * FROM g z
-- WHERE z.a = o.a) read as s says s.a = o.a, and beside o.a = 5 that says
-- s.a = 5 -- a restriction on the item's own scan, which decides the item's
-- rows before a VIRTUAL generated column of one of them is reached. Where no
-- constant reaches the item -- a comparison that is not an equality, or one
-- over a column no equality ties to the item -- PostgreSQL reaches the
-- expression and raises.
--
-- The same rule reaches the relation a writing statement brings in beside its
-- target, whether that relation wears a column alias list or not, and the
-- source a MERGE names in an assignment. The relation a DELETE or an UPDATE
-- writes, on the other hand, takes an alias and nothing after it: a column
-- alias list renames what a query reads, and those statements read the
-- relation's own columns, so the grammar stops at the parenthesis. And a
-- parenthesised join stays a join rather than becoming a relation, so a list
-- that over-names one is refused by the name PostgreSQL gives a join.
--
-- The generation expression here is 10/a over a relation holding a row with
-- a = 0, so a statement that answers at all is one that reached the expression
-- for the rows it read the column of and for no others. Every value was
-- measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE law_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO law_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE law_o (a int, note text);
INSERT INTO law_o VALUES (5,'x'),(0,'y');
CREATE TABLE law_p (a int, m int);
INSERT INTO law_p VALUES (5,50),(0,0),(9,90);

-- ============================================================================
-- The query above restricts the item, and the join condition may name the
-- generated column
-- ============================================================================

-- the item is not correlated at all; the restriction is written on it directly
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM law_o o, LATERAL (SELECT * FROM law_g) s WHERE s.a = 5 AND o.a = 5;

-- the item's own WHERE correlates to the row beside it
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT l.g FROM law_o o, LATERAL (SELECT * FROM law_g WHERE a = o.a) l WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT l.g FROM law_o o LEFT JOIN LATERAL (SELECT * FROM law_g WHERE a = o.a) l ON true WHERE o.a = 5;

-- the join condition names the generated column, so the scan has to have it
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM law_o o LEFT JOIN LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM law_o o JOIN LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM law_o o CROSS JOIN LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM law_o o, LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s WHERE s.g = 2 AND o.a = 5;

-- the select list need not name the column the condition reads
-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT s.k FROM law_o o LEFT JOIN LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM law_o o LEFT JOIN LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- an item answering with its relation's columns written out is read the same way
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM law_o o, LATERAL (SELECT z.a, z.k, z.g FROM law_g z WHERE z.a = o.a) s WHERE o.a = 5 AND s.g = 2;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM law_o o, LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s WHERE o.a = 5 AND s.a = 5;

-- the item narrows itself further, and the query above narrows the row beside it
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM law_o o, LATERAL (SELECT * FROM law_g z WHERE z.a = o.a AND z.k = 'five') s WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM law_o o, LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s WHERE o.a = 5 AND o.note = 'x';

-- a restriction over another column of the outer relation narrows it just as well
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM law_p p, LATERAL (SELECT * FROM law_g z WHERE z.a = p.a) s WHERE p.m = 50;

-- an outer row the item answers nothing for is padded, and the padding is null
-- begin-expected
-- columns: a | g
-- row: 9 | NULL
-- end-expected
SELECT p.a, s.g FROM law_p p LEFT JOIN LATERAL (SELECT * FROM law_g z WHERE z.a = p.a) s ON s.g = 2 WHERE p.a = 9;

-- begin-expected
-- columns: a | g
-- row: 5 | 2
-- end-expected
SELECT p.a, s.g FROM law_p p LEFT JOIN LATERAL (SELECT * FROM law_g z WHERE z.a = p.a) s ON s.g = 2 WHERE p.a = 5;

-- ============================================================================
-- Where no constant reaches the item, its expression is reached
-- ============================================================================

-- no equality ties note to the item, so nothing about a is carried into it
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM law_o o LEFT JOIN LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- nor does a comparison that is not an equality
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM law_o o LEFT JOIN LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a > 4;

-- nor does a query that restricts nothing at all
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM law_o o LEFT JOIN LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s ON true;

-- and where the constant keeps the row the expression cannot be worked out for,
-- it raises
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM law_o o LEFT JOIN LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 0;

-- a query that never names the column reads every row of the item
-- begin-expected
-- columns: k
-- row: five
-- row: zero
-- end-expected
SELECT s.k FROM law_o o, LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s ORDER BY s.k;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM law_o o, LATERAL (SELECT * FROM law_g z WHERE z.a = o.a) s;

-- ============================================================================
-- A write reads a derived relation through a column alias list
-- ============================================================================

CREATE TABLE law_w (a int, note text);
INSERT INTO law_w VALUES (5,'x'),(0,'y');

UPDATE law_w o SET note = s.z::text FROM (SELECT * FROM law_g) s(x,y,z) WHERE s.x = o.a AND o.a = 5;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | 2
-- end-expected
SELECT a, note FROM law_w ORDER BY a;

-- a stored column under the list is read whatever narrows the relation
UPDATE law_w o SET note = s.y FROM (SELECT * FROM law_g) s(x,y,z) WHERE s.x = o.a AND o.a = 5;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | five
-- end-expected
SELECT a, note FROM law_w ORDER BY a;

-- the relation narrows itself, or is narrowed through another of its columns
UPDATE law_w o SET note = 'i' || s.z::text FROM (SELECT * FROM law_g WHERE a = 5) s(x,y,z) WHERE s.x = o.a;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | i2
-- end-expected
SELECT a, note FROM law_w ORDER BY a;

UPDATE law_w o SET note = 'k' || s.z::text FROM (SELECT * FROM law_g) s(x,y,z) WHERE s.x = o.a AND s.y = 'five';

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | k2
-- end-expected
SELECT a, note FROM law_w ORDER BY a;

-- with nothing narrowing it, the write reaches every row of the relation and
-- writes nothing
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
UPDATE law_w o SET note = s.z::text FROM (SELECT * FROM law_g) s(x,y,z) WHERE s.x = o.a;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | k2
-- end-expected
SELECT a, note FROM law_w ORDER BY a;

-- a column the list renamed that nothing has to work out is read for every
-- paired row
UPDATE law_w o SET note = s.y FROM (SELECT * FROM law_g) s(x,y,z) WHERE s.x = o.a;

-- begin-expected
-- columns: a | note
-- row: 0 | zero
-- row: 5 | five
-- end-expected
SELECT a, note FROM law_w ORDER BY a;

-- DELETE ... USING and INSERT ... SELECT read the renamed column the same way
-- begin-expected
-- columns: a | z
-- row: 5 | 2
-- end-expected
DELETE FROM law_w o USING (SELECT * FROM law_g) s(x,y,z) WHERE s.x = o.a AND o.a = 5 RETURNING o.a, s.z;

-- begin-expected
-- columns: a | note
-- row: 0 | zero
-- end-expected
SELECT a, note FROM law_w ORDER BY a;

INSERT INTO law_w (a, note) SELECT s.x, s.z::text FROM (SELECT * FROM law_g) s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: a | note
-- row: 0 | zero
-- row: 5 | 2
-- end-expected
SELECT a, note FROM law_w ORDER BY a;

DELETE FROM law_w o USING (SELECT * FROM law_g) s(x,y,z) WHERE s.x = o.a AND o.a = 5 AND s.z = 2;

-- begin-expected
-- columns: a | note
-- row: 0 | zero
-- end-expected
SELECT a, note FROM law_w ORDER BY a;

-- ============================================================================
-- What a MERGE works out of the generated column it assigns
-- ============================================================================

CREATE TABLE law_m (a int, note text);
INSERT INTO law_m VALUES (5,'x'),(0,'y');

MERGE INTO law_m o USING law_g t ON o.a = t.a AND o.a = 5 WHEN MATCHED THEN UPDATE SET note = t.g::text;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | 2
-- end-expected
SELECT a, note FROM law_m ORDER BY a;

-- the constant written on the source's own column narrows it the same way
MERGE INTO law_m o USING law_g t ON o.a = t.a AND t.a = 5 WHEN MATCHED THEN UPDATE SET note = 't' || t.g::text;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | t2
-- end-expected
SELECT a, note FROM law_m ORDER BY a;

-- as does a source query that narrows itself, and a column alias list over one
MERGE INTO law_m o USING (SELECT * FROM law_g WHERE a = 5) t ON o.a = t.a WHEN MATCHED THEN UPDATE SET note = 'q' || t.g::text;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | q2
-- end-expected
SELECT a, note FROM law_m ORDER BY a;

MERGE INTO law_m o USING (SELECT * FROM law_g) t(x,y,z) ON o.a = t.x AND o.a = 5 WHEN MATCHED THEN UPDATE SET note = 'l' || t.z::text;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | l2
-- end-expected
SELECT a, note FROM law_m ORDER BY a;

-- an arm's own condition names it
MERGE INTO law_m o USING law_g t ON o.a = t.a AND o.a = 5 WHEN MATCHED AND t.g = 2 THEN UPDATE SET note = 'hit';

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | hit
-- end-expected
SELECT a, note FROM law_m ORDER BY a;

-- with nothing narrowing the join, every paired row reaches it and nothing is
-- written
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
MERGE INTO law_m o USING law_g t ON o.a = t.a WHEN MATCHED THEN UPDATE SET note = t.g::text;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | hit
-- end-expected
SELECT a, note FROM law_m ORDER BY a;

-- a column the statement does not have to work out is read for every paired row
MERGE INTO law_m o USING law_g t ON o.a = t.a WHEN MATCHED THEN UPDATE SET note = t.k;

-- begin-expected
-- columns: a | note
-- row: 0 | zero
-- row: 5 | five
-- end-expected
SELECT a, note FROM law_m ORDER BY a;

-- and the arm that deletes reaches it no more than the arm that assigns
MERGE INTO law_m o USING law_g t ON o.a = t.a AND o.a = 5 WHEN MATCHED THEN DELETE;

-- begin-expected
-- columns: a | note
-- row: 0 | zero
-- end-expected
SELECT a, note FROM law_m ORDER BY a;

-- ============================================================================
-- The relation a DELETE or an UPDATE writes takes an alias and nothing after it
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
DELETE FROM law_o AS d(p,q) WHERE p = 99;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
DELETE FROM law_o d(p,q) WHERE p = 99;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
DELETE FROM law_o AS d (p) WHERE d.a = 99;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
DELETE FROM law_o (p,q);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
UPDATE law_o AS u(p,q) SET note = 'z' WHERE p = 99;

-- nothing was written by any of them
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM law_o;

-- an alias on its own is still read
DELETE FROM law_o AS d WHERE d.a = 99;
UPDATE law_o AS u SET note = 'z' WHERE u.a = 99;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | x
-- end-expected
SELECT a, note FROM law_o ORDER BY a;

-- ============================================================================
-- An alias list over a parenthesised join names a join expression
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: join expression "j" has 3 columns available but 4 columns specified
-- end-expected-error
SELECT * FROM (law_o JOIN law_p USING (a)) AS j(c1,c2,c3,c4);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: join expression "j" has 4 columns available but 5 columns specified
-- end-expected-error
SELECT * FROM (law_o CROSS JOIN law_p) AS j(c1,c2,c3,c4,c5);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: join expression "j" has 4 columns available but 5 columns specified
-- end-expected-error
SELECT * FROM (law_o LEFT JOIN law_p ON law_o.a = law_p.a) AS j(c1,c2,c3,c4,c5);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: join expression "j" has 3 columns available but 4 columns specified
-- end-expected-error
SELECT * FROM (law_o NATURAL JOIN law_p) AS j(c1,c2,c3,c4);

-- everything else that may wear such a list PostgreSQL calls a table
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "j" has 2 columns available but 3 columns specified
-- end-expected-error
SELECT * FROM law_o AS j(c1,c2,c3);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "j" has 2 columns available but 3 columns specified
-- end-expected-error
SELECT * FROM (SELECT * FROM law_o) AS j(c1,c2,c3);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "j" has 2 columns available but 3 columns specified
-- end-expected-error
SELECT * FROM (VALUES (1,2)) AS j(c1,c2,c3);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "j" has 1 columns available but 2 columns specified
-- end-expected-error
SELECT * FROM generate_series(1,2) AS j(c1,c2);

-- a list the join expression has room for is read
-- begin-expected
-- columns: c1 | c2 | c3
-- row: 0 | y | 0
-- row: 5 | x | 50
-- end-expected
SELECT j.c1, j.c2, j.c3 FROM (law_o JOIN law_p USING (a)) AS j(c1,c2,c3) ORDER BY c1;

-- ============================================================================
-- None of it changes the value the column carries
--
-- The generation expression below cannot raise, so every row that comes back
-- has to carry the right one.
-- ============================================================================

CREATE TABLE law_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL);
INSERT INTO law_h VALUES (1,'one'),(2,'two'),(3,'three');
CREATE TABLE law_q (a int, note text);
INSERT INTO law_q VALUES (1,'p1'),(9,'p9');

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 9 | NULL
-- end-expected
SELECT o.a, s.g FROM law_q o LEFT JOIN LATERAL (SELECT * FROM law_h z WHERE z.a = o.a) s ON true ORDER BY o.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 9 | NULL
-- end-expected
SELECT o.a, s.g FROM law_q o LEFT JOIN LATERAL (SELECT * FROM law_h z WHERE z.a = o.a) s ON s.g = 10 ORDER BY o.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
SELECT o.a, s.g FROM law_q o, LATERAL (SELECT * FROM law_h z WHERE z.a = o.a) s ORDER BY o.a;

-- begin-expected
-- columns: z
-- row: 20
-- end-expected
SELECT s.z FROM (SELECT * FROM law_h) s(x,y,z) WHERE s.x = 2;

-- begin-expected
-- columns: x | z
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT s.x, s.z FROM (SELECT * FROM law_h) s(x,y,z) ORDER BY s.x;

-- cleanup
DROP TABLE law_g;
DROP TABLE law_o;
DROP TABLE law_p;
DROP TABLE law_w;
DROP TABLE law_m;
DROP TABLE law_h;
DROP TABLE law_q;
