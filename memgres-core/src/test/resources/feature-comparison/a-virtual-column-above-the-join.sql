-- ============================================================================
-- A VIRTUAL generated column is worked out above the join that kept its row
--
-- A derived table, a view body and an inlined WITH item are pulled up into the
-- query that reads them, so a reference to a generated column of a relation
-- underneath stands in the query above and its expression is evaluated over
-- the rows that query's joins and its WHERE kept. A column alias list renames
-- every column the relation exposes, whatever kind of relation it is.
--
-- The generation expression below is 10/a over a relation holding a row with
-- a = 0, so every statement here is really asking which rows it was worked out
-- for. Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE vjc_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO vjc_g (a,k) VALUES (0,'zero'),(5,'five');
CREATE TABLE vjc_o (a int, note text);
INSERT INTO vjc_o VALUES (0,'y'),(5,'y');
CREATE VIEW vjc_v AS SELECT * FROM vjc_g;
CREATE VIEW vjc_vo AS SELECT * FROM vjc_o;

-- ============================================================================
-- A derived relation qualified only through a join to another relation
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM vjc_o o LEFT JOIN (SELECT * FROM vjc_g) s ON o.a = s.a WHERE o.a = 5;

-- the restriction written into the ON condition narrows it just as well
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM vjc_o o JOIN (SELECT * FROM vjc_g) s ON o.a = s.a AND o.a = 5;

-- begin-expected
-- columns: a | g
-- row: 5 | 2
-- end-expected
SELECT o.a, s.g FROM vjc_o o LEFT JOIN (SELECT * FROM vjc_g) s ON o.a = s.a WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM vjc_g) s, vjc_o o WHERE o.a = s.a AND o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT v.g FROM vjc_o o LEFT JOIN vjc_v v ON o.a = v.a WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH c AS (SELECT * FROM vjc_g) SELECT c.g FROM vjc_o o LEFT JOIN c ON o.a = c.a WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM vjc_o o LEFT JOIN (WITH q AS (SELECT * FROM vjc_g) SELECT * FROM q) s
  ON o.a = s.a WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM vjc_o o RIGHT JOIN (SELECT * FROM vjc_g) s ON o.a = s.a WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM vjc_o o FULL JOIN (SELECT * FROM vjc_g) s ON o.a = s.a WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT t1.g FROM (SELECT * FROM vjc_g) t1, (SELECT * FROM vjc_g) t2
  WHERE t1.a = 5 AND t2.a = 5;

-- a join that keeps every row of the relation reaches every row of it
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM vjc_o o LEFT JOIN (SELECT * FROM vjc_g) s ON o.a = s.a;

-- nothing about the generated column is read here, so nothing raises
-- begin-expected
-- columns: k
-- row: five
-- row: zero
-- end-expected
SELECT s.k FROM vjc_o o LEFT JOIN (SELECT * FROM vjc_g) s ON o.a = s.a ORDER BY s.k;

-- ============================================================================
-- A LATERAL relation
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM vjc_o o, LATERAL (SELECT * FROM vjc_g WHERE a = o.a) s WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM vjc_o o CROSS JOIN LATERAL (SELECT * FROM vjc_g z WHERE z.a = o.a) s
  WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM vjc_o o LEFT JOIN LATERAL (SELECT * FROM vjc_g z WHERE z.a = o.a) s
  ON true WHERE o.a = 5;

-- an uncorrelated one narrowed from above answers the same way
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM vjc_o o, LATERAL (SELECT * FROM vjc_g) s WHERE o.a = 5 AND s.a = 5;

-- begin-expected
-- columns: k
-- row: zero
-- row: five
-- end-expected
SELECT s.k FROM vjc_o o, LATERAL (SELECT * FROM vjc_g) s WHERE o.a = 5 ORDER BY s.a;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM vjc_o o, LATERAL (SELECT * FROM vjc_g) s;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM vjc_o o, LATERAL (SELECT * FROM vjc_g) s;

-- a qualification naming the column is a qualification of the scan itself
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM vjc_o o, LATERAL (SELECT * FROM vjc_g WHERE a = o.a) s WHERE s.g = 2;

-- ============================================================================
-- A column alias list on a derived table and on a view
-- ============================================================================

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM (SELECT * FROM vjc_g) s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: x | z
-- row: 5 | 2
-- end-expected
SELECT s.x, s.z FROM (SELECT * FROM vjc_g) s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: y
-- row: five
-- end-expected
SELECT s.y FROM (SELECT * FROM vjc_g) s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM (SELECT * FROM vjc_g) s(x,y,z) WHERE s.x = 5 AND s.y = 'five';

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM (SELECT a, k, g FROM vjc_g) s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT s.r FROM (SELECT * FROM vjc_g) AS s (p, q, r) WHERE s.p = 5;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM vjc_o o LEFT JOIN (SELECT * FROM vjc_g) s(x,y,z) ON o.a = s.x WHERE o.a = 5;

-- a list that reaches only the first column leaves the rest their own names
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM vjc_g) s(x) WHERE s.x = 5;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT * FROM vjc_g) s(x,y,z);

-- a view is renamed the same way
-- begin-expected
-- columns: y
-- row: y
-- row: y
-- end-expected
SELECT s.y FROM vjc_vo s(x,y) ORDER BY s.x;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM vjc_v s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: x | z
-- row: 5 | 2
-- end-expected
SELECT s.x, s.z FROM vjc_v s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM vjc_v s(x,y,z);

-- the relation answers to the names the list gave it and to no others
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.z does not exist
-- end-expected-error
SELECT s.z FROM (SELECT * FROM vjc_g) s(x) WHERE s.x = 5;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.g does not exist
-- end-expected-error
SELECT s.g FROM (SELECT * FROM vjc_g) s(x,y,z) WHERE s.x = 5;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.g does not exist
-- end-expected-error
SELECT s.g FROM (SELECT * FROM vjc_g) AS s (p, q, r) WHERE s.p = 5;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.g does not exist
-- end-expected-error
SELECT s.g FROM vjc_v s(x,y,z) WHERE s.x = 5;

-- a list longer than the relation is a list the relation cannot answer to
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: has 2 columns available but 3 columns specified
-- end-expected-error
SELECT * FROM vjc_vo s(x,y,z);

-- a qualification naming the renamed column is still a scan qualification
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.z FROM (SELECT * FROM vjc_g) s(x,y,z) WHERE s.z = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.z FROM vjc_v s(x,y,z);

-- ============================================================================
-- A restriction the query's own equalities derive between them
--
-- s.a, o.a and 5 stand in one class, so s.a = o.a AND o.a = 5 says s.a = 5 as
-- surely as it says either of the two, and that decides s's rows before the
-- generation expression is reached.
-- ============================================================================

-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT 1 FROM vjc_g s, vjc_o o WHERE s.a = o.a AND o.a = 5 AND s.g = 2;

-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT 1 FROM (SELECT * FROM vjc_g) s, vjc_o o WHERE s.a = o.a AND o.a = 5 AND s.g = 2;

-- nothing is derived from an equality with no constant anywhere in its class
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 1 FROM vjc_g s, vjc_o o WHERE s.a = o.a AND s.g = 2;

-- the outer row stands still while a correlated subquery runs, so a part of
-- the subquery's qualification comparing this relation with it decides too
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT a FROM vjc_g t1 WHERE a = 5
  AND EXISTS (SELECT 1 FROM vjc_g t2 WHERE t2.a = t1.a AND t2.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT a FROM vjc_g t1 WHERE EXISTS (SELECT 1 FROM vjc_g t2 WHERE t2.a = t1.a AND t2.g = 2);

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT o.a FROM vjc_o o WHERE o.a = 5
  AND EXISTS (SELECT 1 FROM (SELECT * FROM vjc_g) s WHERE s.a = o.a AND s.g = 2);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a FROM vjc_o o WHERE EXISTS (SELECT 1 FROM (SELECT * FROM vjc_g) s WHERE s.g = 2);

-- ============================================================================
-- What a relation settles for itself is still worked out below the query
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT * FROM vjc_g) s;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT * FROM vjc_g) s ORDER BY s.g;

-- a LIMIT or an OFFSET settles which rows there are before the query above is
-- read, so the qualification stays where it was written
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT * FROM vjc_g LIMIT 5) s WHERE s.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM (SELECT * FROM vjc_g OFFSET 0) s WHERE s.a = 5;

-- a sort inside the relation reads the column there
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.a FROM (SELECT * FROM vjc_g ORDER BY g) s;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT g FROM vjc_v;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT g FROM vjc_v WHERE a = 5;

-- begin-expected
-- columns: a | k | g
-- row: 5 | five | 2
-- end-expected
SELECT * FROM (SELECT * FROM vjc_g) s WHERE s.a = 5;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT * FROM vjc_g) s;

-- begin-expected
-- columns: max
-- row: 2
-- end-expected
SELECT max(s.g) FROM (SELECT * FROM vjc_g) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT DISTINCT a, k, g FROM vjc_g) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT DISTINCT * FROM vjc_g) s WHERE s.a = 5;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT s.a FROM (SELECT * FROM vjc_g GROUP BY a, k, g) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM vjc_g UNION ALL SELECT * FROM vjc_g) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM vjc_g UNION SELECT * FROM vjc_g) s WHERE s.a = 5;

-- an arm answers for some of the relation's rows and not for the rest, so each
-- arm works its own column out rather than leaving it to the relation
-- begin-expected
-- columns: g
-- row: 7
-- end-expected
SELECT s.g FROM (SELECT * FROM vjc_g UNION ALL SELECT 1, 'one', 7) s WHERE s.a = 1;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM vjc_g UNION ALL SELECT 1, 'one', 7) s WHERE s.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.a, s.g FROM (SELECT * FROM vjc_g UNION ALL SELECT 1, 'one', 7) s ORDER BY s.a;

-- ============================================================================
-- Renamed and reordered columns
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT g, a, k FROM vjc_g) s WHERE s.a = 5;

-- begin-expected
-- columns: gg
-- row: 2
-- end-expected
SELECT s.gg FROM (SELECT a, k, g AS gg FROM vjc_g) s WHERE s.a = 5;

-- the column the expression reads is exposed under another name
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT a AS aa, k, g FROM vjc_g) s WHERE s.aa = 5;

-- begin-expected
-- columns: g | g2
-- row: 2 | 2
-- end-expected
SELECT s.g, s.g2 FROM (SELECT a, g, g AS g2 FROM vjc_g) s WHERE s.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM vjc_g s1 WHERE s1.a = 5) s;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
WITH c AS (SELECT * FROM vjc_g) SELECT x.g FROM c x WHERE x.a = 5;

-- ============================================================================
-- What the relation hands on is a value, not a generation expression
-- ============================================================================

CREATE TABLE vjc_c AS SELECT * FROM (SELECT * FROM vjc_g) s WHERE s.a = 5;

-- begin-expected
-- columns: a | k | g
-- row: 5 | five | 2
-- end-expected
SELECT a, k, g FROM vjc_c;

INSERT INTO vjc_c SELECT 1, 'one', 99;

-- begin-expected
-- columns: a | k | g
-- row: 1 | one | 99
-- row: 5 | five | 2
-- end-expected
SELECT a, k, g FROM vjc_c ORDER BY a;

INSERT INTO vjc_o SELECT s.a, s.g::text FROM (SELECT * FROM vjc_g) s WHERE s.a = 5;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | 2
-- row: 5 | y
-- end-expected
SELECT a, note FROM vjc_o ORDER BY a, note;

DELETE FROM vjc_o WHERE note = '2';

-- ============================================================================
-- MERGE reads the source the same way, and a source that may be padded away
-- is worked out as it is scanned
-- ============================================================================

MERGE INTO vjc_o o USING vjc_g t ON o.a = t.a AND o.a = 5
  WHEN MATCHED THEN UPDATE SET note = t.g::text;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | 2
-- end-expected
SELECT a, note FROM vjc_o ORDER BY a, note;

UPDATE vjc_o SET note = 'y';

-- the source read through a query of its own is the same relation
MERGE INTO vjc_o o USING (SELECT * FROM vjc_g) t ON o.a = t.a AND o.a = 5
  WHEN MATCHED THEN UPDATE SET note = t.g::text;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | 2
-- end-expected
SELECT a, note FROM vjc_o ORDER BY a, note;

UPDATE vjc_o SET note = 'y';

-- with nothing narrowing the join, every source row reaches the assignment
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
MERGE INTO vjc_o o USING vjc_g t ON o.a = t.a
  WHEN MATCHED THEN UPDATE SET note = t.g::text;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
MERGE INTO vjc_o o USING (SELECT * FROM vjc_g) t ON o.a = t.a
  WHEN MATCHED THEN UPDATE SET note = t.g::text;

-- an arm answering every target row keeps the target and may pad the source
-- away, so every row of the source is worked out as it is scanned
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
MERGE INTO vjc_o o USING vjc_g t ON o.a = t.a AND o.a = 5
  WHEN MATCHED THEN UPDATE SET note = t.g::text
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns';

-- an arm that never names the source's generated column raises just the same
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
MERGE INTO vjc_o o USING vjc_g t ON o.a = t.a AND o.a = 5
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns';

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
MERGE INTO vjc_o o USING vjc_g t ON o.a = t.a AND o.a = 5
  WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
MERGE INTO vjc_o o USING (SELECT * FROM vjc_g) t ON o.a = t.a AND o.a = 5
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns';

-- nothing above wrote a row
-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | y
-- end-expected
SELECT a, note FROM vjc_o ORDER BY a, note;

-- a query of its own that does not expose the column has nothing to work out
MERGE INTO vjc_o o USING (SELECT a, k FROM vjc_g) t ON o.a = t.a AND o.a = 5
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns';

-- begin-expected
-- columns: a | note
-- row: 0 | ns
-- row: 5 | y
-- end-expected
SELECT a, note FROM vjc_o ORDER BY a, note;

UPDATE vjc_o SET note = 'y';

-- an arm that does nothing asks nothing of the join, which stays an inner one
MERGE INTO vjc_o o USING vjc_g t ON o.a = t.a AND o.a = 5
  WHEN MATCHED THEN UPDATE SET note = 'm'
  WHEN NOT MATCHED BY SOURCE THEN DO NOTHING;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | m
-- end-expected
SELECT a, note FROM vjc_o ORDER BY a, note;

UPDATE vjc_o SET note = 'y';

-- and a restriction written about the source itself narrows that scan
MERGE INTO vjc_o o USING vjc_g t ON o.a = t.a AND t.a = 5
  WHEN MATCHED THEN UPDATE SET note = t.g::text
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns';

-- begin-expected
-- columns: a | note
-- row: 0 | ns
-- row: 5 | 2
-- end-expected
SELECT a, note FROM vjc_o ORDER BY a, note;

UPDATE vjc_o SET note = 'y';

MERGE INTO vjc_o o USING vjc_g t ON o.a = t.a AND t.k = 'five'
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns';

-- begin-expected
-- columns: a | note
-- row: 0 | ns
-- row: 5 | y
-- end-expected
SELECT a, note FROM vjc_o ORDER BY a, note;

-- a statement that never names the column never works it out
UPDATE vjc_o o SET note = 'x' FROM (SELECT * FROM vjc_g) s WHERE s.a = o.a;

-- begin-expected
-- columns: a | note
-- row: 0 | x
-- row: 5 | x
-- end-expected
SELECT a, note FROM vjc_o ORDER BY a, note;

-- cleanup
DROP TABLE vjc_c;
DROP VIEW vjc_vo;
DROP VIEW vjc_v;
DROP TABLE vjc_g;
DROP TABLE vjc_o;

-- ============================================================================
-- Where the column is worked out decides only WHICH rows it is worked out for
--
-- The generation expression below cannot raise, so every row that comes back
-- has to carry the right value.
-- ============================================================================

-- setup
CREATE TABLE vjc_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL);
INSERT INTO vjc_h VALUES (1,'one'),(2,'two'),(3,'three');
CREATE TABLE vjc_p (a int, note text);
INSERT INTO vjc_p VALUES (1,'p1'),(9,'p9');
CREATE VIEW vjc_hv AS SELECT * FROM vjc_h;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 9 | NULL
-- end-expected
SELECT p.a, s.g FROM vjc_p p LEFT JOIN (SELECT * FROM vjc_h) s ON p.a = s.a
  WHERE p.a >= 1 ORDER BY p.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
SELECT p.a, s.g FROM vjc_p p JOIN (SELECT * FROM vjc_h) s ON p.a = s.a AND p.a >= 1
  ORDER BY p.a;

-- begin-expected
-- columns: x | z
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT s.x, s.z FROM (SELECT * FROM vjc_h) s(x,y,z) WHERE s.x >= 2 ORDER BY s.x;

-- begin-expected
-- columns: y | z
-- row: one | 10
-- row: two | 20
-- row: three | 30
-- end-expected
SELECT s.y, s.z FROM (SELECT * FROM vjc_h) s(x,y,z) ORDER BY s.z;

-- begin-expected
-- columns: x | z
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT s.x, s.z FROM vjc_hv s(x,y,z) WHERE s.x >= 2 ORDER BY s.x;

-- begin-expected
-- columns: count | sum
-- row: 3 | 60
-- end-expected
SELECT count(*), sum(s.z) FROM vjc_hv s(x,y,z);

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
SELECT o.a, s.g FROM vjc_p o, LATERAL (SELECT * FROM vjc_h WHERE a = o.a) s ORDER BY o.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- row: 9 | NULL
-- end-expected
SELECT o.a, s.g FROM vjc_p o LEFT JOIN LATERAL (SELECT * FROM vjc_h z WHERE z.a = o.a) s
  ON true ORDER BY o.a;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
SELECT s.a, s.g FROM vjc_h s, vjc_p o WHERE s.a = o.a AND o.a = 1;

-- begin-expected
-- columns: a | g
-- row: 1 | 10
-- end-expected
SELECT s.a, s.g FROM (SELECT * FROM vjc_h) s, vjc_p o WHERE s.a = o.a AND o.a = 1;

-- the row an outer join padded with nulls has no value to work out
-- begin-expected
-- columns: z
-- row: 10
-- row: NULL
-- end-expected
SELECT s.z FROM vjc_p o LEFT JOIN (SELECT * FROM vjc_h) s(x,y,z) ON o.a = s.x ORDER BY s.z;

MERGE INTO vjc_p o USING vjc_h t ON o.a = t.a
  WHEN MATCHED THEN UPDATE SET note = t.g::text
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns';

-- begin-expected
-- columns: a | note
-- row: 1 | 10
-- row: 9 | ns
-- end-expected
SELECT a, note FROM vjc_p ORDER BY a;

UPDATE vjc_p SET note = 'p' || a::text;
UPDATE vjc_p o SET note = s.g::text FROM (SELECT * FROM vjc_h) s WHERE s.a = o.a;

-- begin-expected
-- columns: a | note
-- row: 1 | 10
-- row: 9 | p9
-- end-expected
SELECT a, note FROM vjc_p ORDER BY a;

DELETE FROM vjc_p o USING vjc_h t WHERE t.a = o.a AND o.a = 1 AND t.g = 10;

-- begin-expected
-- columns: a | note
-- row: 9 | p9
-- end-expected
SELECT a, note FROM vjc_p ORDER BY a;

-- cleanup
DROP VIEW vjc_hv;
DROP TABLE vjc_h;
DROP TABLE vjc_p;
