-- ============================================================================
-- A column alias list renames the reference, not the column behind it
--
-- A FROM item may give a relation's columns names of its own. PostgreSQL
-- renames the references to them and nothing else, so a generated column
-- reached through such a list still answers with the value its generation
-- expression works out -- and that expression is still written in the names the
-- relation underneath answers to. The VIRTUAL expression used below is 10/a,
-- which raises 22012 for the row a = 0, so a statement that answers at all is
-- one that reached the expression for the rows it read the column of and for no
-- others.
--
-- Every value here was measured against PostgreSQL 18.
-- ============================================================================

CREATE TABLE zal_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO zal_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE zal_s (a int, k text, g int GENERATED ALWAYS AS (a*2) STORED);
INSERT INTO zal_s (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE zal_o (a int, note text);
INSERT INTO zal_o VALUES (5,'x'),(0,'y');
CREATE VIEW zal_v AS SELECT * FROM zal_g;
CREATE MATERIALIZED VIEW zal_m AS SELECT * FROM zal_g WHERE a = 5;
CREATE MATERIALIZED VIEW zal_sm AS SELECT * FROM zal_s;

-- the renamed generated column answers with its value
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_g s(x,y,z) WHERE s.x = 5;

-- the same name written without its relation
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT z FROM zal_g s(x,y,z) WHERE x = 5;

-- and written with AS and spaces around the list
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_g AS s (x, y, z) WHERE s.x = 5;

-- a star reads every column under the names the list gave them
-- begin-expected
-- columns: x | y | z
-- row: 5, five, 2
-- end-expected
SELECT * FROM zal_g s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: x | y | z
-- row: 5, five, 2
-- end-expected
SELECT s.* FROM zal_g s(x,y,z) WHERE s.x = 5;

-- a column the list did not rename away is unaffected
-- begin-expected
-- columns: y
-- row: five
-- end-expected
SELECT s.y FROM zal_g s(x,y,z) WHERE s.x = 5;

-- a STORED generated column is a value the row holds, list or no list
-- begin-expected
-- columns: z
-- row: 0
-- row: 10
-- end-expected
SELECT s.z FROM zal_s s(x,y,z) ORDER BY 1;

-- a list shorter than the relation leaves the rest their own names
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM zal_g s(x) WHERE s.x = 5;

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT s.k FROM zal_g s(x) WHERE s.x = 5;

-- begin-expected
-- columns: x | k | g
-- row: 5, five, 2
-- end-expected
SELECT * FROM zal_g s(x) WHERE s.x = 5;

-- a list naming more columns than the relation has
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "s" has 3 columns available but 4 columns specified
-- end-expected-error
SELECT s.z FROM zal_g s(x,y,z,w) WHERE s.x = 5;

-- the name the list renamed away is gone
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.a does not exist
-- end-expected-error
SELECT s.a FROM zal_g s(x,y,z) WHERE s.x = 5;

-- and so is the generated column's own name
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.g does not exist
-- end-expected-error
SELECT s.z FROM zal_g s(x,y,z) WHERE s.x = 5 AND s.g = 2;

-- a list may give one column the name another column had, and the generation
-- expression underneath is still written in the relation's own names
-- begin-expected
-- columns: k
-- row: 2
-- end-expected
SELECT s.k FROM zal_g s(g,a,k) WHERE s.g = 5;

-- begin-expected
-- columns: a
-- row: five
-- end-expected
SELECT s.a FROM zal_g s(g,a,k) WHERE s.g = 5;

-- the qualification may name the renamed column itself
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_g s(x,y,z) WHERE s.x = 5 AND s.z = 2;

-- and then it reaches every row, because nothing else decided them first
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.z FROM zal_g s(x,y,z) WHERE s.z = 2;

-- a column nothing names is not worked out
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM zal_g s(x,y,z);


-- ============================================================================
-- Every kind of relation a list can rename
-- ============================================================================

-- a view
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT z FROM zal_v AS s(x,y,z) WHERE x = 5;

-- begin-expected
-- columns: y
-- row: five
-- end-expected
SELECT y FROM zal_v AS s(x,y,z) WHERE x = 5;

-- begin-expected
-- columns: x
-- row: 0
-- row: 5
-- end-expected
SELECT x FROM zal_v AS s(x,y,z) ORDER BY 1;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_v s(x,y,z) WHERE s.y = 'five';

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "s" has 3 columns available but 4 columns specified
-- end-expected-error
SELECT s.z FROM zal_v s(x,y,z,w);

-- a materialized view
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_m s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_m s(x,y,z) WHERE s.y = 'five';

-- begin-expected
-- columns: z
-- row: 0
-- row: 10
-- end-expected
SELECT s.z FROM zal_sm s(x,y,z) ORDER BY 1;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "s" has 3 columns available but 4 columns specified
-- end-expected-error
SELECT s.z FROM zal_m s(x,y,z,w);

-- a WITH item
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
WITH c AS (SELECT * FROM zal_g) SELECT s.z FROM c s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
WITH c AS (SELECT * FROM zal_g) SELECT s.z FROM c s(x,y,z) WHERE s.y = 'five';

-- a WITH item that names its own columns, renamed again by the reference
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
WITH c(p,q,r) AS (SELECT * FROM zal_g) SELECT s.z FROM c s(x,y,z) WHERE s.x = 5;

-- and read under the names the item itself gave them
-- begin-expected
-- columns: r
-- row: 2
-- end-expected
WITH c(p,q,r) AS (SELECT * FROM zal_g) SELECT r FROM c WHERE p = 5;

-- begin-expected
-- columns: q
-- row: five
-- end-expected
WITH c(p,q,r) AS (SELECT * FROM zal_g) SELECT q FROM c WHERE p = 5;

-- a WITH item's own list may be shorter than the query under it
-- begin-expected
-- columns: p
-- row: 0
-- row: 5
-- end-expected
WITH c(p,q) AS (SELECT * FROM zal_g) SELECT p FROM c ORDER BY 1;

-- but not longer, and the item is named as one
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: WITH query "c" has 3 columns available but 4 columns specified
-- end-expected-error
WITH c(p,q,r,s) AS (SELECT * FROM zal_g) SELECT p FROM c;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "s" has 3 columns available but 4 columns specified
-- end-expected-error
WITH c AS (SELECT * FROM zal_g) SELECT s.z FROM c s(x,y,z,w);

-- an item kept apart is computed before the query reading it is planned
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM zal_g WHERE a = 5) SELECT s.z FROM c s(x,y,z);

-- a derived table
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM (SELECT * FROM zal_g) s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM (SELECT * FROM zal_g) s(x,y,z) WHERE s.y = 'five';

-- one derived table over another, each with a list of its own
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM (SELECT * FROM (SELECT * FROM zal_g) t(p,q,r)) s(x,y,z) WHERE s.x = 5;

-- a derived table over a view
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM (SELECT * FROM zal_v) s(x,y,z) WHERE s.x = 5;

-- a derived table whose own select list already renamed the column
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM (SELECT a, k, g AS gg FROM zal_g) s(x,y,z) WHERE s.x = 5;

-- a derived table that put the generated column first
-- begin-expected
-- columns: z
-- row: 5
-- end-expected
SELECT s.z FROM (SELECT g, k, a FROM zal_g) s(x,y,z) WHERE s.z = 5;

-- begin-expected
-- columns: x
-- row: 2
-- end-expected
SELECT s.x FROM (SELECT g, k, a FROM zal_g) s(x,y,z) WHERE s.z = 5;

-- both arms of a set operation answer to the names the list gave
-- begin-expected
-- columns: z
-- row: 2
-- row: 2
-- end-expected
SELECT s.z FROM (SELECT * FROM zal_g WHERE a = 5 UNION ALL SELECT * FROM zal_g WHERE a = 5) s(x,y,z);

-- a LIMIT settles which rows there are before the enclosing qualification
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.z FROM (SELECT * FROM zal_g LIMIT 2) s(x,y,z) WHERE s.x = 5;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "s" has 3 columns available but 4 columns specified
-- end-expected-error
SELECT s.z FROM (SELECT * FROM zal_g) s(x,y,z,w);

-- a VALUES list
-- begin-expected
-- columns: b | c
-- row: 2, b
-- end-expected
SELECT s.b, s.c FROM (VALUES (1,'a'),(2,'b')) s(b,c) WHERE s.b = 2;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "s" has 2 columns available but 3 columns specified
-- end-expected-error
SELECT s.b FROM (VALUES (1,'a')) s(b,c,d);

-- a set-returning function
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT s.n FROM generate_series(1,3) WITH ORDINALITY s(n,o) ORDER BY 1;

-- begin-expected
-- columns: o
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT s.o FROM generate_series(1,3) WITH ORDINALITY s(n,o) ORDER BY 1;

-- begin-expected
-- columns: v
-- row: 10
-- row: 20
-- end-expected
SELECT s.v FROM unnest(ARRAY[10,20]) s(v) ORDER BY 1;

-- begin-expected
-- columns: v | o
-- row: 10, 1
-- row: 20, 2
-- end-expected
SELECT s.v, s.o FROM unnest(ARRAY[10,20]) WITH ORDINALITY s(v,o) ORDER BY 1;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "s" has 1 columns available but 2 columns specified
-- end-expected-error
SELECT s.n FROM generate_series(1,3) s(n,o);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "s" has 2 columns available but 3 columns specified
-- end-expected-error
SELECT s.o FROM generate_series(1,3) WITH ORDINALITY s(n,o,p);


-- ============================================================================
-- A list over a relation the query reaches through a join
-- ============================================================================

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_o o JOIN zal_g s(x,y,z) ON o.a = s.x WHERE o.a = 5;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_o o LEFT JOIN zal_g s(x,y,z) ON o.a = s.x WHERE o.a = 5;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_g s(x,y,z) RIGHT JOIN zal_o o ON o.a = s.x WHERE o.a = 5;

-- begin-expected
-- columns: a | z
-- row: 5, 2
-- end-expected
SELECT o.a, s.z FROM zal_o o FULL JOIN zal_g s(x,y,z) ON o.a = s.x WHERE o.a = 5;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_o o, zal_g s(x,y,z) WHERE s.x = o.a AND o.a = 5;

-- the row an outer join found nothing for has no value to work out
-- begin-expected
-- columns: z
-- row: 2
-- row: NULL
-- end-expected
SELECT s.z FROM zal_o o LEFT JOIN zal_g s(x,y,z) ON o.a = s.x AND o.a = 5 ORDER BY 1;

-- and where nothing at all restricts it, the expression reaches every row
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT o.a, s.z FROM zal_o o LEFT JOIN zal_g s(x,y,z) ON o.a = s.x ORDER BY o.a;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_o o CROSS JOIN LATERAL (SELECT * FROM zal_g WHERE a = o.a) s(x,y,z) WHERE o.a = 5;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_o o, LATERAL (SELECT * FROM zal_g WHERE a = o.a) s(x,y,z) WHERE o.a = 5;

-- a parenthesised join wearing a list of its own
-- begin-expected
-- columns: x | z
-- row: 5, 2
-- end-expected
SELECT j.x, j.z FROM (zal_g JOIN zal_o ON zal_g.a = zal_o.a) j(x,y,z,w,u) WHERE j.x = 5;

-- a qualification of any shape reaches the relation under the list
-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_g s(x,y,z) WHERE s.x = (SELECT 5);

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_g s(x,y,z) WHERE s.x = abs(-5);

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_g s(x,y,z) WHERE s.x IN (5);

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_g s(x,y,z) WHERE s.y LIKE 'f%';

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_g s(x,y,z) WHERE s.x = 5 OR s.y = 'nope';

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_g s(x,y,z) WHERE NOT (s.x = 0);

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_g s(x,y,z) WHERE s.x = 5 FOR UPDATE;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM ONLY zal_g s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: z | count
-- row: 2, 1
-- end-expected
SELECT s.z, count(*) FROM zal_g s(x,y,z) WHERE s.x = 5 GROUP BY s.z;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM zal_g s(x,y,z) WHERE s.x = 5 UNION SELECT s.z FROM zal_g s(x,y,z) WHERE s.x = 5;

-- begin-expected
-- columns: max
-- row: 2
-- end-expected
SELECT (SELECT max(s.z) FROM zal_g s(x,y,z) WHERE s.x = o.a) FROM zal_o o WHERE o.a = 5;

-- begin-expected
-- columns: note
-- row: x
-- end-expected
SELECT o.note FROM zal_o o WHERE o.a IN (SELECT s.x FROM zal_g s(x,y,z) WHERE s.x = 5);

DROP MATERIALIZED VIEW zal_sm;
DROP MATERIALIZED VIEW zal_m;
DROP VIEW zal_v;
DROP TABLE zal_o;
DROP TABLE zal_s;
DROP TABLE zal_g;


-- ============================================================================
-- The value a renamed generated column carries, where nothing can raise
--
-- The expressions below cannot fail for any row, so every answer states the
-- value itself: what a relation behind an alias list carries is what the
-- relation underneath carries.
-- ============================================================================

CREATE TABLE zal_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL);
INSERT INTO zal_h (a,k) VALUES (1,'one'),(2,'two'),(3,'three');
CREATE TABLE zal_hs (a int, k text, g int GENERATED ALWAYS AS (a*10) STORED);
INSERT INTO zal_hs (a,k) VALUES (1,'one'),(2,'two'),(3,'three');
CREATE VIEW zal_hv AS SELECT * FROM zal_h;
CREATE MATERIALIZED VIEW zal_hm AS SELECT * FROM zal_h;
CREATE TABLE zal_p (a int, note text);
INSERT INTO zal_p VALUES (1,'p1'),(9,'p9');

-- begin-expected
-- columns: x | y | z
-- row: 1, one, 10
-- row: 2, two, 20
-- row: 3, three, 30
-- end-expected
SELECT s.x, s.y, s.z FROM zal_h s(x,y,z) ORDER BY s.x;

-- begin-expected
-- columns: x | y | z
-- row: 1, one, 10
-- row: 2, two, 20
-- row: 3, three, 30
-- end-expected
SELECT s.x, s.y, s.z FROM zal_hs s(x,y,z) ORDER BY s.x;

-- begin-expected
-- columns: x | y | z
-- row: 1, one, 10
-- row: 2, two, 20
-- row: 3, three, 30
-- end-expected
SELECT s.x, s.y, s.z FROM zal_hv s(x,y,z) ORDER BY s.x;

-- begin-expected
-- columns: x | y | z
-- row: 1, one, 10
-- row: 2, two, 20
-- row: 3, three, 30
-- end-expected
SELECT s.x, s.y, s.z FROM zal_hm s(x,y,z) ORDER BY s.x;

-- begin-expected
-- columns: x | g
-- row: 1, 10
-- row: 2, 20
-- row: 3, 30
-- end-expected
SELECT s.x, s.g FROM zal_h s(x) ORDER BY s.x;

-- begin-expected
-- columns: z
-- row: 10
-- row: 20
-- row: 30
-- end-expected
SELECT s.z FROM (SELECT * FROM zal_h) s(x,y,z) ORDER BY 1;

-- begin-expected
-- columns: z
-- row: 10
-- row: 20
-- row: 30
-- end-expected
WITH c AS (SELECT * FROM zal_h) SELECT s.z FROM c s(x,y,z) ORDER BY 1;

-- the renamed column may be compared, grouped, ordered and aggregated
-- begin-expected
-- columns: z
-- row: 20
-- end-expected
SELECT s.z FROM zal_h s(x,y,z) WHERE s.z = 20;

-- begin-expected
-- columns: x | z
-- row: 2, 20
-- end-expected
SELECT s.x, s.z FROM zal_h s(x,y,z) WHERE s.y = 'two';

-- begin-expected
-- columns: sum
-- row: 60
-- end-expected
SELECT sum(s.z) FROM zal_h s(x,y,z);

-- begin-expected
-- columns: y | max
-- row: one, 10
-- row: three, 30
-- row: two, 20
-- end-expected
SELECT s.y, max(s.z) FROM zal_h s(x,y,z) GROUP BY s.y ORDER BY 1;

-- begin-expected
-- columns: z
-- row: 30
-- end-expected
SELECT s.z FROM zal_h s(x,y,z) ORDER BY s.z DESC LIMIT 1;

-- an outer join's unmatched row has no value under the list either
-- begin-expected
-- columns: a | z
-- row: 1, 10
-- row: 9, NULL
-- end-expected
SELECT p.a, s.z FROM zal_p p LEFT JOIN zal_h s(x,y,z) ON p.a = s.x ORDER BY p.a;

-- begin-expected
-- columns: a | z
-- row: 1, 10
-- row: 9, NULL
-- end-expected
SELECT p.a, s.z FROM zal_p p LEFT JOIN zal_hs s(x,y,z) ON p.a = s.x ORDER BY p.a;

-- an alias list on each side of a join
-- begin-expected
-- columns: note | z
-- row: p1, 10
-- end-expected
SELECT b.note, s.z FROM zal_p b(a,note) JOIN zal_h s(x,y,z) ON b.a = s.x ORDER BY b.a;

-- a list takes the join column's name away, so a natural join finds nothing to
-- join on and every pair of rows is kept
-- begin-expected
-- columns: z
-- row: 10
-- row: 10
-- row: 20
-- row: 20
-- row: 30
-- row: 30
-- end-expected
SELECT s.z FROM zal_h s(x,y,z) NATURAL JOIN zal_p ORDER BY 1;

-- a parenthesised join wearing a list of its own
-- begin-expected
-- columns: z
-- row: 10
-- end-expected
SELECT j.z FROM (zal_h JOIN zal_p ON zal_h.a = zal_p.a) j(x,y,z,w,q) ORDER BY 1;

-- a view may be defined over a relation wearing an alias list
CREATE VIEW zal_w AS SELECT s.x, s.z FROM zal_h s(x,y,z);

-- begin-expected
-- columns: x | z
-- row: 1, 10
-- row: 2, 20
-- row: 3, 30
-- end-expected
SELECT * FROM zal_w ORDER BY 1;


-- ============================================================================
-- A list on a relation a writing statement brings in beside its target
-- ============================================================================

UPDATE zal_p p SET note = s.z::text FROM zal_h s(x,y,z) WHERE s.x = p.a;

-- begin-expected
-- columns: a | note
-- row: 1, 10
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

UPDATE zal_p p SET note = 's' || s.z::text FROM zal_hs s(x,y,z) WHERE s.x = p.a;

-- begin-expected
-- columns: a | note
-- row: 1, s10
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

UPDATE zal_p p SET note = 'd' || s.z::text FROM (SELECT * FROM zal_h) s(x,y,z) WHERE s.x = p.a;

-- begin-expected
-- columns: a | note
-- row: 1, d10
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

UPDATE zal_p p SET note = 'v' || s.z::text FROM zal_hv s(x,y,z) WHERE s.x = p.a;

-- begin-expected
-- columns: a | note
-- row: 1, v10
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

UPDATE zal_p p SET note = 'm' || s.z::text FROM zal_hm s(x,y,z) WHERE s.x = p.a;

-- begin-expected
-- columns: a | note
-- row: 1, m10
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

WITH c AS (SELECT * FROM zal_h) UPDATE zal_p p SET note = 'w' || s.z::text FROM c s(x,y,z) WHERE s.x = p.a;

-- begin-expected
-- columns: a | note
-- row: 1, w10
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

UPDATE zal_p p SET note = s.c FROM (VALUES (1,'vv')) s(b,c) WHERE s.b = p.a;

-- begin-expected
-- columns: a | note
-- row: 1, vv
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

UPDATE zal_p p SET note = s.o::text FROM generate_series(1,1) WITH ORDINALITY s(n,o) WHERE s.n = p.a;

-- begin-expected
-- columns: a | note
-- row: 1, 1
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

-- the assigned value is read the same way in a RETURNING list
-- begin-expected
-- columns: a | note
-- row: 1, z10
-- end-expected
UPDATE zal_p p SET note = 'z' || s.z::text FROM zal_h s(x,y,z) WHERE s.x = p.a RETURNING p.a, p.note;

MERGE INTO zal_p p USING zal_h s(x,y,z) ON p.a = s.x WHEN MATCHED THEN UPDATE SET note = 'm' || s.z::text;

-- begin-expected
-- columns: a | note
-- row: 1, m10
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

MERGE INTO zal_p p USING (SELECT * FROM zal_h) s(x,y,z) ON p.a = s.x WHEN MATCHED THEN UPDATE SET note = 'd' || s.z::text;

-- begin-expected
-- columns: a | note
-- row: 1, d10
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

WITH c AS (SELECT * FROM zal_h) MERGE INTO zal_p p USING c s(x,y,z) ON p.a = s.x WHEN MATCHED THEN UPDATE SET note = 'c' || s.z::text;

-- begin-expected
-- columns: a | note
-- row: 1, c10
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

MERGE INTO zal_p p USING zal_hv s(x,y,z) ON p.a = s.x WHEN MATCHED THEN UPDATE SET note = 'v' || s.z::text;

-- begin-expected
-- columns: a | note
-- row: 1, v10
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

MERGE INTO zal_p p USING zal_hm s(x,y,z) ON p.a = s.x WHEN MATCHED THEN UPDATE SET note = 'w' || s.z::text;

-- begin-expected
-- columns: a | note
-- row: 1, w10
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

MERGE INTO zal_p p USING zal_hs s(x,y,z) ON p.a = s.x WHEN MATCHED THEN UPDATE SET note = 's' || s.z::text;

-- begin-expected
-- columns: a | note
-- row: 1, s10
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

MERGE INTO zal_p p USING (VALUES (9,'vv')) s(b,c) ON p.a = s.b WHEN MATCHED THEN UPDATE SET note = s.c;

-- begin-expected
-- columns: a | note
-- row: 1, s10
-- row: 9, vv
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

MERGE INTO zal_p p USING zal_h s(x,y,z) ON p.a = s.x WHEN NOT MATCHED THEN INSERT (a, note) VALUES (s.x, 'n' || s.z::text);

-- begin-expected
-- columns: a | note
-- row: 1, s10
-- row: 2, n20
-- row: 3, n30
-- row: 9, vv
-- end-expected
SELECT a, note FROM zal_p ORDER BY a;

CREATE TABLE zal_d (a int, note text);
INSERT INTO zal_d VALUES (1,'p1'),(2,'p2'),(3,'p3'),(9,'p9');

DELETE FROM zal_d p USING zal_h s(x,y,z) WHERE s.x = p.a AND s.z = 30;

-- begin-expected
-- columns: a | note
-- row: 1, p1
-- row: 2, p2
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_d ORDER BY a;

DELETE FROM zal_d p USING (SELECT * FROM zal_h) s(x,y,z) WHERE s.x = p.a AND s.z = 20;

-- begin-expected
-- columns: a | note
-- row: 1, p1
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_d ORDER BY a;

WITH c AS (SELECT * FROM zal_h) DELETE FROM zal_d p USING c s(x,y,z) WHERE s.x = p.a AND s.z = 10;

-- begin-expected
-- columns: a | note
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_d ORDER BY a;

INSERT INTO zal_d VALUES (1,'p1'),(2,'p2'),(3,'p3');

DELETE FROM zal_d p USING zal_hv s(x,y,z) WHERE s.x = p.a AND s.z = 10;

-- begin-expected
-- columns: a | note
-- row: 2, p2
-- row: 3, p3
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_d ORDER BY a;

DELETE FROM zal_d p USING zal_hm s(x,y,z) WHERE s.x = p.a AND s.z = 20;

-- begin-expected
-- columns: a | note
-- row: 3, p3
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_d ORDER BY a;

DELETE FROM zal_d p USING zal_hs s(x,y,z) WHERE s.x = p.a AND s.z = 30;

-- begin-expected
-- columns: a | note
-- row: 9, p9
-- end-expected
SELECT a, note FROM zal_d ORDER BY a;

CREATE TABLE zal_i (a int, note text);
INSERT INTO zal_i SELECT s.x, s.z::text FROM zal_h s(x,y,z) WHERE s.x = 3;
INSERT INTO zal_i SELECT s.x, s.z::text FROM zal_hv s(x,y,z) WHERE s.x = 2;
INSERT INTO zal_i SELECT s.b, s.c FROM (VALUES (7,'seven')) s(b,c);
INSERT INTO zal_i SELECT s.n, s.o::text FROM generate_series(5,5) WITH ORDINALITY s(n,o);

-- begin-expected
-- columns: a | note
-- row: 2, 20
-- row: 3, 30
-- row: 5, 1
-- row: 7, seven
-- end-expected
SELECT a, note FROM zal_i ORDER BY a;

DROP TABLE zal_i;
DROP TABLE zal_d;
DROP TABLE zal_p;
DROP VIEW zal_w;
DROP MATERIALIZED VIEW zal_hm;
DROP VIEW zal_hv;
DROP TABLE zal_hs;
DROP TABLE zal_h;
