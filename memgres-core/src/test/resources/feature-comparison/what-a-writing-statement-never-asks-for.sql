-- ============================================================================
-- What a writing statement never asks of an item kept apart from it
--
-- A WITH item PostgreSQL keeps apart from the statement reading it is computed
-- when that statement first asks it for a row. A statement whose qualification
-- is decided against before a row is read never asks: it writes nothing, the
-- item does not run, and a VIRTUAL generated column of the relation under it is
-- never worked out. That holds for a statement that writes exactly as it holds
-- for a query -- an UPDATE's FROM clause, a DELETE's USING clause and a MERGE's
-- source are read for such a statement as a query's FROM clause is read for a
-- query -- and it holds for a plain relation, a derived table and a view as
-- well as for a WITH item.
--
-- Whether the qualification is settled that early is read off the whole written
-- expression rather than off any one part of it. The parts of an AND must all
-- hold, so one of them false settles the whole of it; a branch of an OR holds
-- on its own, so an OR is settled only where every branch is. That is why
-- "false AND c.a = 5 OR false" asks for nothing while "false OR c.a = 5" is
-- read row by row.
--
-- A MERGE reads its source however the pairing reads: an arm written WHEN NOT
-- MATCHED acts on a source row that paired with nothing, so PostgreSQL
-- preserves the source side of the join and reads every row of it. An arm that
-- does nothing asks nothing of the join.
--
-- The generation expression below is 10/a over a relation holding a row with
-- a = 0, so every statement here is really asking whether the relation under
-- the item was read at all. Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE wnr_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO wnr_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE wnr_o (a int, note text);
INSERT INTO wnr_o VALUES (5,'x'),(0,'y');
CREATE TABLE wnr_w (i int, s text);
CREATE VIEW wnr_v AS SELECT * FROM wnr_g;

-- ============================================================================
-- An OR is settled before a row is read only where every branch is
-- ============================================================================

-- the left branch is settled by its own false part and the right branch is
-- written false, so neither can admit a row and the item is never asked
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE false AND c.a = 5 OR false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE false OR false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE false AND c.a = 5 OR false AND c.k = 'x';

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE (false OR false) AND c.a = 5;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE false OR (false AND c.a = 5);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE false AND c.a = 5 OR c.a = 5 AND false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE 1 = 0 OR 2 = 0;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE NULL OR false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE NULL AND c.a = 5 OR false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE false = true;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE 'f'::bool;

-- a part beside a branch holding a call still settles that branch
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE random() < 0 AND false;

-- a branch nothing can be read out of leaves the OR unsettled, so the item is
-- asked for a row and is computed in full
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE false OR c.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE c.a = 5 OR false AND c.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE false OR random() < 0;

-- NOT is not read through: what it stands over is settled false, so the whole
-- of it is true and every row is asked for
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE NOT (false OR false);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c WHERE NOT (c.a = 5 AND false);

-- and with no qualification at all
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) SELECT count(*) FROM c;

-- ============================================================================
-- The same reading settles a scan of the relation that holds the column
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM wnr_g WHERE false AND g = 2 OR false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM wnr_g WHERE g = 2 AND false OR false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM wnr_g WHERE false AND g = 2 OR g = 2 AND false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM wnr_v WHERE false AND g = 2 OR false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT * FROM wnr_g) s WHERE false AND s.g = 2 OR false;

-- begin-expected
-- columns: a
-- end-expected
SELECT a FROM wnr_g WHERE false AND g = 2 OR false;

-- a qualification only a row can settle is read row by row, as it always was
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT count(*) FROM wnr_g WHERE false OR g = 2;

-- a statement that writes reads its own qualification the same way
UPDATE wnr_g SET k = 'z' WHERE false AND g = 2 OR false;

DELETE FROM wnr_g WHERE false AND g = 2 OR false;

-- begin-expected
-- columns: a | k
-- row: 0, zero
-- row: 5, five
-- end-expected
SELECT a, k FROM wnr_g ORDER BY a;

-- ============================================================================
-- An UPDATE that writes nothing asks its FROM clause for nothing
-- ============================================================================

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM c WHERE false;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM c WHERE false AND c.a = 5;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM c WHERE false AND c.a = 5 OR false;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM c WHERE 1 = 0;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM c WHERE NULL;

-- what the assignment reads of the item changes nothing: it is worked out for
-- the rows the statement wrote, and it wrote none
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = c.k FROM c WHERE false;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = c.g::text FROM c WHERE false;

-- an inner join in the clause is settled with it
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM c, wnr_w w WHERE false;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM c JOIN wnr_w w ON false;

-- a query inside the clause is read for the same statement, so it asks for
-- nothing either, and neither does an item read by an item
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM (SELECT count(*) AS n FROM c) s WHERE false;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g), d AS MATERIALIZED (SELECT * FROM c) UPDATE wnr_o SET note = 'z' FROM d WHERE false;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- row: 5, x
-- end-expected
SELECT a, note FROM wnr_o ORDER BY a;

-- an outer join answers a row of the side it preserves whatever its condition
-- says, so that side is read and the expression is worked out
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM c LEFT JOIN wnr_w w ON false;

-- a qualification that admits no row only because of what it reads of a row is
-- read row by row, so the item is asked
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM c WHERE c.a = 99;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM c WHERE wnr_o.a = 99;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM c WHERE false OR c.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) UPDATE wnr_o SET note = 'z' FROM c;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- row: 5, x
-- end-expected
SELECT a, note FROM wnr_o ORDER BY a;

-- ============================================================================
-- A DELETE reads its USING clause the same way, whatever the relation is
-- ============================================================================

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) DELETE FROM wnr_o USING c WHERE false;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) DELETE FROM wnr_o USING c WHERE false AND c.a = 5;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) DELETE FROM wnr_o USING c WHERE false AND c.a = 5 OR false;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) DELETE FROM wnr_o USING c WHERE NULL;

-- a derived table, a view and the relation itself are asked for nothing too,
-- even where the qualification names the generated column beside the false part
DELETE FROM wnr_o USING (SELECT * FROM wnr_g) s WHERE false AND s.g = 2;

DELETE FROM wnr_o USING wnr_v WHERE false AND wnr_v.g = 2;

DELETE FROM wnr_o USING wnr_g WHERE false AND wnr_g.g = 2;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- row: 5, x
-- end-expected
SELECT a, note FROM wnr_o ORDER BY a;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) DELETE FROM wnr_o USING c WHERE c.a = 99;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) DELETE FROM wnr_o USING c;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
DELETE FROM wnr_o USING (SELECT * FROM wnr_g) s WHERE s.g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
DELETE FROM wnr_o USING wnr_v WHERE wnr_v.g = 2;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- row: 5, x
-- end-expected
SELECT a, note FROM wnr_o ORDER BY a;

-- ============================================================================
-- An INSERT reads its query the way any other query is read
-- ============================================================================

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) INSERT INTO wnr_w SELECT a, k FROM c WHERE false;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) INSERT INTO wnr_w SELECT a, k FROM c WHERE false AND c.a = 5 OR false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM wnr_w;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) INSERT INTO wnr_w SELECT a, k FROM c WHERE c.a = 99;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) INSERT INTO wnr_w SELECT a, k FROM c;

-- ============================================================================
-- A MERGE pairs nothing, unless an arm answers for an unpaired source row
-- ============================================================================

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING c ON false WHEN MATCHED THEN UPDATE SET note = 'z';

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING c ON o.a = c.a AND false WHEN MATCHED THEN UPDATE SET note = 'z';

-- an arm that does nothing asks nothing of the join
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING c ON o.a = c.a AND false WHEN MATCHED THEN UPDATE SET note = 'z' WHEN NOT MATCHED THEN DO NOTHING;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING c ON false WHEN MATCHED THEN DO NOTHING;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING c ON false WHEN MATCHED THEN DELETE;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- row: 5, x
-- end-expected
SELECT a, note FROM wnr_o ORDER BY a;

-- an arm written WHEN NOT MATCHED acts on a source row that paired with
-- nothing, so every row of the source is read whatever the condition says
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING c ON false WHEN NOT MATCHED THEN INSERT (a, note) VALUES (c.a, 'n');

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING c ON false WHEN NOT MATCHED AND c.a = 99 THEN INSERT (a, note) VALUES (c.a, 'n');

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING c ON o.a = c.a WHEN MATCHED THEN UPDATE SET note = 'z';

-- begin-expected
-- columns: a | note
-- row: 0, y
-- row: 5, x
-- end-expected
SELECT a, note FROM wnr_o ORDER BY a;

-- an arm written WHEN NOT MATCHED BY SOURCE acts on a target row instead, so
-- the source is the side that may be padded away and is never asked
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING c ON false WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 's';

-- begin-expected
-- columns: a | note
-- row: 0, s
-- row: 5, s
-- end-expected
SELECT a, note FROM wnr_o ORDER BY a;

-- an arm that does nothing does not make the source the preserved side either
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING c ON false WHEN NOT MATCHED THEN DO NOTHING WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'q';

-- begin-expected
-- columns: a | note
-- row: 0, q
-- row: 5, q
-- end-expected
SELECT a, note FROM wnr_o ORDER BY a;

-- one arm that acts on an unpaired source row is enough to have every row of
-- the source read, whatever the other arms say
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING c ON false WHEN NOT MATCHED THEN INSERT (a, note) VALUES (c.a, 'n') WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'r';

-- a source narrowed to no rows before the statement reads it is read as it
-- stands, and pairs with nothing
WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING (SELECT * FROM wnr_g WHERE false) s ON o.a = s.a WHEN MATCHED THEN UPDATE SET note = 'p';

-- begin-expected
-- columns: a | note
-- row: 0, q
-- row: 5, q
-- end-expected
SELECT a, note FROM wnr_o ORDER BY a;

WITH c AS MATERIALIZED (SELECT * FROM wnr_g) MERGE INTO wnr_o o USING c ON false WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM wnr_o;

-- cleanup
DROP VIEW wnr_v;
DROP TABLE wnr_w;
DROP TABLE wnr_o;
DROP TABLE wnr_g;
