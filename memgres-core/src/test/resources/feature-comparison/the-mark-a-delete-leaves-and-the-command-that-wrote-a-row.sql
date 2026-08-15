-- ============================================================================
-- The mark a DELETE leaves on a row, and the command identifier that wrote it
--
-- A DELETE writes the deleting transaction's id into xmax of the version it
-- takes, and leaves it there whatever becomes of the transaction. An INSERT
-- and an UPDATE report the version they wrote, whose xmax is nobody's yet, so
-- both answer 0 -- which is the control for the DELETE.
--
-- cmin is the command identifier the writing statement held, and a statement
-- takes one identifier for every catalogue row it wrote: three for a table
-- taken down, two for a view created, four for one dropped, two for a sequence
-- created, one apiece for CREATE TABLE, ALTER TABLE and DROP SEQUENCE, and
-- none at all for a statement that wrote nothing.
--
-- The transaction id itself cannot be written down, so it is asked for as a
-- comparison against pg_current_xact_id(). Every value below was measured
-- against PostgreSQL 18.
-- ============================================================================

-- ============================================================================
-- DELETE ... RETURNING xmax names the deleting transaction; INSERT and UPDATE
-- leave it at zero
-- ============================================================================
CREATE TABLE mdc_a (i int);
INSERT INTO mdc_a VALUES (1),(2),(3),(4);

-- a row nobody has removed is marked with nobody
-- begin-expected
-- columns: i | xmax
-- row: 1 | 0
-- row: 2 | 0
-- row: 3 | 0
-- row: 4 | 0
-- end-expected
SELECT i, xmax::text AS xmax FROM mdc_a ORDER BY i;

-- begin-expected
-- columns: i | mine
-- row: 1 | t
-- end-expected
DELETE FROM mdc_a WHERE i = 1
  RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine;

-- every row one DELETE takes is marked by the same transaction
-- begin-expected
-- columns: i | mine
-- row: 2 | t
-- row: 3 | t
-- end-expected
DELETE FROM mdc_a WHERE i IN (2,3)
  RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine;

-- the control: what an INSERT and an UPDATE report is the version they wrote
-- begin-expected
-- columns: i | xmax
-- row: 5 | 0
-- end-expected
INSERT INTO mdc_a VALUES (5) RETURNING i, xmax::text AS xmax;

-- begin-expected
-- columns: i | xmax
-- row: 20 | 0
-- end-expected
UPDATE mdc_a SET i = 20 WHERE i = 4 RETURNING i, xmax::text AS xmax;

-- a DELETE inside a block marks the row for the block's transaction
BEGIN;

-- begin-expected
-- columns: i | mine
-- row: 5 | t
-- end-expected
DELETE FROM mdc_a WHERE i = 5
  RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine;

-- begin-expected
-- columns: i | xmax
-- row: 6 | 0
-- end-expected
INSERT INTO mdc_a VALUES (6) RETURNING i, xmax::text AS xmax;

-- begin-expected
-- columns: i | xmax
-- row: 21 | 0
-- end-expected
UPDATE mdc_a SET i = 21 WHERE i = 20 RETURNING i, xmax::text AS xmax;

COMMIT;

-- what the block wrote carries no mark of its own
-- begin-expected
-- columns: i | marked
-- row: 6 | f
-- row: 21 | f
-- end-expected
SELECT i, xmax::text <> '0' AS marked FROM mdc_a ORDER BY i;

-- the mark stays on the version a rolled-back DELETE put back
BEGIN;
DELETE FROM mdc_a WHERE i = 6;
ROLLBACK;

-- begin-expected
-- columns: i | marked
-- row: 6 | t
-- row: 21 | f
-- end-expected
SELECT i, xmax::text <> '0' AS marked FROM mdc_a ORDER BY i;

-- a DELETE read through a modifying WITH item is marked the same way
-- begin-expected
-- columns: i | mine
-- row: 21 | t
-- end-expected
WITH d AS (DELETE FROM mdc_a WHERE i = 21 RETURNING i, xmax)
SELECT i, xmax = pg_current_xact_id()::text::xid AS mine FROM d;

DROP TABLE mdc_a;

-- ============================================================================
-- A row removed through a parent is marked in the relation that stores it
-- ============================================================================
CREATE TABLE mdc_bp (i int, s text);
CREATE TABLE mdc_bc (extra int) INHERITS (mdc_bp);
INSERT INTO mdc_bp VALUES (1,'a');
INSERT INTO mdc_bc VALUES (2,'b',7);

-- begin-expected
-- columns: i | mine
-- row: 1 | t
-- end-expected
DELETE FROM mdc_bp WHERE i = 1
  RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine;

-- the child's row, taken through the parent
-- begin-expected
-- columns: i | mine
-- row: 2 | t
-- end-expected
DELETE FROM mdc_bp WHERE i = 2
  RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine;

INSERT INTO mdc_bc VALUES (3,'c',8);

-- and the same row taken through the child
-- begin-expected
-- columns: i | mine
-- row: 3 | t
-- end-expected
DELETE FROM mdc_bc WHERE i = 3
  RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine;

DROP TABLE mdc_bc;
DROP TABLE mdc_bp;

CREATE TABLE mdc_c (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE mdc_c0 PARTITION OF mdc_c FOR VALUES FROM (0) TO (100);
INSERT INTO mdc_c VALUES (1,'a'),(2,'b'),(3,'c');

-- begin-expected
-- columns: i | mine
-- row: 1 | t
-- end-expected
DELETE FROM mdc_c WHERE i = 1
  RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine;

-- begin-expected
-- columns: i | mine
-- row: 2 | t
-- end-expected
DELETE FROM mdc_c0 WHERE i = 2
  RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine;

-- begin-expected
-- columns: i | xmax
-- row: 3 | 0
-- end-expected
UPDATE mdc_c SET s = 'z' WHERE i = 3 RETURNING i, xmax::text AS xmax;

DROP TABLE mdc_c;

-- ============================================================================
-- cmin counts the catalogue rows each statement wrote
-- ============================================================================
BEGIN;
CREATE TABLE mdc_d (i int);
CREATE TABLE mdc_d1 (i int);
INSERT INTO mdc_d VALUES (1);
DROP TABLE mdc_d1;
INSERT INTO mdc_d VALUES (2);
CREATE VIEW mdc_dv AS SELECT 1 AS x;
INSERT INTO mdc_d VALUES (3);
DROP VIEW mdc_dv;
INSERT INTO mdc_d VALUES (4);
CREATE SEQUENCE mdc_ds;
INSERT INTO mdc_d VALUES (5);
DROP SEQUENCE mdc_ds;
INSERT INTO mdc_d VALUES (6);
CREATE TABLE mdc_d2 (i int);
INSERT INTO mdc_d VALUES (7);
ALTER TABLE mdc_d2 ADD COLUMN j int;
INSERT INTO mdc_d VALUES (8);

-- a DROP over a name nothing answers to writes no catalogue row and costs
-- nothing, so the next statement takes the identifier after the last INSERT's
DROP TABLE IF EXISTS mdc_nothere;
INSERT INTO mdc_d VALUES (9);

-- begin-expected
-- columns: i | cmin
-- row: 1 | 2
-- row: 2 | 6
-- row: 3 | 9
-- row: 4 | 14
-- row: 5 | 17
-- row: 6 | 19
-- row: 7 | 21
-- row: 8 | 23
-- row: 9 | 24
-- end-expected
SELECT i, cmin::text AS cmin FROM mdc_d ORDER BY i;

ROLLBACK;

-- ============================================================================
-- A statement that reads takes no identifier; one that writes takes one even
-- when it wrote no row of its own
-- ============================================================================
BEGIN;
CREATE TABLE mdc_e (i int);
INSERT INTO mdc_e VALUES (1);
SELECT 1;
INSERT INTO mdc_e VALUES (2);
UPDATE mdc_e SET i = i WHERE false;
INSERT INTO mdc_e VALUES (3);
DELETE FROM mdc_e WHERE false;
INSERT INTO mdc_e VALUES (4);

-- begin-expected
-- columns: i | cmin
-- row: 1 | 1
-- row: 2 | 2
-- row: 3 | 4
-- row: 4 | 6
-- end-expected
SELECT i, cmin::text AS cmin FROM mdc_e ORDER BY i;

ROLLBACK;

-- ============================================================================
-- A command identifier a rolled-back savepoint spent is not handed out again
-- ============================================================================
BEGIN;
CREATE TABLE mdc_f (i int);
INSERT INTO mdc_f VALUES (1);
SAVEPOINT sp;
INSERT INTO mdc_f VALUES (2);
ROLLBACK TO SAVEPOINT sp;
INSERT INTO mdc_f VALUES (3);

-- begin-expected
-- columns: i | cmin
-- row: 1 | 1
-- row: 3 | 3
-- end-expected
SELECT i, cmin::text AS cmin FROM mdc_f ORDER BY i;

COMMIT;

-- the identifier is written on the version, so it reads the same afterwards
-- begin-expected
-- columns: i | cmin
-- row: 1 | 1
-- row: 3 | 3
-- end-expected
SELECT i, cmin::text AS cmin FROM mdc_f ORDER BY i;

DROP TABLE mdc_f;

-- ============================================================================
-- cmin and cmax of a version this transaction wrote are the same command
-- ============================================================================
BEGIN;
CREATE TABLE mdc_g (i int);
INSERT INTO mdc_g VALUES (1);
UPDATE mdc_g SET i = 2;

-- begin-expected
-- columns: i | cmin | cmax
-- row: 2 | 2 | 2
-- end-expected
SELECT i, cmin::text AS cmin, cmax::text AS cmax FROM mdc_g ORDER BY i;

INSERT INTO mdc_g VALUES (3);

-- begin-expected
-- columns: i | cmin | cmax
-- row: 2 | 2 | 2
-- row: 3 | 3 | 3
-- end-expected
SELECT i, cmin::text AS cmin, cmax::text AS cmax FROM mdc_g ORDER BY i;

COMMIT;
DROP TABLE mdc_g;
