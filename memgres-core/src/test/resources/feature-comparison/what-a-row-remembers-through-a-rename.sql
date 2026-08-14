-- ============================================================================
-- What a row remembers through a rename
--
-- A row's place, the transaction that wrote it and the command identifier that
-- statement held all belong to the relation that stores the row, not to the
-- name the relation stands under. So a rename, a move to another schema and a
-- rename of the schema itself rewrite no tuple: ctid, xmin, xmax and cmin all
-- answer afterwards exactly what they answered before, and only tableoid --
-- which is the relation's name -- reads differently.
--
-- The relation goes on numbering from where it left off, a rename undone by a
-- ROLLBACK leaves the rows answering under the name they had, and a relation
-- made again under a name that has been used before starts clean.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

CREATE TABLE zzp5tr_r (i int, s text);
INSERT INTO zzp5tr_r VALUES (1,'a'),(2,'b'),(3,'c');
DELETE FROM zzp5tr_r WHERE i = 2;
UPDATE zzp5tr_r SET s = 'cc' WHERE i = 3;

-- ============================================================================
-- A rename leaves every row's place, its writer and its command identifier
-- exactly as they were
-- ============================================================================

-- the update wrote a new version, which sits after the three the insert wrote
-- begin-expected
-- columns: i | ctid | xmin_set | xmax | cmin_set | relation
-- row: 1 | (0,1) | t | 0 | f | zzp5tr_r
-- row: 3 | (0,4) | t | 0 | f | zzp5tr_r
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set, xmax, cmin::text <> '0' AS cmin_set,
       tableoid::regclass::text AS relation FROM zzp5tr_r ORDER BY i;

ALTER TABLE zzp5tr_r RENAME TO zzp5tr_r2;

-- begin-expected
-- columns: i | ctid | xmin_set | xmax | cmin_set | relation
-- row: 1 | (0,1) | t | 0 | f | zzp5tr_r2
-- row: 3 | (0,4) | t | 0 | f | zzp5tr_r2
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set, xmax, cmin::text <> '0' AS cmin_set,
       tableoid::regclass::text AS relation FROM zzp5tr_r2 ORDER BY i;

-- ============================================================================
-- A move to another schema, and a rename of the schema itself
-- ============================================================================
CREATE SCHEMA zzp5tr_sc;
ALTER TABLE zzp5tr_r2 SET SCHEMA zzp5tr_sc;

-- begin-expected
-- columns: i | ctid | xmin_set | xmax | cmin_set | relation
-- row: 1 | (0,1) | t | 0 | f | zzp5tr_sc.zzp5tr_r2
-- row: 3 | (0,4) | t | 0 | f | zzp5tr_sc.zzp5tr_r2
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set, xmax, cmin::text <> '0' AS cmin_set,
       tableoid::regclass::text AS relation FROM zzp5tr_sc.zzp5tr_r2 ORDER BY i;

ALTER SCHEMA zzp5tr_sc RENAME TO zzp5tr_sc2;

-- begin-expected
-- columns: i | ctid | xmin_set | xmax | cmin_set | relation
-- row: 1 | (0,1) | t | 0 | f | zzp5tr_sc2.zzp5tr_r2
-- row: 3 | (0,4) | t | 0 | f | zzp5tr_sc2.zzp5tr_r2
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set, xmax, cmin::text <> '0' AS cmin_set,
       tableoid::regclass::text AS relation FROM zzp5tr_sc2.zzp5tr_r2 ORDER BY i;

-- and the relation goes on numbering from where it left off
INSERT INTO zzp5tr_sc2.zzp5tr_r2 VALUES (4,'d');

-- begin-expected
-- columns: i | ctid | xmin_set
-- row: 1 | (0,1) | t
-- row: 3 | (0,4) | t
-- row: 4 | (0,5) | t
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set FROM zzp5tr_sc2.zzp5tr_r2 ORDER BY i;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(DISTINCT ctid) FROM zzp5tr_sc2.zzp5tr_r2;

-- begin-expected
-- columns: i
-- row: 3
-- end-expected
SELECT i FROM zzp5tr_sc2.zzp5tr_r2 WHERE ctid = '(0,4)';

DROP SCHEMA zzp5tr_sc2 CASCADE;

-- ============================================================================
-- A rename inside the transaction that wrote the rows keeps the command
-- identifier each row was written under
-- ============================================================================
BEGIN;
CREATE TABLE zzp5tr_q (i int);
INSERT INTO zzp5tr_q VALUES (1);
INSERT INTO zzp5tr_q VALUES (2);
ALTER TABLE zzp5tr_q RENAME TO zzp5tr_q2;

-- begin-expected
-- columns: i | ctid | xmin_set | cmin
-- row: 1 | (0,1) | t | 1
-- row: 2 | (0,2) | t | 2
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set, cmin FROM zzp5tr_q2 ORDER BY i;

COMMIT;

-- begin-expected
-- columns: i | ctid | xmin_set
-- row: 1 | (0,1) | t
-- row: 2 | (0,2) | t
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set FROM zzp5tr_q2 ORDER BY i;

DROP TABLE zzp5tr_q2;

-- ============================================================================
-- A rename that is rolled back, and a name used a second time
-- ============================================================================
CREATE TABLE zzp5tr_p (i int);
INSERT INTO zzp5tr_p VALUES (1),(2),(3);
DELETE FROM zzp5tr_p WHERE i = 1;

BEGIN;
ALTER TABLE zzp5tr_p RENAME TO zzp5tr_p2;

-- begin-expected
-- columns: i | ctid | xmin_set
-- row: 2 | (0,2) | t
-- row: 3 | (0,3) | t
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set FROM zzp5tr_p2 ORDER BY i;

ROLLBACK;

-- the rows answer under the old name again, saying exactly what they said
-- begin-expected
-- columns: i | ctid | xmin_set
-- row: 2 | (0,2) | t
-- row: 3 | (0,3) | t
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set FROM zzp5tr_p ORDER BY i;

-- a relation made again under a name that has been used before starts clean
DROP TABLE zzp5tr_p;
CREATE TABLE zzp5tr_p (i int);
INSERT INTO zzp5tr_p VALUES (7),(8);

-- begin-expected
-- columns: i | ctid | xmin_set
-- row: 7 | (0,1) | t
-- row: 8 | (0,2) | t
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set FROM zzp5tr_p ORDER BY i;

-- ============================================================================
-- A renamed inheritance child, read through its parent and by its own name
-- ============================================================================
CREATE TABLE zzp5tr_kid (j int) INHERITS (zzp5tr_p);
INSERT INTO zzp5tr_kid VALUES (9, 90);
ALTER TABLE zzp5tr_kid RENAME TO zzp5tr_kid2;

-- begin-expected
-- columns: i | ctid | xmin_set | relation
-- row: 7 | (0,1) | t | zzp5tr_p
-- row: 8 | (0,2) | t | zzp5tr_p
-- row: 9 | (0,1) | t | zzp5tr_kid2
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set, tableoid::regclass::text AS relation
FROM zzp5tr_p ORDER BY i;

-- begin-expected
-- columns: i | ctid | xmin_set
-- row: 9 | (0,1) | t
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set FROM zzp5tr_kid2 ORDER BY i;

DROP TABLE zzp5tr_kid2;
DROP TABLE zzp5tr_p;

-- ============================================================================
-- A snapshot taken before the rename goes on answering with the places it had
-- ============================================================================
CREATE TABLE zzp5tr_rr (i int, s text);
INSERT INTO zzp5tr_rr VALUES (1,'a'),(2,'b'),(3,'c');
UPDATE zzp5tr_rr SET s = 'bb' WHERE i = 2;

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: i | ctid | xmin_set
-- row: 1 | (0,1) | t
-- row: 2 | (0,4) | t
-- row: 3 | (0,3) | t
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set FROM zzp5tr_rr ORDER BY i;

ALTER TABLE zzp5tr_rr RENAME TO zzp5tr_rr2;

-- begin-expected
-- columns: i | ctid | xmin_set
-- row: 1 | (0,1) | t
-- row: 2 | (0,4) | t
-- row: 3 | (0,3) | t
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set FROM zzp5tr_rr2 ORDER BY i;

COMMIT;

-- begin-expected
-- columns: i | ctid | xmin_set
-- row: 1 | (0,1) | t
-- row: 2 | (0,4) | t
-- row: 3 | (0,3) | t
-- end-expected
SELECT i, ctid, xmin::text <> '0' AS xmin_set FROM zzp5tr_rr2 ORDER BY i;

DROP TABLE zzp5tr_rr2;
