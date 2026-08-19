-- ============================================================================
-- what a row says about itself is written when it happens
--
-- The system columns a row answers with — xmin, xmax, cmin, cmax and ctid — are recorded
-- as the row is inserted, updated, deleted and locked, and each says what PostgreSQL
-- records at that moment rather than a value fixed once.
--
-- ============================================================================

-- setup
CREATE TABLE zz_mv1 (id int, k int);
INSERT INTO zz_mv1 VALUES (1,1),(2,2),(3,3);
CREATE TABLE zz_mv2 (id int);
CREATE TABLE zz_mv3 (id int);
CREATE TABLE zz_mv4 (id int, k int);
INSERT INTO zz_mv4 VALUES (1,1),(2,2);

-- ============================================================================
-- a row nobody has touched names no second transaction
-- ============================================================================
-- begin-expected
-- columns: xmax
-- row: 0
-- end-expected
SELECT xmax::text FROM zz_mv1 WHERE id=1;
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM zz_mv1 WHERE xmax::text = '0';

-- ============================================================================
-- a locked row names the transaction that locked it
-- ============================================================================
BEGIN;
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM zz_mv1 WHERE id=1 FOR UPDATE;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT xmax::text = txid_current()::text FROM zz_mv1 WHERE id=1;
-- and only the row the lock reached
-- begin-expected
-- columns: xmax
-- row: 0
-- end-expected
SELECT xmax::text FROM zz_mv1 WHERE id=2;
COMMIT;
-- the mark stays after the transaction that made it has finished
-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT xmax::text = '0' FROM zz_mv1 WHERE id=1;

-- ============================================================================
-- every lock mode writes it, not only the strongest
-- ============================================================================
BEGIN;
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
SELECT id FROM zz_mv1 WHERE id=2 FOR NO KEY UPDATE;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT xmax::text = txid_current()::text FROM zz_mv1 WHERE id=2;
ROLLBACK;
-- and a rolled-back lock leaves its mark, as a rolled-back delete does
-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT xmax::text = '0' FROM zz_mv1 WHERE id=2;
BEGIN;
-- begin-expected
-- columns: id
-- row: 3
-- end-expected
SELECT id FROM zz_mv1 WHERE id=3 FOR SHARE;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT xmax::text = txid_current()::text FROM zz_mv1 WHERE id=3;
COMMIT;
BEGIN;
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM zz_mv4 WHERE id=1 FOR KEY SHARE;
-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT xmax::text = txid_current()::text FROM zz_mv4 WHERE id=1;
COMMIT;

-- ============================================================================
-- a lock that reaches no row writes nothing
-- ============================================================================
BEGIN;
-- begin-expected
-- columns: id
-- end-expected
SELECT id FROM zz_mv4 WHERE id=99 FOR UPDATE;
-- begin-expected
-- columns: xmax
-- row: 0
-- end-expected
SELECT xmax::text FROM zz_mv4 WHERE id=2;
COMMIT;

-- ============================================================================
-- an update writes a new version, which nobody has locked yet
-- ============================================================================
BEGIN;
UPDATE zz_mv4 SET k = k + 1 WHERE id=2;
-- begin-expected
-- columns: xmax
-- row: 0
-- end-expected
SELECT xmax::text FROM zz_mv4 WHERE id=2;
COMMIT;

-- ============================================================================
-- a command counter advances within a transaction
-- ============================================================================
BEGIN;
INSERT INTO zz_mv2 VALUES (10);
INSERT INTO zz_mv2 VALUES (11);
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(DISTINCT cmin::text) FROM zz_mv2 WHERE id IN (10,11);
COMMIT;
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(DISTINCT cmin::text) FROM zz_mv2 WHERE id IN (10,11);

-- ============================================================================
-- a line pointer belongs to the relation, and starts again where the relation does
-- ============================================================================
INSERT INTO zz_mv3 VALUES (1),(2),(3);
-- begin-expected
-- columns: id | ctid
-- row: 1 | (0,1)
-- row: 2 | (0,2)
-- row: 3 | (0,3)
-- end-expected
SELECT id, ctid::text FROM zz_mv3 ORDER BY id;
TRUNCATE zz_mv3;
INSERT INTO zz_mv3 VALUES (7),(8);
-- begin-expected
-- columns: id | ctid
-- row: 7 | (0,1)
-- row: 8 | (0,2)
-- end-expected
SELECT id, ctid::text FROM zz_mv3 ORDER BY id;


-- teardown
DROP TABLE zz_mv1;
DROP TABLE zz_mv2;
DROP TABLE zz_mv3;
DROP TABLE zz_mv4;
