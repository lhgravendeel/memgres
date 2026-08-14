-- ============================================================================
-- What a sub-select in a RETURNING list is allowed to see
--
-- A RETURNING list is answered row by row as the statement writes, but a
-- sub-select standing in it that reads nothing of the row around it is read in
-- the snapshot the statement began with. So every row of an INSERT reports the
-- count the relation held before the statement ran, an UPDATE reports the rows
-- its own assignment has not yet made, and a DELETE reports what it has not yet
-- taken away. The answer is the same for every row, because the sub-select is
-- read once for the statement and not once per row.
--
-- A statement that reports no row at all does not read it: a sequence named in
-- such a sub-select is left exactly where it stood.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE zzt4f_sn (i int, v text);
CREATE SEQUENCE zzt4f_sq;
INSERT INTO zzt4f_sn VALUES (1,'a'),(2,'b');

-- ============================================================================
-- What the rows an INSERT writes report
-- ============================================================================

-- the relation holds two rows, and all three written rows say two
-- begin-expected
-- columns: i | count
-- row: 10 | 2
-- row: 11 | 2
-- row: 12 | 2
-- end-expected
INSERT INTO zzt4f_sn VALUES (10,'x'),(11,'y'),(12,'z') RETURNING i, (SELECT count(*) FROM zzt4f_sn);

-- an INSERT reading a query reports the count and the greatest key from before it
-- begin-expected
-- columns: i | count | max
-- row: 20 | 5 | 12
-- row: 21 | 5 | 12
-- row: 22 | 5 | 12
-- end-expected
INSERT INTO zzt4f_sn SELECT g, 'g' FROM generate_series(20,22) g RETURNING i, (SELECT count(*) FROM zzt4f_sn), (SELECT max(i) FROM zzt4f_sn);

-- ============================================================================
-- What the rows an UPDATE and a DELETE report
-- ============================================================================

-- no row carries the value this UPDATE is assigning yet
-- begin-expected
-- columns: i | count
-- row: 1 | 0
-- row: 2 | 0
-- end-expected
UPDATE zzt4f_sn SET v='u' WHERE i<3 RETURNING i, (SELECT count(*) FROM zzt4f_sn WHERE v='u');

-- and the rows a DELETE takes away are still counted while it reports them
-- begin-expected
-- columns: i | count
-- row: 20 | 8
-- row: 21 | 8
-- row: 22 | 8
-- end-expected
DELETE FROM zzt4f_sn WHERE i>=20 RETURNING i, (SELECT count(*) FROM zzt4f_sn);

-- what the statements above really left behind
-- begin-expected
-- columns: count
-- row: 5
-- end-expected
SELECT count(*) FROM zzt4f_sn;

-- ============================================================================
-- The sub-select is read once for the statement, and only if a row is written
-- ============================================================================

-- begin-expected
-- columns: i | nextval
-- row: 30 | 1
-- end-expected
INSERT INTO zzt4f_sn VALUES (30,'q') RETURNING i, (SELECT nextval('zzt4f_sq'));

-- begin-expected
-- columns: last_value | is_called
-- row: 1 | t
-- end-expected
SELECT last_value, is_called FROM zzt4f_sq;

-- a statement that writes no row leaves the sequence exactly where it stood
-- begin-expected
-- columns: i | nextval
-- end-expected
UPDATE zzt4f_sn SET v='w' WHERE false RETURNING i, (SELECT nextval('zzt4f_sq'));

-- begin-expected
-- columns: last_value | is_called
-- row: 1 | t
-- end-expected
SELECT last_value, is_called FROM zzt4f_sq;

-- begin-expected
-- columns: i | nextval
-- end-expected
DELETE FROM zzt4f_sn WHERE false RETURNING i, (SELECT nextval('zzt4f_sq'));

-- begin-expected
-- columns: last_value | is_called
-- row: 1 | t
-- end-expected
SELECT last_value, is_called FROM zzt4f_sq;

-- two rows written move it on once, and both report the one value
-- begin-expected
-- columns: i | nextval
-- row: 1 | 2
-- row: 2 | 2
-- end-expected
UPDATE zzt4f_sn SET v='e' WHERE i<3 RETURNING i, (SELECT nextval('zzt4f_sq'));

-- begin-expected
-- columns: last_value | is_called
-- row: 2 | t
-- end-expected
SELECT last_value, is_called FROM zzt4f_sq;

-- ============================================================================
-- What the rule does not reach: a sub-select that reads the row around it
-- ============================================================================

-- begin-expected
-- columns: i | v | ?column?
-- row: 40 | m | 41
-- end-expected
INSERT INTO zzt4f_sn VALUES (40,'m') RETURNING i, v, (SELECT i + 1);

-- and a plain expression over the row is answered from the row that was written
-- begin-expected
-- columns: i | v
-- row: 40 | M
-- end-expected
UPDATE zzt4f_sn SET v=upper(v) WHERE i=40 RETURNING i, v;

-- begin-expected
-- columns: i | v
-- row: 1, e
-- row: 2, e
-- row: 10, x
-- row: 11, y
-- row: 12, z
-- row: 30, q
-- row: 40, M
-- end-expected
SELECT i, v FROM zzt4f_sn ORDER BY i;

-- teardown
DROP TABLE zzt4f_sn;
DROP SEQUENCE zzt4f_sq;
