-- ============================================================================
-- -- A write is judged by what it writes, not by the shape of the statement.
-- --
-- -- A view's WITH CHECK OPTION is the view's: a write that reached it through a join is still a
-- -- write through the view, and so is one a MERGE made. A MERGE resolves the whole statement
-- -- before it reads a row, so a clause no row reaches still has to name things that exist. Its
-- -- target may be written with ONLY or with a star, and its INSERT takes the OVERRIDING clause
-- -- that lets a plain INSERT write an always-generated column. And a conflict clause updates the
-- -- row where it found it: a write that would put the row in another partition is refused, not
-- -- moved, because there is no route from the arbiter to another partition.
--
-- ============================================================================

-- ============================================================================
-- 1. A view's check option is the view's, whatever shape the write has
-- ============================================================================
CREATE TABLE zzv_base (id int, n int);
INSERT INTO zzv_base VALUES (1, 5);
CREATE VIEW zzv_vw AS SELECT id, n FROM zzv_base WHERE n < 10 WITH CHECK OPTION;
CREATE TABLE zzv_src (k int, nv int);
INSERT INTO zzv_src VALUES (1, 500);
-- begin-expected-error
-- sqlstate: 44000
-- message-like: new row violates check option for view "zzv_vw"
-- end-expected-error
UPDATE zzv_vw SET n = 99;
-- begin-expected-error
-- sqlstate: 44000
-- message-like: new row violates check option for view "zzv_vw"
-- end-expected-error
UPDATE zzv_vw v SET n = s.nv FROM zzv_src s WHERE v.id = s.k;
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT n AS a FROM zzv_base;
CREATE TABLE zzv_ga (i int PRIMARY KEY, v int);
INSERT INTO zzv_ga VALUES (1, 1);
CREATE VIEW zzv_gav AS SELECT i, v FROM zzv_ga WHERE v < 10 WITH CHECK OPTION;
-- begin-expected-error
-- sqlstate: 44000
-- message-like: new row violates check option for view "zzv_gav"
-- end-expected-error
MERGE INTO zzv_gav t USING (VALUES (1, 90)) s(i, v) ON t.i = s.i WHEN MATCHED THEN UPDATE SET v = s.v;
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT v AS a FROM zzv_ga;
-- ============================================================================
-- 2. A MERGE resolves every clause before it reads a row
-- ============================================================================
CREATE TABLE zzv_g9 (id int, v int);
INSERT INTO zzv_g9 VALUES (1, 1);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
MERGE INTO zzv_g9 t USING (VALUES (2, 2)) s(id, v) ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = nosuchcol;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
MERGE INTO zzv_g9 t USING (VALUES (2, 2)) s(id, v) ON t.id = s.id WHEN MATCHED AND nosuchcol = 1 THEN UPDATE SET v = s.v;
MERGE INTO zzv_g9 t USING (VALUES (1, 7)) s(id, v) ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v;
-- begin-expected
-- columns: a
-- row: 7
-- end-expected
SELECT v AS a FROM zzv_g9;
-- ============================================================================
-- 3. A MERGE target may be written ONLY, or with a star, and may override
-- ============================================================================
CREATE TABLE zzv_g5 (id int, v int);
INSERT INTO zzv_g5 VALUES (1, 1);
-- begin-expected
-- columns: id | v
-- row: 1 | 2
-- end-expected
MERGE INTO ONLY zzv_g5 t USING (VALUES (1, 2)) s(id, v) ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v RETURNING t.id, t.v;
-- begin-expected
-- columns: v
-- row: 3
-- end-expected
MERGE INTO zzv_g5 * t USING (VALUES (1, 3)) s(id, v) ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v RETURNING t.v;
CREATE TABLE zzv_g6 (i int GENERATED ALWAYS AS IDENTITY, v int);
-- begin-expected-error
-- sqlstate: 428C9
-- message-like: cannot insert a non-DEFAULT value into column "i"
-- end-expected-error
MERGE INTO zzv_g6 t USING (VALUES (5, 50)) s(i, v) ON t.i = s.i WHEN NOT MATCHED THEN INSERT (i, v) VALUES (s.i, s.v);
-- begin-expected
-- columns: v
-- row: 50
-- end-expected
MERGE INTO zzv_g6 t USING (VALUES (5, 50)) s(i, v) ON t.i = s.i WHEN NOT MATCHED THEN INSERT (i, v) OVERRIDING SYSTEM VALUE VALUES (s.i, s.v) RETURNING t.v;
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT i AS a FROM zzv_g6;
-- ============================================================================
-- 4. merge_action() belongs to a MERGE's RETURNING list
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42601
-- message-like: MERGE_ACTION() can only be used in the RETURNING list of a MERGE command
-- end-expected-error
SELECT merge_action();
-- begin-expected-error
-- sqlstate: 42601
-- message-like: MERGE_ACTION() can only be used in the RETURNING list of a MERGE command
-- end-expected-error
SELECT 1 AS a WHERE merge_action() = 'UPDATE';
-- ============================================================================
-- 5. A conflict clause updates the row where it found it
-- ============================================================================
CREATE TABLE zzv_p3 (i int PRIMARY KEY, s text) PARTITION BY RANGE (i);
CREATE TABLE zzv_p3a PARTITION OF zzv_p3 FOR VALUES FROM (1) TO (10);
CREATE TABLE zzv_p3b PARTITION OF zzv_p3 FOR VALUES FROM (10) TO (20);
INSERT INTO zzv_p3 VALUES (5, 'a');
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid ON UPDATE specification
-- end-expected-error
INSERT INTO zzv_p3 VALUES (5, 'b') ON CONFLICT (i) DO UPDATE SET i = 17;
INSERT INTO zzv_p3 VALUES (5, 'b') ON CONFLICT (i) DO UPDATE SET s = 'c';
-- begin-expected
-- columns: a
-- row: c
-- end-expected
SELECT s AS a FROM zzv_p3a;
DROP VIEW zzv_vw;
DROP VIEW zzv_gav;
DROP TABLE zzv_base, zzv_src, zzv_ga, zzv_g9, zzv_g5, zzv_g6, zzv_p3 CASCADE;
