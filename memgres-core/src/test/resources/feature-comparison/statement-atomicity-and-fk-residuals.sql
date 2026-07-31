-- ============================================================================
-- Feature Comparison: a statement is atomic, and SET DEFAULT keeps the key
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Outside an explicit transaction every statement is a transaction of its own,
-- so a statement that fails partway must leave nothing behind. memgres recorded
-- undo only inside a transaction, so a multi-row UPDATE that divided by zero on
-- its third row kept the first two, and a query whose data-modifying WITH item
-- had already written kept the write even though the statement was refused.
--
-- The referential residuals are from the same wave. ON DELETE SET DEFAULT was
-- taught to check the key it writes, but the check refused a self-referential
-- DELETE PostgreSQL performs, never ran for ON UPDATE at all, and did not see a
-- default row that a CASCADE further up was about to remove.
--
-- Note on wording: PostgreSQL names whichever side its referential triggers
-- reach first and that depends on the order the rows come up in -- the same
-- shape gives the child's "is not present" for one arrangement and the parent's
-- "still referenced" for another. Only the SQLSTATE is asserted where the two
-- engines pick different sides; the rows are compared either way.
-- ============================================================================

-- ============================================================================
-- 1. A multi-row UPDATE that fails partway changes nothing
-- ============================================================================
DROP TABLE IF EXISTS sa_t CASCADE;
CREATE TABLE sa_t (id int PRIMARY KEY, v int);
INSERT INTO sa_t VALUES (1,1),(2,2),(3,0),(4,4);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
UPDATE sa_t SET v = 100 / v;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM sa_t WHERE v = 100;

-- begin-expected
-- columns: id, v
-- row: 1, 1
-- row: 2, 2
-- row: 3, 0
-- row: 4, 4
-- end-expected
SELECT id::text AS id, v::text AS v FROM sa_t ORDER BY id;

-- ============================================================================
-- 2. Nor does one that trips a constraint partway
-- ============================================================================
DROP TABLE IF EXISTS sa_c CASCADE;
CREATE TABLE sa_c (id int PRIMARY KEY, v int CHECK (v < 100));
INSERT INTO sa_c VALUES (1,1),(2,2),(3,99);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "sa_c" violates check constraint "sa_c_v_check"
-- end-expected-error
UPDATE sa_c SET v = v + 10;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM sa_c WHERE v > 5;

-- ============================================================================
-- 3. A refused statement applies none of its WITH item's writes
-- ============================================================================
DROP TABLE IF EXISTS sa_log CASCADE;
CREATE TABLE sa_log (id int PRIMARY KEY);

-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
WITH w AS (INSERT INTO sa_log VALUES (1) RETURNING id) SELECT sa_nosuchfn(id) FROM w;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM sa_log;

INSERT INTO sa_log VALUES (1),(2),(3);

-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
WITH w AS (UPDATE sa_log SET id = id + 50 RETURNING id) SELECT sa_nosuchfn(1) FROM w;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM sa_log WHERE id >= 50;

-- begin-expected-error
-- sqlstate: 42883
-- end-expected-error
WITH w AS (DELETE FROM sa_log RETURNING id) SELECT sa_nosuchfn(1) FROM w;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*)::text AS n FROM sa_log;

-- one that succeeds still writes
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
WITH w AS (INSERT INTO sa_log VALUES (7) RETURNING id) SELECT count(*)::text AS n FROM w;

-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*)::text AS n FROM sa_log;

-- ============================================================================
-- 4. A self-referential DELETE that needs no default is performed
-- ============================================================================
DROP TABLE IF EXISTS sa_self CASCADE;
CREATE TABLE sa_self (id int PRIMARY KEY,
                      parent int DEFAULT 9 REFERENCES sa_self(id) ON DELETE SET DEFAULT);
INSERT INTO sa_self VALUES (9,NULL);
INSERT INTO sa_self VALUES (1,9),(2,1);
DELETE FROM sa_self WHERE id = 1;

-- row 2 is itself removed, so nothing is left needing the default
DELETE FROM sa_self WHERE id IN (2, 9);

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM sa_self;

-- ============================================================================
-- 5. ON UPDATE SET DEFAULT checks the key it writes
-- ============================================================================
DROP TABLE IF EXISTS sa_uc CASCADE;
DROP TABLE IF EXISTS sa_up CASCADE;
CREATE TABLE sa_up (id int PRIMARY KEY);
INSERT INTO sa_up VALUES (1),(9);
CREATE TABLE sa_uc (id int PRIMARY KEY,
                    pid int DEFAULT 9 REFERENCES sa_up(id) ON UPDATE SET DEFAULT);
INSERT INTO sa_uc VALUES (10,1);
UPDATE sa_up SET id = 5 WHERE id = 1;

-- begin-expected-error
-- sqlstate: 23503
-- end-expected-error
UPDATE sa_up SET id = id + 100;

-- begin-expected
-- columns: dangling
-- row: 0
-- end-expected
SELECT count(*)::text AS dangling FROM sa_uc c
 WHERE NOT EXISTS (SELECT 1 FROM sa_up p WHERE p.id = c.pid);

-- ============================================================================
-- 6. A default whose row a CASCADE is about to remove is seen
-- ============================================================================
DROP TABLE IF EXISTS sa_gc CASCADE;
DROP TABLE IF EXISTS sa_mid CASCADE;
DROP TABLE IF EXISTS sa_top CASCADE;
CREATE TABLE sa_top (id int PRIMARY KEY);
INSERT INTO sa_top VALUES (1),(9);
CREATE TABLE sa_mid (id int PRIMARY KEY, tid int REFERENCES sa_top(id) ON DELETE CASCADE);
INSERT INTO sa_mid VALUES (100,1),(109,9);
CREATE TABLE sa_gc (id int PRIMARY KEY,
                    mid int DEFAULT 109 REFERENCES sa_mid(id) ON DELETE SET DEFAULT);
INSERT INTO sa_gc VALUES (1000,100);

-- begin-expected-error
-- sqlstate: 23503
-- end-expected-error
DELETE FROM sa_top WHERE id IN (1,9);

-- begin-expected
-- columns: dangling
-- row: 0
-- end-expected
SELECT count(*)::text AS dangling FROM sa_gc g
 WHERE NOT EXISTS (SELECT 1 FROM sa_mid p WHERE p.id = g.mid);

-- nothing was deleted
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM sa_top;

-- ============================================================================
-- 7. The ordinary referential shapes are untouched
-- ============================================================================
DROP TABLE IF EXISTS sa_dc CASCADE;
DROP TABLE IF EXISTS sa_dp CASCADE;
CREATE TABLE sa_dp (id int PRIMARY KEY);
INSERT INTO sa_dp VALUES (1),(2),(9);
CREATE TABLE sa_dc (id int PRIMARY KEY,
                    pid int DEFAULT 9 REFERENCES sa_dp(id) ON DELETE SET DEFAULT);
INSERT INTO sa_dc VALUES (11,2);

DELETE FROM sa_dp WHERE id = 2;

-- begin-expected
-- columns: id, pid
-- row: 11, 9
-- end-expected
SELECT id::text AS id, pid::text AS pid FROM sa_dc ORDER BY id;

DROP TABLE IF EXISTS sa_cc CASCADE;
DROP TABLE IF EXISTS sa_cp CASCADE;
CREATE TABLE sa_cp (id int PRIMARY KEY);
INSERT INTO sa_cp VALUES (1),(2);
CREATE TABLE sa_cc (id int PRIMARY KEY, pid int REFERENCES sa_cp(id) ON DELETE CASCADE);
INSERT INTO sa_cc VALUES (10,1),(11,2);

DELETE FROM sa_cp WHERE id = 1;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM sa_cc;

DROP TABLE IF EXISTS sa_nc CASCADE;
DROP TABLE IF EXISTS sa_np CASCADE;
CREATE TABLE sa_np (id int PRIMARY KEY);
INSERT INTO sa_np VALUES (1),(2);
CREATE TABLE sa_nc (id int PRIMARY KEY, pid int REFERENCES sa_np(id) ON DELETE SET NULL);
INSERT INTO sa_nc VALUES (10,1);

DELETE FROM sa_np;

-- begin-expected
-- columns: id, pid
-- row: 10, NULL
-- end-expected
SELECT id::text AS id, pid::text AS pid FROM sa_nc ORDER BY id;

DROP TABLE IF EXISTS sa_nc CASCADE;
DROP TABLE IF EXISTS sa_np CASCADE;
DROP TABLE IF EXISTS sa_cc CASCADE;
DROP TABLE IF EXISTS sa_cp CASCADE;
DROP TABLE IF EXISTS sa_dc CASCADE;
DROP TABLE IF EXISTS sa_dp CASCADE;
DROP TABLE IF EXISTS sa_gc CASCADE;
DROP TABLE IF EXISTS sa_mid CASCADE;
DROP TABLE IF EXISTS sa_top CASCADE;
DROP TABLE IF EXISTS sa_uc CASCADE;
DROP TABLE IF EXISTS sa_up CASCADE;
DROP TABLE IF EXISTS sa_self CASCADE;
DROP TABLE IF EXISTS sa_log CASCADE;
DROP TABLE IF EXISTS sa_c CASCADE;
DROP TABLE IF EXISTS sa_t CASCADE;
