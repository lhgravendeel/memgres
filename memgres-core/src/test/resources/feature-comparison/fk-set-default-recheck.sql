-- ============================================================================
-- Feature Comparison: ON DELETE SET DEFAULT re-checks the key it writes
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- ON DELETE SET DEFAULT puts an ordinary value into the referencing column,
-- and nothing about a column default guarantees the referenced table holds it.
-- The default was already checked against the parent table, but against the
-- parent table as it stood while the action ran -- so a default pointing at a
-- row the same DELETE was about to remove passed the check and left the child
-- referencing a row that no longer existed.
--
-- What is pinned here: that the value the action writes is checked against the
-- parent as it will be once the statement is over, for a single-column and a
-- composite key; and that every neighbouring shape still works -- a default
-- whose parent survives, a NULL default, SET NULL, CASCADE, a parent with no
-- referencing rows, and the NO ACTION an unwritten ON UPDATE means.
-- ============================================================================

-- ============================================================================
-- 1. The default's own parent row goes in the same statement
-- ============================================================================
DROP TABLE IF EXISTS fsd_c CASCADE;
DROP TABLE IF EXISTS fsd_p CASCADE;
CREATE TABLE fsd_p (id int PRIMARY KEY);
INSERT INTO fsd_p VALUES (1),(2);
CREATE TABLE fsd_c (id int PRIMARY KEY,
                    pid int DEFAULT 1 REFERENCES fsd_p(id) ON DELETE SET DEFAULT);
INSERT INTO fsd_c VALUES (10,2);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "fsd_c" violates foreign key constraint "fsd_c_pid_fkey"
-- end-expected-error
DELETE FROM fsd_p;

-- the refused statement leaves both tables as they were
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id::text AS id FROM fsd_p ORDER BY id;

-- begin-expected
-- columns: id, pid
-- row: 10, 2
-- end-expected
SELECT id::text AS id, pid::text AS pid FROM fsd_c ORDER BY id;

-- ============================================================================
-- 2. The default's parent row survives, so the action stands
-- ============================================================================
DROP TABLE IF EXISTS fsd_c CASCADE;
DROP TABLE IF EXISTS fsd_p CASCADE;
CREATE TABLE fsd_p (id int PRIMARY KEY);
INSERT INTO fsd_p VALUES (1),(2);
CREATE TABLE fsd_c (id int PRIMARY KEY,
                    pid int DEFAULT 1 REFERENCES fsd_p(id) ON DELETE SET DEFAULT);
INSERT INTO fsd_c VALUES (10,2);

DELETE FROM fsd_p WHERE id = 2;

-- begin-expected
-- columns: id, pid
-- row: 10, 1
-- end-expected
SELECT id::text AS id, pid::text AS pid FROM fsd_c ORDER BY id;

-- ============================================================================
-- 3. A default no parent row ever held is refused as before
-- ============================================================================
DROP TABLE IF EXISTS fsd_c CASCADE;
DROP TABLE IF EXISTS fsd_p CASCADE;
CREATE TABLE fsd_p (id int PRIMARY KEY);
INSERT INTO fsd_p VALUES (1),(2);
CREATE TABLE fsd_c (id int PRIMARY KEY,
                    pid int DEFAULT 99 REFERENCES fsd_p(id) ON DELETE SET DEFAULT);
INSERT INTO fsd_c VALUES (10,2);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "fsd_c" violates foreign key constraint "fsd_c_pid_fkey"
-- end-expected-error
DELETE FROM fsd_p WHERE id = 2;

-- ============================================================================
-- 4. A NULL default references nothing, so it is always allowed
-- ============================================================================
DROP TABLE IF EXISTS fsd_c CASCADE;
DROP TABLE IF EXISTS fsd_p CASCADE;
CREATE TABLE fsd_p (id int PRIMARY KEY);
INSERT INTO fsd_p VALUES (1),(2);
CREATE TABLE fsd_c (id int PRIMARY KEY,
                    pid int REFERENCES fsd_p(id) ON DELETE SET DEFAULT);
INSERT INTO fsd_c VALUES (10,2);

DELETE FROM fsd_p;

-- begin-expected
-- columns: id, pid
-- row: 10, NULL
-- end-expected
SELECT id::text AS id, pid::text AS pid FROM fsd_c ORDER BY id;

-- ============================================================================
-- 5. SET NULL and CASCADE are untouched by the check
-- ============================================================================
DROP TABLE IF EXISTS fsd_c CASCADE;
DROP TABLE IF EXISTS fsd_p CASCADE;
CREATE TABLE fsd_p (id int PRIMARY KEY);
INSERT INTO fsd_p VALUES (1),(2);
CREATE TABLE fsd_c (id int PRIMARY KEY,
                    pid int DEFAULT 1 REFERENCES fsd_p(id) ON DELETE SET NULL);
INSERT INTO fsd_c VALUES (10,2);

DELETE FROM fsd_p;

-- begin-expected
-- columns: id, pid
-- row: 10, NULL
-- end-expected
SELECT id::text AS id, pid::text AS pid FROM fsd_c ORDER BY id;

DROP TABLE IF EXISTS fsd_c CASCADE;
DROP TABLE IF EXISTS fsd_p CASCADE;
CREATE TABLE fsd_p (id int PRIMARY KEY);
INSERT INTO fsd_p VALUES (1),(2);
CREATE TABLE fsd_c (id int PRIMARY KEY,
                    pid int DEFAULT 1 REFERENCES fsd_p(id) ON DELETE CASCADE);
INSERT INTO fsd_c VALUES (10,2);

DELETE FROM fsd_p;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM fsd_c;

-- ============================================================================
-- 6. A parent with no referencing rows is deleted freely
-- ============================================================================
DROP TABLE IF EXISTS fsd_c CASCADE;
DROP TABLE IF EXISTS fsd_p CASCADE;
CREATE TABLE fsd_p (id int PRIMARY KEY);
INSERT INTO fsd_p VALUES (1),(2);
CREATE TABLE fsd_c (id int PRIMARY KEY,
                    pid int DEFAULT 1 REFERENCES fsd_p(id) ON DELETE SET DEFAULT);

DELETE FROM fsd_p;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM fsd_p;

-- ============================================================================
-- 7. The same question for a composite key
-- ============================================================================
DROP TABLE IF EXISTS fsd_c CASCADE;
DROP TABLE IF EXISTS fsd_p CASCADE;
CREATE TABLE fsd_p (a int, b int, PRIMARY KEY (a,b));
INSERT INTO fsd_p VALUES (1,1),(2,2);
CREATE TABLE fsd_c (id int PRIMARY KEY, x int DEFAULT 1, y int DEFAULT 1,
                    FOREIGN KEY (x,y) REFERENCES fsd_p(a,b) ON DELETE SET DEFAULT);
INSERT INTO fsd_c VALUES (10,2,2);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "fsd_c" violates foreign key constraint "fsd_c_x_y_fkey"
-- end-expected-error
DELETE FROM fsd_p;

-- begin-expected
-- columns: id, x, y
-- row: 10, 2, 2
-- end-expected
SELECT id::text AS id, x::text AS x, y::text AS y FROM fsd_c ORDER BY id;

-- deleting only the row the child does not point at leaves the child alone
DELETE FROM fsd_p WHERE a = 1 AND b = 1;

-- begin-expected
-- columns: id, x, y
-- row: 10, 2, 2
-- end-expected
SELECT id::text AS id, x::text AS x, y::text AS y FROM fsd_c ORDER BY id;

-- ============================================================================
-- 8. ON UPDATE is not written, so it is NO ACTION: moving a referenced key is
--    refused rather than quietly repointing the child at its default
-- ============================================================================
DROP TABLE IF EXISTS fsd_c CASCADE;
DROP TABLE IF EXISTS fsd_p CASCADE;
CREATE TABLE fsd_p (id int PRIMARY KEY);
INSERT INTO fsd_p VALUES (1),(2);
CREATE TABLE fsd_c (id int PRIMARY KEY,
                    pid int DEFAULT 1 REFERENCES fsd_p(id) ON DELETE SET DEFAULT);
INSERT INTO fsd_c VALUES (10,2);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "fsd_p" violates foreign key constraint "fsd_c_pid_fkey" on table "fsd_c"
-- end-expected-error
UPDATE fsd_p SET id = 7 WHERE id = 2;

-- begin-expected
-- columns: id, pid
-- row: 10, 2
-- end-expected
SELECT id::text AS id, pid::text AS pid FROM fsd_c ORDER BY id;

DROP TABLE IF EXISTS fsd_c CASCADE;
DROP TABLE IF EXISTS fsd_p CASCADE;
