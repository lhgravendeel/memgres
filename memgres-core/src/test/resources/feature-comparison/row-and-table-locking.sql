-- ============================================================================
-- Feature Comparison: row and table locking
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- What a transaction locks has to unwind with the transaction: a savepoint
-- rollback drops the row locks and the SETs made after it, a REPEATABLE READ
-- transaction still sees its own DDL and TRUNCATE, and TRUNCATE CASCADE runs
-- the cascaded children as full truncations, triggers and all.
-- ============================================================================

DROP TABLE IF EXISTS lk_gk CASCADE;
DROP TABLE IF EXISTS lk_kid CASCADE;
DROP TABLE IF EXISTS lk_par CASCADE;
DROP TABLE IF EXISTS lk_log CASCADE;
DROP TABLE IF EXISTS lk_a CASCADE;
DROP TABLE IF EXISTS lk_b CASCADE;

CREATE TABLE lk_a (id int PRIMARY KEY, v text);
CREATE TABLE lk_b (id int PRIMARY KEY, a int, v text);
INSERT INTO lk_a VALUES (1,'a1'),(2,'a2');
INSERT INTO lk_b VALUES (10,1,'b1'),(11,2,'b2');

-- ============================================================================
-- 1. FOR UPDATE over a join returns rows, with and without OF
-- ============================================================================

-- begin-expected
-- columns: aid | bid
-- row: 1, 10
-- end-expected
SELECT a.id AS aid, b.id AS bid FROM lk_a a JOIN lk_b b ON b.a = a.id
  WHERE a.id = 1 FOR UPDATE OF a;

-- begin-expected
-- columns: aid | bid
-- row: 2, 11
-- end-expected
SELECT a.id AS aid, b.id AS bid FROM lk_a a JOIN lk_b b ON b.a = a.id
  WHERE a.id = 2 FOR UPDATE;

-- A relation named in FOR UPDATE OF has to be in the FROM clause
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: in FOR UPDATE clause not found in FROM clause
-- end-expected-error
SELECT a.id FROM lk_a a JOIN lk_b b ON b.a = a.id FOR UPDATE OF nosuch;

-- ============================================================================
-- 2. ROLLBACK TO SAVEPOINT reverts settings made after the savepoint
-- ============================================================================

BEGIN;
SET LOCAL work_mem = '5MB';

-- begin-expected
-- columns: work_mem
-- row: 5MB
-- end-expected
SHOW work_mem;

SAVEPOINT sp1;
SET LOCAL work_mem = '9MB';

-- begin-expected
-- columns: work_mem
-- row: 9MB
-- end-expected
SHOW work_mem;

ROLLBACK TO SAVEPOINT sp1;

-- begin-expected
-- columns: work_mem
-- row: 5MB
-- end-expected
SHOW work_mem;

COMMIT;

-- ============================================================================
-- 3. A REPEATABLE READ transaction sees its own DDL
-- ============================================================================

CREATE TABLE lk_ddl (id int PRIMARY KEY);
INSERT INTO lk_ddl VALUES (1),(2);

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM lk_ddl;

ALTER TABLE lk_ddl ADD COLUMN extra text DEFAULT 'x';

-- begin-expected
-- columns: id | extra
-- row: 1, x
-- row: 2, x
-- end-expected
SELECT id, extra FROM lk_ddl ORDER BY id;

COMMIT;

-- ============================================================================
-- 4. TRUNCATE CASCADE inside a REPEATABLE READ transaction
-- ============================================================================

CREATE TABLE lk_par (id int PRIMARY KEY);
CREATE TABLE lk_kid (id int PRIMARY KEY, p int REFERENCES lk_par(id));
CREATE TABLE lk_gk (id int PRIMARY KEY, k int REFERENCES lk_kid(id));
INSERT INTO lk_par VALUES (1),(2);
INSERT INTO lk_kid VALUES (10,1),(11,2);
INSERT INTO lk_gk VALUES (100,10);

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM lk_kid;

TRUNCATE lk_par CASCADE;

-- begin-expected
-- columns: par | kid | gk
-- row: 0, 0, 0
-- end-expected
SELECT (SELECT count(*) FROM lk_par) AS par,
       (SELECT count(*) FROM lk_kid) AS kid,
       (SELECT count(*) FROM lk_gk) AS gk;

ROLLBACK;

-- Rollback restores every cascaded table
-- begin-expected
-- columns: par | kid | gk
-- row: 2, 2, 1
-- end-expected
SELECT (SELECT count(*) FROM lk_par) AS par,
       (SELECT count(*) FROM lk_kid) AS kid,
       (SELECT count(*) FROM lk_gk) AS gk;

-- ============================================================================
-- 5. A cascaded TRUNCATE fires the child's statement triggers
-- ============================================================================

CREATE TABLE lk_log (msg text);
CREATE FUNCTION lk_log_fn() RETURNS trigger AS $$
BEGIN INSERT INTO lk_log VALUES (TG_TABLE_NAME); RETURN NULL; END $$ LANGUAGE plpgsql;
CREATE TRIGGER lk_t1 BEFORE TRUNCATE ON lk_par FOR EACH STATEMENT EXECUTE FUNCTION lk_log_fn();
CREATE TRIGGER lk_t2 BEFORE TRUNCATE ON lk_kid FOR EACH STATEMENT EXECUTE FUNCTION lk_log_fn();

TRUNCATE lk_par CASCADE;

-- begin-expected
-- columns: msg
-- row: lk_kid
-- row: lk_par
-- end-expected
SELECT msg FROM lk_log ORDER BY msg;

DROP TABLE lk_gk;
DROP TABLE lk_kid;
DROP TABLE lk_par;
DROP TABLE lk_log;
DROP FUNCTION lk_log_fn();
DROP TABLE lk_ddl;
DROP TABLE lk_a;
DROP TABLE lk_b;
