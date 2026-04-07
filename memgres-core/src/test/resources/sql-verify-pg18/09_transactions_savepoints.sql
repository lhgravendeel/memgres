\echo '=== 09_transactions_savepoints.sql ==='
\set VERBOSITY verbose
\set SHOW_CONTEXT never
\set ON_ERROR_STOP off
SET search_path = pg_catalog, public;
DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;

CREATE TABLE tx_t(id int PRIMARY KEY, note text);
INSERT INTO tx_t VALUES (1, 'one');

BEGIN;
INSERT INTO tx_t VALUES (2, 'two');
SELECT * FROM tx_t ORDER BY id;
COMMIT;

BEGIN;
INSERT INTO tx_t VALUES (1, 'dup-in-tx');
SELECT 'should fail because tx is aborted after previous error' AS msg;
ROLLBACK;

SELECT * FROM tx_t ORDER BY id;

BEGIN;
SAVEPOINT s1;
INSERT INTO tx_t VALUES (3, 'three');
SAVEPOINT s2;
INSERT INTO tx_t VALUES (1, 'dup-again');
ROLLBACK TO s2;
INSERT INTO tx_t VALUES (4, 'four');
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
COMMIT;

SELECT * FROM tx_t ORDER BY id;

BEGIN;
CREATE TABLE tx_ddl(a int);
INSERT INTO tx_ddl VALUES (1);
ROLLBACK;
SELECT * FROM tx_ddl;

-- transaction syntax/state errors
COMMIT;
ROLLBACK;
BEGIN;
BEGIN;
SAVEPOINT x;
SAVEPOINT x;
RELEASE SAVEPOINT no_such_savepoint;
ROLLBACK TO SAVEPOINT no_such_savepoint;
ROLLBACK;

DROP SCHEMA compat CASCADE;
