\pset pager off
\pset format unaligned
\pset tuples_only off
\pset null <NULL>
\set VERBOSITY verbose
\set SHOW_CONTEXT always
\set ON_ERROR_STOP off

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;
SET client_min_messages = notice;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';

SELECT current_schema() AS current_schema,
       current_setting('TimeZone') AS timezone,
       current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle;

CREATE TABLE tx(id int PRIMARY KEY, v text);

BEGIN;
INSERT INTO tx VALUES (1,'a');
INSERT INTO tx VALUES (1,'dup');
SELECT * FROM tx ORDER BY id;
ROLLBACK;

SELECT * FROM tx ORDER BY id;

BEGIN;
INSERT INTO tx VALUES (1,'a');
SAVEPOINT s1;
INSERT INTO tx VALUES (1,'dup');
ROLLBACK TO s1;
INSERT INTO tx VALUES (2,'b');
COMMIT;

SELECT * FROM tx ORDER BY id;

BEGIN;
CREATE TABLE tx2(a int);
INSERT INTO tx2 VALUES (1);
ROLLBACK;
SELECT * FROM tx2;

BEGIN;
INSERT INTO tx VALUES (3,'c');
SELECT 1/0;
SELECT * FROM tx;
COMMIT;
ROLLBACK;

BEGIN;
SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s2;

DROP SCHEMA compat CASCADE;

