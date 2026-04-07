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

CREATE SCHEMA s1;
CREATE SCHEMA s2;

CREATE TABLE s1.t(id int, note text);
CREATE TABLE s2.t(id int, note text);
INSERT INTO s1.t VALUES (1, 's1');
INSERT INTO s2.t VALUES (2, 's2');

SET search_path = s1, s2, pg_catalog;
SELECT * FROM t ORDER BY id;

SET search_path = s2, s1, pg_catalog;
SELECT * FROM t ORDER BY id;

CREATE TEMP TABLE t(id int, note text) ON COMMIT PRESERVE ROWS;
INSERT INTO t VALUES (99, 'temp');
SELECT * FROM t ORDER BY id;

SET search_path = pg_temp, s1, s2, pg_catalog;
SELECT * FROM t ORDER BY id;

CREATE TEMP TABLE tt_drop(a int) ON COMMIT DROP;
BEGIN;
INSERT INTO tt_drop VALUES (1);
COMMIT;
SELECT * FROM tt_drop;

CREATE TEMP TABLE tt_delete(a int) ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO tt_delete VALUES (1);
COMMIT;
SELECT * FROM tt_delete;

-- metadata-ish regclass/regtype helpers
CREATE TABLE compat.meta_t(id int PRIMARY KEY, note text DEFAULT 'x');
SELECT 'compat.meta_t'::regclass;
SELECT 'int4'::regtype;
SELECT to_regclass('compat.meta_t');
SELECT to_regtype('int4');
SELECT to_regclass('compat.missing_table');
SELECT to_regtype('missing_type');

-- bad search_path / regclass / regtype cases
SET search_path = no_such_schema, pg_catalog;
SELECT * FROM t;
SELECT 'compat.missing_table'::regclass;
SELECT 'missing_type'::regtype;

DROP SCHEMA compat CASCADE;
