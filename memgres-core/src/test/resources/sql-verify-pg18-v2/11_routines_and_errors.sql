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

CREATE FUNCTION f_sql_add(a int, b int) RETURNS int
LANGUAGE SQL
AS $$ SELECT a + b $$;

CREATE OR REPLACE FUNCTION f_pair(a int, b text DEFAULT 'x')
RETURNS TABLE(x int, y text)
LANGUAGE SQL
AS $$ SELECT a, b $$;

CREATE OR REPLACE PROCEDURE p_ins(i int, t text)
LANGUAGE SQL
AS $$ INSERT INTO compat.proc_t VALUES (i, t) $$;

CREATE TABLE proc_t(id int PRIMARY KEY, t text);

SELECT f_sql_add(1,2), pg_typeof(f_sql_add(1,2));
SELECT * FROM f_pair(1, 'ok');
CALL p_ins(1, 'a');
SELECT * FROM proc_t ORDER BY id;

CREATE OR REPLACE FUNCTION f_raise(p_msg text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION USING MESSAGE = p_msg, ERRCODE = 'P1234';
END;
$$;

CREATE OR REPLACE FUNCTION f_handle(x int)
RETURNS int
LANGUAGE plpgsql
AS $$
BEGIN
  BEGIN
    PERFORM 1 / x;
  EXCEPTION
    WHEN division_by_zero THEN
      RETURN -1;
  END;
  RETURN 1;
END;
$$;

SELECT f_handle(1), f_handle(0);
SELECT f_raise('boom');

-- bad routine definitions/calls
SELECT f_sql_add(1);
SELECT f_sql_add('x', 1);
CALL p_ins(1);
CALL no_such_proc();
CREATE FUNCTION badf(a int) RETURNS int LANGUAGE SQL AS $$ SELECT $$;
CREATE OR REPLACE FUNCTION badpl() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$;
DO $$ BEGIN RAISE NOTICE 'hello'; END $$;
DO $$ BEGIN PERFORM no_such_func(); END $$;
CREATE PROCEDURE badp() LANGUAGE SQL AS $$ SELECT 1 $$;
DROP FUNCTION no_such();

DROP SCHEMA compat CASCADE;

