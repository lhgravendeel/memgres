\echo '=== 10_routines_and_errors.sql ==='
\set VERBOSITY verbose
\set SHOW_CONTEXT never
\set ON_ERROR_STOP off
SET search_path = pg_catalog, public;
DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;

CREATE TABLE log_t(msg text);

CREATE FUNCTION add1(x int)
RETURNS int
LANGUAGE sql
AS $$
    SELECT x + 1
$$;

CREATE FUNCTION concat_pair(a text, b text DEFAULT 'z')
RETURNS text
LANGUAGE sql
AS $$
    SELECT a || ':' || b
$$;

CREATE OR REPLACE FUNCTION fail_if_negative(x int)
RETURNS int
LANGUAGE plpgsql
AS $$
BEGIN
    IF x < 0 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'negative input not allowed';
    END IF;
    RETURN x;
END
$$;

CREATE OR REPLACE PROCEDURE p_insert(m text)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO log_t(msg) VALUES (m);
END
$$;

CREATE OR REPLACE FUNCTION catch_div_zero()
RETURNS text
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM 1 / 0;
    RETURN 'unreachable';
EXCEPTION WHEN division_by_zero THEN
    RETURN 'caught division_by_zero';
END
$$;

SELECT add1(1) AS v, pg_typeof(add1(1)) AS t;
SELECT concat_pair('a') AS v1, concat_pair('a','b') AS v2;
SELECT fail_if_negative(5);
CALL p_insert('hello');
CALL p_insert('world');
SELECT * FROM log_t ORDER BY msg;
SELECT catch_div_zero();

-- routine and call errors
SELECT add1();
SELECT add1(1, 2);
SELECT add1('x');
SELECT no_such_function(1);
SELECT fail_if_negative(-1);
CALL p_insert();
CALL no_such_proc(1);
CALL add1(1);

CREATE FUNCTION bad_sql_fn(x int)
RETURNS int
LANGUAGE sql
AS $$
    SELECT missing_col FROM log_t
$$;
SELECT bad_sql_fn(1);

CREATE OR REPLACE FUNCTION nested_exceptions(x int)
RETURNS text
LANGUAGE plpgsql
AS $$
BEGIN
    BEGIN
        IF x = 0 THEN
            RAISE division_by_zero;
        ELSIF x = 1 THEN
            RAISE unique_violation USING MESSAGE = 'forced unique violation';
        ELSE
            RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'generic plpgsql error';
        END IF;
    EXCEPTION
        WHEN division_by_zero THEN
            RETURN 'caught div0';
        WHEN unique_violation THEN
            RETURN 'caught unique';
    END;
    RETURN 'after inner block';
END
$$;

SELECT nested_exceptions(0);
SELECT nested_exceptions(1);
SELECT nested_exceptions(2);

-- syntax-ish routine errors
CREATE FUNCTION bad_parse_fn(x int)
RETURNS int
LANGUAGE sql
AS $$
    SELECT x +
$$;

DROP SCHEMA compat CASCADE;
