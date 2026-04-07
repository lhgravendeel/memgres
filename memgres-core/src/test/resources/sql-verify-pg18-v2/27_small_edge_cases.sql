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

-- object names, quoting, and simple built-in invocation edges
CREATE TABLE " spaced name " ("weird-col!" int, """quoted""" text);
INSERT INTO " spaced name " ("weird-col!", """quoted""") VALUES (1, 'x');
SELECT "weird-col!", """quoted""" FROM " spaced name ";
DROP TABLE " spaced name ";

SELECT abs(-5), lower('ABC'), repeat('x', 3), substring('abcdef' FROM 2 FOR 3);
SELECT repeat(NULL, 3), repeat('x', NULL), substring(NULL FROM 1 FOR 2);
SELECT concat_ws(',', VARIADIC ARRAY[]::text[]);
SELECT format('');
SELECT string_to_array('', ',');
SELECT array_to_string(ARRAY[]::text[], ',');
SELECT jsonb_build_array();
SELECT jsonb_build_object();

-- simple bad built-in function calls
SELECT abs();
SELECT abs(1,2);
SELECT substring('abc');
SELECT overlay('abc' placing 'x');
SELECT lower();
SELECT lower(123);
SELECT abs('x');
SELECT repeat('x', 'y');
SELECT date_part('year', 123);
SELECT array_length('not_array', 1);
SELECT range_merge('x', 'y');
SELECT convert_to('abc', 'NO_SUCH_ENCODING');
SELECT convert_from(E'\\xFF'::bytea, 'UTF8');
SELECT chr(-1);
SELECT chr(1114112);
SELECT decode('xyz', 'hex');
SELECT jsonb_set(NULL, '{a}', '1'::jsonb);

-- empty inputs / odd but useful cases
SELECT xmlforest();
SELECT format('%s', NULL);
SELECT quote_nullable(NULL);
SELECT quote_ident('Mixed Name');
SELECT quote_literal(E'x\ny');
SELECT ascii('A'), chr(65);

DROP SCHEMA compat CASCADE;
