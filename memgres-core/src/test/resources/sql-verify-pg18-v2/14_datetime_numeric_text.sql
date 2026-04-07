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

-- date/time and interval edge cases
SELECT DATE '0001-01-01', DATE '0001-12-31 BC';
SELECT TIMESTAMP 'infinity', TIMESTAMP '-infinity';
SELECT TIMESTAMPTZ '2024-03-31 01:30:00+00';
SELECT TIMESTAMP '2024-03-31 01:30:00' AT TIME ZONE 'UTC';
SELECT TIMESTAMPTZ '2024-03-31 01:30:00+00' AT TIME ZONE 'Europe/Amsterdam';
SELECT TIME '12:34:56', TIMETZ '12:34:56+02';
SELECT INTERVAL '1 year 2 mons 3 days 04:05:06';
SELECT justify_days(INTERVAL '35 days'), justify_hours(INTERVAL '49 hours');
SELECT age(TIMESTAMP '2024-03-01', TIMESTAMP '2024-02-28');
SELECT DATE '2024-02-29' + 1, DATE '2024-03-01' - DATE '2024-02-28';
SELECT TIMESTAMP '2024-02-29 12:00' + INTERVAL '1 day 2 hours';
SELECT EXTRACT(epoch FROM TIMESTAMP '1970-01-02 00:00:00');
SELECT isfinite(DATE '2024-01-01'), isfinite(TIMESTAMP 'infinity');

-- bad date/time inputs
SELECT DATE '2024-02-30';
SELECT TIMESTAMP '2024-13-01 00:00:00';
SELECT TIME '24:00:01';
SELECT TIMETZ '10:00 UTC+99';
SELECT TIMESTAMP 'not-a-date';
SELECT INTERVAL 'nonsense';
SELECT TIMESTAMP '2024-03-31 02:30:00' AT TIME ZONE 'NoSuch/Zone';

-- numeric and floating-point edge cases
SELECT 'NaN'::float8, 'Infinity'::float8, '-Infinity'::float8;
SELECT 'NaN'::numeric;
SELECT pg_typeof('NaN'::float8), pg_typeof('NaN'::numeric);
SELECT round(123.456::numeric, 2), trunc(123.456::numeric, 1);
SELECT width_bucket(5.35, 0.024, 10.06, 5);
SELECT 10 % 3, mod(10, 3), power(2, 10);
SELECT 1e308::float8 * 10::float8;
SELECT (99999::numeric(5,0));
SELECT 1::numeric(10,2) / 3::numeric(10,2);
SELECT 'NaN'::float8 = 'NaN'::float8, 'NaN'::float8 > 0::float8;

-- string, bit-string, and binary
SELECT 'abc'::char(5), 'abcdef'::varchar(3);
SELECT B'1010'::bit(4), B'1010'::bit varying(10);
SELECT B'1010' & B'1100', B'1010' | B'1100', B'1010' # B'1100';
SELECT bit_length(B'1010'), octet_length(E'\\xDEADBEEF'::bytea);
SELECT substring('abcdef' FROM 2 FOR 3), overlay('Txxxxas' placing 'hom' from 2 for 4);
SELECT trim(both 'x' from 'xxxtrimxxx'), position('bc' in 'abcd');
SELECT convert_to('abc', 'UTF8'), convert_from(convert_to('abc', 'UTF8'), 'UTF8');
SELECT U&'d\0061t\+000061' AS unicode_string;
SELECT E'\\x41'::bytea, decode('DEADBEEF', 'hex'), encode(E'\\xDEADBEEF'::bytea, 'hex');
SELECT ascii('A'), chr(65);

-- bad text/bit/binary inputs
SELECT B'1020';
SELECT B'101'::bit(4);
SELECT B'1010' & B'11';
SELECT convert_to('abc', 'NO_SUCH_ENCODING');
SELECT convert_from(E'\\xFF'::bytea, 'UTF8');
SELECT chr(-1);
SELECT chr(1114112);
SELECT decode('xyz', 'hex');
SELECT U&'bad\0G';
SELECT substring('abc' FROM 'x');

DROP SCHEMA compat CASCADE;
