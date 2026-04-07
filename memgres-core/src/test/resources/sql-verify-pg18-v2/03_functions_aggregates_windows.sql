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

CREATE TABLE ftab(a int, b int, c text);
INSERT INTO ftab VALUES
(1,10,'x'),
(2,20,'y'),
(3,NULL,'x'),
(NULL,40,NULL);

CREATE FUNCTION addxy(x int, y int DEFAULT 100) RETURNS int
LANGUAGE SQL
AS $$ SELECT x + y $$;

CREATE FUNCTION namedemo(a int, b text DEFAULT 'zzz', c boolean DEFAULT false)
RETURNS text
LANGUAGE SQL
AS $$ SELECT a::text || ':' || b || ':' || c::text $$;

-- ordinary and positional calls
SELECT addxy(1,2), addxy(1);
SELECT substring('abcdef' FROM 2 FOR 3);
SELECT overlay('Txxxxas' placing 'hom' from 2 for 4);

-- named and mixed notation
SELECT namedemo(a => 5, b => 'qq', c => true);
SELECT namedemo(c => true, a => 5, b => 'qq');
SELECT namedemo(5, c => true);
SELECT namedemo(5, 'bb');
SELECT addxy(y => 7, x => 8);

-- aggregates
SELECT count(*), count(a), sum(a), avg(a), min(c), max(c) FROM ftab;
SELECT count(*) FILTER (WHERE a IS NOT NULL),
       sum(b) FILTER (WHERE c='x')
FROM ftab;
SELECT string_agg(c, ',' ORDER BY c) FROM ftab;

-- windows
SELECT a, b,
       row_number() OVER (ORDER BY a NULLS LAST),
       rank() OVER (ORDER BY b NULLS LAST),
       sum(b) OVER (ORDER BY a NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM ftab
ORDER BY a NULLS LAST;

SELECT a, b,
       lag(b) OVER (PARTITION BY c ORDER BY a),
       lead(b) OVER (PARTITION BY c ORDER BY a)
FROM ftab
ORDER BY c, a;

-- bad calls / syntax / semantic errors
SELECT addxy();
SELECT addxy(1,2,3);
SELECT addxy(x => 1, x => 2);
SELECT namedemo(b => 'x');
SELECT namedemo(1, a => 2);
SELECT substring('abc' FOR 2 FROM 1);
SELECT count(*) FILTER (a > 1);
SELECT sum(a ORDER BY b) FROM ftab;
SELECT row_number() OVER ();
SELECT sum(a) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM ftab;
SELECT nonexistent_func(1);
SELECT abs('x');
SELECT count(DISTINCT a, b) FROM ftab;
SELECT lag() OVER (ORDER BY a) FROM ftab;
SELECT sum(a) OVER (PARTITION BY ORDER BY a) FROM ftab;
SELECT string_agg(c) FROM ftab;

DROP SCHEMA compat CASCADE;



-- more named/mixed notation, overload-ish and aggregate/window edges
CREATE FUNCTION f_over(a int) RETURNS text LANGUAGE SQL AS $$ SELECT 'int' $$;
CREATE FUNCTION f_over(a text) RETURNS text LANGUAGE SQL AS $$ SELECT 'text' $$;
SELECT f_over(1), f_over('x');
SELECT f_over(NULL);

SELECT namedemo(a => 1);
SELECT namedemo(1, b => 'bbb');
SELECT namedemo(1, DEFAULT, true);
SELECT namedemo(a => 1, c => true);
SELECT addxy(DEFAULT, 5);

SELECT array_agg(a ORDER BY b NULLS LAST) FROM ftab;
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY b) FROM ftab;
SELECT rank(10) WITHIN GROUP (ORDER BY b) FROM ftab;

SELECT sum(b) OVER (PARTITION BY c) FROM ftab;
SELECT sum(b) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM ftab;
SELECT nth_value(b, 2) OVER (ORDER BY a NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM ftab;

SELECT namedemo(DEFAULT => 1);
SELECT namedemo(a := 1, b := 'x');
SELECT addxy(x => 1, 2);
SELECT percentile_disc('x') WITHIN GROUP (ORDER BY b) FROM ftab;
SELECT rank() WITHIN GROUP (ORDER BY b) FROM ftab;
SELECT nth_value(b, 0) OVER (ORDER BY a) FROM ftab;
SELECT sum(b) OVER (ORDER BY a GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM ftab;

