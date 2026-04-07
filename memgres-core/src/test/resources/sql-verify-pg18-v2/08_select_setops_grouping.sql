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

CREATE TABLE s(a int, b text, c int);
INSERT INTO s VALUES
(1,'x',10),
(2,'x',20),
(3,'y',30),
(NULL,'y',NULL);

SELECT * FROM s ORDER BY a NULLS LAST;
SELECT DISTINCT b FROM s ORDER BY b;
SELECT b, count(*), sum(c) FROM s GROUP BY b ORDER BY b;
SELECT b, count(*) FROM s GROUP BY b HAVING count(*) > 1 ORDER BY b;
SELECT a, sum(c) OVER (ORDER BY a NULLS LAST) FROM s ORDER BY a NULLS LAST;

SELECT a FROM s
UNION
SELECT c FROM s
ORDER BY 1 NULLS LAST;

SELECT a FROM s
INTERSECT
SELECT c FROM s
ORDER BY 1 NULLS LAST;

SELECT a FROM s
EXCEPT
SELECT c FROM s
ORDER BY 1 NULLS LAST;

-- bad / edge grouping
SELECT a, count(*) FROM s;
SELECT b, a FROM s GROUP BY b;
SELECT * FROM s ORDER BY 99;
SELECT * FROM s LIMIT -1;
SELECT * FROM s OFFSET -5;
SELECT a FROM s UNION SELECT a, b FROM s;
SELECT sum(b) FROM s;
SELECT count(*) OVER () FROM s GROUP BY b;
SELECT a, sum(c) FROM s GROUP BY GROUPING SETS ((a), ());
SELECT grouping(a) FROM s;
SELECT DISTINCT ON () * FROM s;
SELECT * FROM s FETCH FIRST WITH TIES;

DROP SCHEMA compat CASCADE;



-- deeper grouping/setop semantics
SELECT DISTINCT ON (b) a, b, c FROM s ORDER BY b, c DESC;
SELECT b, a, sum(c) FROM s GROUP BY ROLLUP (b, a) ORDER BY b, a;
SELECT b, a, sum(c) FROM s GROUP BY CUBE (b, a) ORDER BY b, a;
SELECT b, a, sum(c), grouping(b), grouping(a)
FROM s
GROUP BY GROUPING SETS ((b, a), (b), ())
ORDER BY b, a;

SELECT * FROM (VALUES (1,'x'), (2,'y')) AS v(a,b) ORDER BY 1;
TABLE s;

(SELECT a FROM s WHERE a IS NOT NULL)
UNION
(SELECT c FROM s WHERE c IS NOT NULL)
ORDER BY 1;

SELECT a AS x FROM s ORDER BY x NULLS LAST;
SELECT b, count(*) FROM s GROUP BY 1 ORDER BY 1;

-- more bad cases
SELECT DISTINCT ON (b) a, b FROM s ORDER BY a;
SELECT b, sum(c) FROM s GROUP BY ROLLUP ();
SELECT * FROM (VALUES (1), (2)) AS v(a) ORDER BY 2;
TABLE no_such;
SELECT b AS x, count(*) FROM s GROUP BY x;

