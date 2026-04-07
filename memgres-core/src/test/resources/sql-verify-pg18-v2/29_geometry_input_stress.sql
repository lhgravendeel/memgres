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

-- valid geometric inputs
SELECT point(1,2), '(3,4)'::point;
SELECT lseg(point(0,0), point(1,1));
SELECT box(point(0,0), point(2,2));
SELECT path '[(0,0),(1,1),(2,2)]';
SELECT polygon '((0,0),(1,0),(1,1),(0,1))';
SELECT circle(point(0,0), 5);
SELECT center(circle(point(0,0), 5));
SELECT area(box(point(0,0), point(2,3)));
SELECT point(0,0) <@ box(point(0,0), point(2,2));
SELECT box(point(0,0), point(2,2)) @> point(1,1);
SELECT point(0,0) <-> point(3,4);
SELECT circle(point(0,0), 0);

-- malformed literals
SELECT '((1,2)'::point;
SELECT '(1,2,3)'::point;
SELECT ''::point;
SELECT '[(0,0),(1,1)'::path;
SELECT '((0,0),(1,1)'::polygon;
SELECT '<(0,0),1>'::circle;
SELECT '((0,0),(1,1),(2,2)'::lseg;
SELECT '[(0,0)]'::box;
SELECT 'bad'::line;

-- wrong argument types
SELECT point('x', 'y');
SELECT circle('abc', 1);
SELECT circle(point(0,0), 'r');
SELECT box('a', 'b');
SELECT center('not_a_circle');
SELECT area('not_a_box');
SELECT radius(box(point(0,0), point(1,1)));
SELECT line(point(0,0), point(0,0));
SELECT polygon(ARRAY['x','y']);

-- operator and shape oddities
SELECT point(0,0) <@ polygon '((0,0),(1,0),(1,1),(0,1))';
SELECT path '((0,0),(1,1))' @> point(0,0);
SELECT point(1,1) <@ circle(point(0,0), 0.5);
SELECT point(1,2) + 'abc';
SELECT box(1,2);

DROP SCHEMA compat CASCADE;
