-- What a set-returning function produces, and where it may be written.
--
-- A set-returning call in a select list does not return a value: it multiplies the row. Several
-- of them in one query run side by side, not one inside the other -- the query has as many rows
-- as the longest of them and the shorter ones read NULL past their end, so
-- generate_series(1,2) + generate_series(10,12) is 11, 13 and NULL. That holds whether the calls
-- are in separate select items or in the same expression, and it holds through every operator,
-- cast and construct the expression is made of. A call among another's arguments is the one
-- exception: it runs first, and the outer call runs once per element it yields.
--
-- Placement follows from that. A clause that produces rows may hold one -- FROM, the select
-- list, GROUP BY. A clause that reads rows already produced may not: WHERE and a JOIN condition
-- decide whether to keep a row, HAVING whether to keep a group, and LIMIT and OFFSET are read
-- once for the whole query. Somewhere that wants one boolean, PostgreSQL names the kind of value
-- rather than the clause: a WHEN condition and the arguments of AND, OR and NOT must not return
-- a set (42804). An aggregate or a window call cannot contain one either, and OVER on a function
-- that is neither a window function nor an aggregate is a clause that function has no use for
-- (42809), not a function that does not exist.
--
-- None of this reaches into a nested query, whose set-returning calls produce that query's rows:
-- WHERE x IN (SELECT generate_series(1,2)) is ordinary SQL.
--
-- ROWS FROM puts several functions side by side as columns, each keeping all of its own columns
-- under their own names, and each may carry the column definition list a record result needs.
-- That list is names with types; a bare alias list only renames what is already there, and the
-- two are not interchangeable -- a record result is not described by one and a signature that
-- already names its columns is contradicted by the other.

-- setup
DROP FUNCTION IF EXISTS srf_rec();
DROP FUNCTION IF EXISTS srf_out();
DROP FUNCTION IF EXISTS srf_tab();
DROP FUNCTION IF EXISTS srf_scalar();
DROP TABLE IF EXISTS srf_t CASCADE;

CREATE TABLE srf_t (x int);
INSERT INTO srf_t VALUES (1), (2);

CREATE FUNCTION srf_rec() RETURNS SETOF record AS $$ SELECT 1, 'a'::text $$ LANGUAGE sql;
CREATE FUNCTION srf_out(OUT x int, OUT y text) RETURNS SETOF record
  AS $$ SELECT 1, 'a'::text $$ LANGUAGE sql;
CREATE FUNCTION srf_tab() RETURNS TABLE(x int, y text) AS $$ SELECT 1, 'a'::text $$ LANGUAGE sql;
CREATE FUNCTION srf_scalar() RETURNS int AS $$ SELECT 7 $$ LANGUAGE sql;

-- 1: two calls in one expression run side by side, to the longest of them

-- begin-expected
-- columns: a
-- row: 11
-- row: 13
-- row: NULL
-- end-expected
SELECT generate_series(1,2) + generate_series(10,12) AS a;

-- begin-expected
-- columns: a
-- row: 11
-- row: 22
-- end-expected
SELECT unnest(ARRAY[1,2]) + unnest(ARRAY[10,20]) AS a;

-- begin-expected
-- columns: a
-- row: 2
-- row: 6
-- row: NULL
-- end-expected
SELECT (generate_series(1,3) + 1) * generate_series(1,2) AS a;

-- begin-expected
-- columns: a
-- row: 13
-- row: 24
-- end-expected
SELECT generate_series(1,2)::text || generate_series(3,4)::text AS a;

-- the same rule across separate select items

-- begin-expected
-- columns: a | b
-- row: 1, 10
-- row: 2, 11
-- row: NULL, 12
-- row: NULL, 13
-- end-expected
SELECT generate_series(1,2) AS a, generate_series(10,13) AS b;

-- begin-expected
-- columns: a | b
-- row: 1, x
-- row: 2, y
-- row: NULL, z
-- end-expected
SELECT unnest(ARRAY[1,2]) AS a, unnest(ARRAY['x','y','z']) AS b;

-- 2: a call among another's arguments runs first, and the outer runs once per element

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT generate_series(generate_series(1,2), 4) AS a;

-- 3: the expansion reaches through whatever the expression is made of

-- begin-expected
-- columns: a
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT generate_series(1,3) + 1 AS a;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT (generate_series(1,2))::text AS a;

-- begin-expected
-- columns: a
-- row: true
-- row: true
-- end-expected
SELECT generate_series(1,2) IN (1,2) AS a;

-- begin-expected
-- columns: a
-- row: false
-- row: false
-- end-expected
SELECT generate_series(1,2) IS NULL AS a;

-- begin-expected
-- columns: a
-- row: {1}
-- row: {2}
-- end-expected
SELECT ARRAY[generate_series(1,2)] AS a;

-- begin-expected
-- columns: a
-- row: -1
-- row: -2
-- end-expected
SELECT -generate_series(1,2) AS a;

-- an empty set leaves no row at all

-- begin-expected
-- columns: a
-- end-expected
SELECT generate_series(1,0) AS a;

-- 4: a clause that reads rows rather than producing them holds no set

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in WHERE
-- end-expected-error
SELECT 1 FROM srf_t WHERE generate_series(1,2) > 0;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in HAVING
-- end-expected-error
SELECT 1 FROM srf_t HAVING generate_series(1,2) > 0;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in LIMIT
-- end-expected-error
SELECT 1 FROM srf_t LIMIT generate_series(1,2);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in OFFSET
-- end-expected-error
SELECT 1 FROM srf_t OFFSET generate_series(1,2);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in JOIN conditions
-- end-expected-error
SELECT 1 FROM srf_t JOIN srf_t u ON generate_series(1,2) > 0;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in JOIN conditions
-- end-expected-error
SELECT 1 FROM srf_t LEFT JOIN srf_t u ON generate_series(1,2) > 0;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in COALESCE
-- end-expected-error
SELECT coalesce(generate_series(1,2), 9) AS a;

-- 5: somewhere one boolean is wanted, the kind of value is what is wrong

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CASE/WHEN must not return a set
-- end-expected-error
SELECT CASE WHEN generate_series(1,3) > 1 THEN 'y' ELSE 'n' END AS a;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of AND must not return a set
-- end-expected-error
SELECT generate_series(1,2) > 0 AND true AS a;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of OR must not return a set
-- end-expected-error
SELECT generate_series(1,2) > 0 OR false AS a;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of NOT must not return a set
-- end-expected-error
SELECT NOT (generate_series(1,2) > 0) AS a;

-- BETWEEN is an AND once written out

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of AND must not return a set
-- end-expected-error
SELECT generate_series(1,2) BETWEEN 1 AND 2 AS a;

-- a set elsewhere in the same CASE is the placement rule again

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in CASE
-- end-expected-error
SELECT CASE generate_series(1,2) WHEN 1 THEN 'x' ELSE 'y' END AS a;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in CASE
-- end-expected-error
SELECT CASE WHEN true THEN generate_series(1,2) ELSE 0 END AS a;

-- 6: an aggregate or a window call reads one value at a time

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: aggregate function calls cannot contain set-returning function calls
-- end-expected-error
SELECT array_agg(unnest(ARRAY[1,2]));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: aggregate function calls cannot contain set-returning function calls
-- end-expected-error
SELECT string_agg(generate_series(1,2)::text, ',');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: aggregate function calls cannot contain set-returning function calls
-- end-expected-error
SELECT string_agg((srf_tab()).y, ',');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: window function calls cannot contain set-returning function calls
-- end-expected-error
SELECT count(generate_series(1,2)) OVER () AS a;

-- OVER on a function that is neither is a clause it has no use for

-- begin-expected-error
-- sqlstate: 42809
-- message-like: OVER specified, but generate_series is not a window function nor an aggregate
-- end-expected-error
SELECT generate_series(1,2) OVER ();

-- begin-expected-error
-- sqlstate: 42809
-- message-like: OVER specified, but abs is not a window function nor an aggregate function
-- end-expected-error
SELECT abs(-1) OVER ();

-- and the ordinary shapes around it still work

-- begin-expected
-- columns: sum
-- row: 3
-- row: 3
-- end-expected
SELECT sum(x) OVER () AS sum FROM srf_t;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- end-expected
SELECT row_number() OVER (ORDER BY x) AS n FROM srf_t;

-- begin-expected
-- columns: agg
-- row: {1,2,3}
-- end-expected
SELECT array_agg(g) AS agg FROM generate_series(1,3) g;

-- 7: a nested query's sets are that query's rows, not this one's

-- begin-expected
-- columns: one
-- row: 1
-- row: 1
-- end-expected
SELECT 1 AS one FROM srf_t WHERE x IN (SELECT generate_series(1,2)) ORDER BY x;

-- begin-expected
-- columns: one
-- row: 1
-- row: 1
-- end-expected
SELECT 1 AS one FROM srf_t WHERE EXISTS (SELECT generate_series(1,2)) ORDER BY x;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT (SELECT generate_series(1,1)) AS a;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM (SELECT generate_series(1,3)) s;

-- 8: a grouping key that is a set expands the rows before they are grouped

-- begin-expected
-- columns: one
-- row: 1
-- row: 1
-- end-expected
SELECT 1 AS one FROM (VALUES (1)) v(x) GROUP BY generate_series(1,2);

-- begin-expected
-- columns: n
-- row: 1
-- row: 1
-- end-expected
SELECT count(*) AS n FROM (VALUES (1)) v(x) GROUP BY generate_series(1,2);

-- begin-expected
-- columns: x
-- row: 1
-- row: 1
-- row: 2
-- row: 2
-- end-expected
SELECT x FROM srf_t GROUP BY x, generate_series(1,2) ORDER BY 1;

-- begin-expected
-- columns: one
-- row: 1
-- row: 1
-- row: 1
-- end-expected
SELECT 1 AS one FROM (VALUES (1)) v(x) GROUP BY generate_series(1,2), generate_series(1,3);

-- begin-expected
-- columns: one
-- row: 1
-- row: 1
-- end-expected
SELECT 1 AS one FROM (VALUES (1)) v(x) GROUP BY unnest(ARRAY[1,2]);

-- a grouping key named by ordinal or by output alias is the same key

-- begin-expected
-- columns: g
-- row: 1
-- row: 2
-- end-expected
SELECT generate_series(1,2) AS g FROM (VALUES (1)) v(x) GROUP BY 1;

-- begin-expected
-- columns: g
-- row: 1
-- row: 2
-- end-expected
SELECT generate_series(1,2) AS g FROM (VALUES (1)) v(x) GROUP BY g;

-- an ordinary GROUP BY is unaffected

-- begin-expected
-- columns: x | n
-- row: 1, 1
-- row: 2, 1
-- end-expected
SELECT x, count(*) AS n FROM srf_t GROUP BY x ORDER BY x;

-- 9: ROWS FROM puts the functions side by side, each keeping its own columns

-- begin-expected
-- columns: generate_series
-- row: 1
-- row: 2
-- end-expected
SELECT * FROM ROWS FROM (generate_series(1,2));

-- begin-expected
-- columns: generate_series | key | value
-- row: 1, a, 1
-- row: 2, NULL, NULL
-- end-expected
SELECT * FROM ROWS FROM (generate_series(1,2), json_each('{"a":1}'::json));

-- begin-expected
-- columns: generate_series | generate_series
-- row: 1, 1
-- row: 2, 2
-- row: NULL, 3
-- end-expected
SELECT * FROM ROWS FROM (generate_series(1,2), generate_series(1,3));

-- begin-expected
-- columns: a | b
-- row: 1, 1
-- row: 2, 2
-- row: NULL, 3
-- end-expected
SELECT * FROM ROWS FROM (generate_series(1,2), generate_series(1,3)) AS t(a, b);

-- begin-expected
-- columns: generate_series | ordinality
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT * FROM ROWS FROM (generate_series(1,2)) WITH ORDINALITY;

-- each function may carry the column definition list its own result needs

-- begin-expected
-- columns: x | y
-- row: 1, a
-- end-expected
SELECT * FROM ROWS FROM (srf_rec() AS (x int, y text));

-- begin-expected
-- columns: x | y
-- row: 1, a
-- end-expected
SELECT * FROM ROWS FROM (srf_tab());

-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is redundant for a function with OUT parameters
-- end-expected-error
SELECT * FROM ROWS FROM (srf_tab() AS (x int, y text));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is only allowed for functions returning "record"
-- end-expected-error
SELECT * FROM ROWS FROM (generate_series(1,2) AS (g int));

-- 10: a column definition list describes a record result, and only a record result

-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is required for functions returning "record"
-- end-expected-error
SELECT * FROM srf_rec();

-- a bare alias list renames columns; it does not describe them

-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is required for functions returning "record"
-- end-expected-error
SELECT * FROM srf_rec() AS t(p, q);

-- begin-expected
-- columns: x | y
-- row: 1, a
-- end-expected
SELECT * FROM srf_rec() AS t(x int, y text);

-- begin-expected-error
-- sqlstate: 42P13
-- message-like: return type mismatch in function declared to return record
-- end-expected-error
SELECT * FROM srf_rec() AS t(x int, y int, z int);

-- a signature that names its own columns is not described a second time

-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is redundant for a function with OUT parameters
-- end-expected-error
SELECT * FROM srf_out() AS t(x int, y text);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is redundant for a function with OUT parameters
-- end-expected-error
SELECT * FROM srf_tab() AS t(x int, y text);

-- but it may still be renamed

-- begin-expected
-- columns: p | q
-- row: 1, a
-- end-expected
SELECT * FROM srf_tab() AS t(p, q);

-- begin-expected
-- columns: x | y
-- row: 1, a
-- end-expected
SELECT * FROM srf_out();

-- and a function that returns a named type has nothing a list could add

-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is only allowed for functions returning "record"
-- end-expected-error
SELECT * FROM srf_scalar() AS t(p int);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is only allowed for functions returning "record"
-- end-expected-error
SELECT * FROM generate_series(1,2) AS t(g int);

-- begin-expected
-- columns: g
-- row: 1
-- row: 2
-- end-expected
SELECT * FROM generate_series(1,2) AS t(g);

-- begin-expected
-- columns: p
-- row: 7
-- end-expected
SELECT * FROM srf_scalar() AS t(p);

-- 11: the json_each family is a set-returning function of two columns

-- begin-expected
-- columns: key | value
-- row: a, 1
-- row: b, 2
-- end-expected
SELECT * FROM json_each('{"a":1,"b":2}'::json);

-- begin-expected
-- columns: key | value
-- row: a, 1
-- end-expected
SELECT * FROM jsonb_each_text('{"a":"1"}'::jsonb);

-- begin-expected
-- columns: key | value | ordinality
-- row: a, 1, 1
-- end-expected
SELECT * FROM json_each('{"a":1}'::json) WITH ORDINALITY;

-- begin-expected
-- columns: key
-- row: a
-- row: b
-- end-expected
SELECT (jsonb_each('{"a":1,"b":2}'::jsonb)).key;

-- begin-expected
-- columns: value
-- row: 1
-- end-expected
SELECT (json_each_text('{"a":"1"}'::json)).value;

-- begin-expected
-- columns: jsonb_each
-- row: (a,1)
-- end-expected
SELECT jsonb_each('{"a":1}'::jsonb)::text;

-- teardown
DROP TABLE IF EXISTS srf_t CASCADE;
DROP FUNCTION IF EXISTS srf_rec();
DROP FUNCTION IF EXISTS srf_out();
DROP FUNCTION IF EXISTS srf_tab();
DROP FUNCTION IF EXISTS srf_scalar();
