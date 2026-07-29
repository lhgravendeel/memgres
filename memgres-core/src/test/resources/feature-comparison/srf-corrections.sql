-- Where a set-returning call may stand, and what it produces when it does.
--
-- A set-returning function answers with rows rather than a value, so it belongs wherever rows are
-- still being produced and nowhere that reads rows already produced. Everything below was measured
-- against PostgreSQL 18 before and after the change.
--
-- 1. Two errors carried a Position PostgreSQL does not send, and two did not carry one it does.
--    The protocol layer guesses a Position by finding the message's quoted name in the statement
--    text. That is right often enough to keep, and wrong for the errors raised while a query's
--    range table is built -- a FROM name given twice, a USING column named twice or missing, an
--    alias list longer than the item -- where PostgreSQL sends no Position at all. Meanwhile the
--    placement refusals, which PostgreSQL points at the call, quote no name and so got nothing.
--    Both are now decided at the throw site: an error either names the word its Position points
--    at, or says it has none. A WITH query's alias-count error does carry one, and is also named
--    "WITH query" rather than "table".
--
-- 2. NULLIF, GREATEST and LEAST are not conditionals. PostgreSQL refuses a set inside CASE and
--    COALESCE because those may skip evaluating an argument, so which rows the query answers with
--    would depend on a value the planner does not have. The other three evaluate every argument
--    and expand the set. Listing them refused valid SQL.
--
-- 3. A set expands after the window and before the sort. A select-list call beside a window
--    function stopped expanding and came back as an array literal -- a wrong answer rather than an
--    error. A call in the window's own PARTITION BY or ORDER BY is a sort key computed below the
--    window and expands the input instead, and so does one written only in the query's ORDER BY.
--
-- 4. An UPDATE assignment and a RETURNING item may not hold one; a one-row VALUES list may, and
--    writes one row per element; two or more VALUES rows are a scan of a constant table with
--    nowhere to expand into, and PostgreSQL refuses that. All three halves measured.
--
-- 5. A function FROM item is a relation, not always a lateral one. Running one row by row on the
--    nullable side of a RIGHT or FULL join dropped every unmatched row on both sides. A lateral
--    reference is illegal there anyway, so the item is an ordinary relation. A padded one answers
--    NULL, not an empty record, and a comma join drops the row it produced nothing for.
--
-- 6. string_to_table and regexp_split_to_table existed only as FROM items; the many-argument
--    unnest exists only as one. A function FROM item's columns carry the type the call answers in
--    and the name PostgreSQL gives them, and so does a sub-query used as a value.
--
-- The last section is ordinary SQL, which has to keep working: the cost of a rule that reaches too
-- far is a refused valid statement.

-- setup
DROP FUNCTION IF EXISTS src_fint();
DROP TABLE IF EXISTS src_t2 CASCADE;
DROP TABLE IF EXISTS src_r3 CASCADE;
DROP TABLE IF EXISTS src_r1 CASCADE;
DROP TABLE IF EXISTS src_emp CASCADE;
DROP TABLE IF EXISTS src_dpt CASCADE;

CREATE TABLE src_dpt (id int PRIMARY KEY, name text, budget int);
INSERT INTO src_dpt VALUES (1,'eng',100),(2,'ops',200),(3,'hr',300);

CREATE TABLE src_emp (id int PRIMARY KEY, name text, dept_id int);
INSERT INTO src_emp VALUES (1,'amy',1),(2,'bob',2),(3,'cal',3),(4,'dan',NULL);

CREATE TABLE src_r1 (a int PRIMARY KEY);
INSERT INTO src_r1 VALUES (1),(2);

CREATE TABLE src_r3 (a int PRIMARY KEY);

CREATE TABLE src_t2 (j int, k text);
INSERT INTO src_t2 VALUES (1,'a'),(2,'b');

CREATE FUNCTION src_fint() RETURNS SETOF int AS $$ SELECT 1 UNION ALL SELECT 2 $$ LANGUAGE sql;

-- ============================================================================
-- 1. Errors PostgreSQL raises with no parse location of their own
-- ============================================================================

-- note: PostgreSQL sends no Position for any of these; the guessed one was the regression
-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "src_dpt" specified more than once
-- end-expected-error
SELECT count(*) FROM src_dpt, src_dpt;

-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "a" specified more than once
-- end-expected-error
SELECT count(*) FROM src_dpt a, src_dpt a;

-- note: a function FROM item names itself after the function when it has no alias
-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "generate_series" specified more than once
-- end-expected-error
SELECT count(*) FROM generate_series(1,2), generate_series(1,3);

-- note: ROWS FROM is spelled internally as a call to a made-up function; unaliased it still
-- note: names itself after the first function written inside it
-- begin-expected-error
-- sqlstate: 42712
-- message-like: table name "generate_series" specified more than once
-- end-expected-error
SELECT count(*) FROM ROWS FROM (generate_series(1,2)), generate_series(1,2);

-- begin-expected-error
-- sqlstate: 42701
-- message-like: column name "id" appears more than once in USING clause
-- end-expected-error
SELECT * FROM src_dpt a JOIN src_dpt b USING (id, id);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" specified in USING clause does not exist in left table
-- end-expected-error
SELECT * FROM src_dpt a JOIN src_emp b USING (nosuch);

-- note: an alias list may not name more columns than the item has. WITH ORDINALITY is what adds
-- note: the second column; inferring it from a second alias invented a column to hang it on
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "t" has 1 columns available but 2 columns specified
-- end-expected-error
SELECT * FROM generate_series(1,2) AS t(v, w);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "t" has 2 columns available but 3 columns specified
-- end-expected-error
SELECT * FROM (SELECT 1, 2) AS t(a, b, c);

-- note: a WITH query is named as one, and this error does carry a Position
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: WITH query "c" has 1 columns available but 2 columns specified
-- end-expected-error
WITH c(a, b) AS (SELECT 1) SELECT * FROM c;

-- ============================================================================
-- 2. The placement refusals, which point at the call
-- ============================================================================

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in LIMIT
-- end-expected-error
SELECT * FROM src_dpt LIMIT generate_series(1,1);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in OFFSET
-- end-expected-error
SELECT * FROM src_dpt OFFSET generate_series(1,1);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in WHERE
-- end-expected-error
SELECT * FROM src_dpt WHERE id = generate_series(1,1);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in HAVING
-- end-expected-error
SELECT count(*) FROM src_dpt HAVING count(*) = generate_series(1,1);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in JOIN conditions
-- end-expected-error
SELECT * FROM src_dpt a JOIN src_emp b ON a.id = generate_series(1,1);

-- note: CASE and COALESCE also carry the hint that a LATERAL FROM item would answer the query;
-- note: LIMIT, OFFSET, WHERE, HAVING and a join condition do not
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in CASE
-- end-expected-error
SELECT CASE WHEN true THEN generate_series(1,2) ELSE 0 END;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in COALESCE
-- end-expected-error
SELECT coalesce(generate_series(1,2), 0);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: aggregate function calls cannot contain set-returning function calls
-- end-expected-error
SELECT count(generate_series(1,2));

-- note: FILTER selects which rows an aggregate accumulates, so only an aggregate may carry one
-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but generate_series is not an aggregate function
-- end-expected-error
SELECT generate_series(1,2) FILTER (WHERE true);

-- note: IN compares one value against a list of values, and a set is neither
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of IN must not return a set
-- end-expected-error
SELECT generate_series(1,2) IN (1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of IN must not return a set
-- end-expected-error
SELECT 1 IN (generate_series(1,2));

-- ============================================================================
-- 3. NULLIF, GREATEST and LEAST evaluate every argument
-- ============================================================================

-- begin-expected
-- columns: nullif
-- row: 1
-- row: 2
-- end-expected
SELECT nullif(generate_series(1,2), 0);

-- note: the operator still applies per row -- the element equal to 1 becomes NULL
-- begin-expected
-- columns: nullif
-- row: NULL
-- row: 2
-- end-expected
SELECT nullif(generate_series(1,2), 1);

-- begin-expected
-- columns: greatest
-- row: 1
-- row: 2
-- end-expected
SELECT greatest(generate_series(1,2), 0);

-- begin-expected
-- columns: least
-- row: 1
-- row: 2
-- end-expected
SELECT least(generate_series(1,2), 5);

-- note: the argument order does not matter; every argument is evaluated either way
-- begin-expected
-- columns: greatest
-- row: 1
-- row: 2
-- end-expected
SELECT greatest(0, generate_series(1,2));

-- ============================================================================
-- 4. Where the expansion happens relative to windows and sorting
-- ============================================================================

-- note: the window is numbered over the input rows, then the set expands and every row it
-- note: produced carries the window value of the row it came from
-- begin-expected
-- columns: g | count
-- row: 1, 1
-- row: 2, 1
-- row: 3, 1
-- end-expected
SELECT generate_series(1,3) g, count(*) OVER () ORDER BY 1;

-- begin-expected
-- columns: g | sum
-- row: 1, 3
-- row: 1, 3
-- row: 2, 3
-- row: 2, 3
-- row: 3, 3
-- row: 3, 3
-- end-expected
SELECT generate_series(1,3) g, sum(a) OVER () FROM src_r1 ORDER BY 1, 2;

-- note: a set in the window's own ORDER BY is a sort key computed below the window, so it
-- note: expands the input and the window is numbered over what the expansion produced
-- begin-expected
-- columns: row_number
-- row: 1
-- row: 2
-- end-expected
SELECT row_number() OVER (ORDER BY generate_series(1,2));

-- begin-expected
-- columns: sum
-- row: 3
-- row: 3
-- row: 6
-- row: 6
-- end-expected
SELECT sum(a) OVER (ORDER BY generate_series(1,2)) FROM src_r1 ORDER BY 1;

-- begin-expected
-- columns: count
-- row: 1
-- row: 1
-- end-expected
SELECT count(*) OVER (PARTITION BY generate_series(1,2));

-- note: a set written only in ORDER BY is an output column the query does not print, and it
-- note: expands the rows the same way one in the select list does
-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM src_r1 ORDER BY generate_series(1,2);

-- note: a sort key that is also a select target is expanded once, by the projection
-- begin-expected
-- columns: g
-- row: 1
-- row: 2
-- end-expected
SELECT generate_series(1,2) g FROM src_r1 WHERE a = 1 ORDER BY g;

-- ============================================================================
-- 5. The statements that write rows
-- ============================================================================

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in UPDATE
-- end-expected-error
UPDATE src_r3 SET a = generate_series(1,2) WHERE a = 5;

-- note: refused for what it says, not for what it would have done: no row matches this WHERE
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in UPDATE
-- end-expected-error
UPDATE src_dpt SET budget = generate_series(1,1) WHERE id = -1;

-- note: ON CONFLICT DO UPDATE assigns to one row, and PostgreSQL names it an UPDATE
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in UPDATE
-- end-expected-error
INSERT INTO src_r1 VALUES (1) ON CONFLICT (a) DO UPDATE SET a = generate_series(1,1);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in RETURNING
-- end-expected-error
INSERT INTO src_r3 VALUES (1) RETURNING generate_series(1,2);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in RETURNING
-- end-expected-error
UPDATE src_r1 SET a = a RETURNING generate_series(1,2);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in RETURNING
-- end-expected-error
DELETE FROM src_r3 RETURNING generate_series(1,2);

-- note: a VALUES list of two or more rows is a scan of a constant table, with nowhere to expand
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in VALUES
-- end-expected-error
INSERT INTO src_r3 VALUES (generate_series(1,2)), (5);

-- note: one VALUES row is projected like a select list, so it writes one row per element
INSERT INTO src_r3 VALUES (generate_series(5,6));

-- begin-expected
-- columns: a
-- row: 5
-- row: 6
-- end-expected
SELECT a FROM src_r3 ORDER BY a;

DELETE FROM src_r3;

-- ============================================================================
-- 6. A function FROM item is a relation
-- ============================================================================

-- begin-expected
-- columns: n | n
-- row: 1, NULL
-- row: 2, 2
-- row: 3, 3
-- row: NULL, 4
-- end-expected
SELECT a.n, b.n FROM generate_series(1,3) a(n)
    FULL JOIN generate_series(2,4) b(n) ON a.n = b.n ORDER BY 1, 2;

-- begin-expected
-- columns: name | x
-- row: amy, 1
-- row: bob, 2
-- row: cal, 3
-- row: dan, NULL
-- end-expected
SELECT e.name, x FROM src_emp e FULL JOIN generate_series(1,3) x ON e.dept_id = x ORDER BY 1, 2;

-- begin-expected
-- columns: n | n
-- row: 2, 2
-- row: 3, 3
-- row: NULL, 4
-- end-expected
SELECT a.n, b.n FROM generate_series(1,3) a(n)
    RIGHT JOIN generate_series(2,4) b(n) ON a.n = b.n ORDER BY 1, 2;

-- note: a FULL JOIN still has to be one the planner could answer, whatever its arms are
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
-- end-expected-error
SELECT count(*) FROM generate_series(1,3) a(x) FULL JOIN generate_series(1,3) b(y) ON a.x < b.y;

-- note: the padded row answers NULL, not the empty record a shapeless placeholder gave
-- begin-expected
-- columns: name | g
-- row: amy, 1
-- row: bob, 1
-- row: bob, 2
-- row: cal, 1
-- row: cal, 2
-- row: cal, 3
-- row: dan, NULL
-- end-expected
SELECT e.name, g FROM src_emp e
    LEFT JOIN LATERAL generate_series(1, e.dept_id) g ON true ORDER BY 1, 2;

-- note: a comma is an inner join, so the row the function produced nothing for goes
-- begin-expected
-- columns: name | g
-- row: amy, 1
-- row: bob, 1
-- row: bob, 2
-- row: cal, 1
-- row: cal, 2
-- row: cal, 3
-- end-expected
SELECT e.name, g FROM src_emp e, LATERAL generate_series(1, e.dept_id) g ORDER BY 1, 2;

-- ============================================================================
-- 7. The calls that only worked in FROM, and the names and types they answer under
-- ============================================================================

-- begin-expected
-- columns: string_to_table
-- row: a
-- row: b
-- row: c
-- end-expected
SELECT string_to_table('a,b,c', ',');

-- note: the third argument names the string that stands for NULL
-- begin-expected
-- columns: string_to_table
-- row: a
-- row: NULL
-- row: c
-- end-expected
SELECT string_to_table('a,b,c', ',', 'b');

-- begin-expected
-- columns: regexp_split_to_table
-- row: a
-- row: b
-- row: c
-- end-expected
SELECT regexp_split_to_table('a1b2c', '[0-9]');

-- note: the many-argument unnest produces a row of several columns, which a select-list
-- note: expression has no room for -- PostgreSQL has no such function to call there
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function unnest(integer[], text[]) does not exist
-- end-expected-error
SELECT unnest(ARRAY[1,2], ARRAY['a','b']);

-- note: in FROM every column of it is named for the function and carries its array's type
-- begin-expected
-- columns: unnest | unnest
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT * FROM unnest(ARRAY[1,2], ARRAY['a','b']) ORDER BY 1;

-- note: a user function's rows carry the type it declared, not text
-- begin-expected
-- columns: src_fint
-- row: 1
-- row: 2
-- end-expected
SELECT * FROM src_fint();

-- begin-expected
-- columns: src_fint | ordinality
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT * FROM src_fint() WITH ORDINALITY;

-- begin-expected
-- columns: v | n
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT * FROM src_fint() WITH ORDINALITY t(v, n);

-- note: a sub-query written with a star keeps the underlying column's name, the way the same
-- note: query written with the column keeps it
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
WITH c AS (SELECT id FROM src_dpt) SELECT (SELECT * FROM c LIMIT 1);

-- begin-expected
-- columns: generate_series
-- row: 1
-- end-expected
SELECT (SELECT * FROM generate_series(1,1));

-- ============================================================================
-- 8. Ordinary SQL, which has to keep working
-- ============================================================================

-- begin-expected
-- columns: generate_series
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT generate_series(1,3) ORDER BY 1;

-- begin-expected
-- columns: unnest
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT unnest(ARRAY[1,2,3]) ORDER BY 1;

-- begin-expected
-- columns: a | generate_series
-- row: 1, 1
-- row: 1, 2
-- row: 2, 1
-- row: 2, 2
-- end-expected
SELECT a, generate_series(1,2) FROM src_r1 ORDER BY 1, 2;

-- note: the sets of one row run side by side, the shorter reading NULL past its end
-- begin-expected
-- columns: generate_series | generate_series
-- row: 1, 1
-- row: 2, 2
-- row: NULL, 3
-- end-expected
SELECT generate_series(1,2), generate_series(1,3) ORDER BY 1;

-- note: a set inside a sub-query belongs to that query, not to the IN around it
-- begin-expected
-- columns: ?column?
-- row: true
-- end-expected
SELECT 1 IN (SELECT generate_series(1,2));

-- begin-expected
-- columns: coalesce | nullif | greatest | least
-- row: 1, NULL, 1, 1
-- row: 2, 2, 2, 1
-- end-expected
SELECT coalesce(a, 0), nullif(a, 1), greatest(a, 1), least(a, 1) FROM src_r1 ORDER BY 1;

-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*) FROM src_dpt d FULL JOIN src_emp e ON d.id = e.dept_id;

-- note: an alias list shorter than the item is ordinary; the rest keep their own names
-- begin-expected
-- columns: v
-- row: 1
-- row: 2
-- end-expected
SELECT * FROM generate_series(1,2) AS t(v) ORDER BY 1;

-- begin-expected
-- columns: v | w
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT * FROM generate_series(1,2) WITH ORDINALITY AS t(v, w) ORDER BY 1;

-- begin-expected
-- columns: a | b
-- row: 1, 2
-- end-expected
SELECT * FROM (SELECT 1, 2) AS t(a, b) LIMIT 1;

-- begin-expected
-- columns: x
-- row: 1
-- end-expected
WITH c(x) AS (SELECT 1) SELECT * FROM c;

-- begin-expected
-- columns: row_number
-- row: 1
-- row: 2
-- end-expected
SELECT row_number() OVER (ORDER BY a) FROM src_r1;

-- begin-expected
-- columns: sum
-- row: 1
-- row: 3
-- end-expected
SELECT sum(a) OVER (ORDER BY a) FROM src_r1 ORDER BY 1;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FILTER (WHERE true) FROM src_r1;

-- note: an ordinary UPDATE matching nothing is still an ordinary UPDATE
UPDATE src_dpt SET budget = 1 WHERE id = -1;

-- note: INSERT ... SELECT has rows of its own to expand into, and always did
INSERT INTO src_r3 SELECT generate_series(3,4);

-- begin-expected
-- columns: a
-- row: 3
-- row: 4
-- end-expected
SELECT a FROM src_r3 ORDER BY a;

-- cleanup
DROP FUNCTION IF EXISTS src_fint();
DROP TABLE IF EXISTS src_t2 CASCADE;
DROP TABLE IF EXISTS src_r3 CASCADE;
DROP TABLE IF EXISTS src_r1 CASCADE;
DROP TABLE IF EXISTS src_emp CASCADE;
DROP TABLE IF EXISTS src_dpt CASCADE;
