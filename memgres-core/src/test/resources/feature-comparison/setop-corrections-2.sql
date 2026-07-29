-- What a set operation, a row comparison and a multi-column assignment mean.
--
-- Eight things, all measured against PostgreSQL 18.
--
-- 1. The parentheses of a set operation come off one at a time. What stands inside them is a
--    whole query expression whose own arms may be parenthesised in turn, so each closing
--    parenthesis belongs to whatever its own opening one began. Counting the run of opening
--    parentheses and then expecting that many closing ones made
--    (SELECT 1) UNION ((SELECT 2) UNION (SELECT 3)) a syntax error at the inner UNION.
--
-- 2. An INSERT is not an arm of a set operation. INSERT INTO t(id) (SELECT 1) UNION (SELECT 2)
--    inserts both rows; reading the parentheses off the source and stopping there left the UNION
--    to the statement level, which made the INSERT itself the left arm. The INSERT ran, the arms
--    were then found to be of different widths, and the row it had already written stayed.
--
-- 3. Two rows are unequal when some pair of members is non-null and unequal, whatever else is
--    null, and unknown only when no pair settles it. A ROW value is not a java list, so a row
--    compared against a subquery missed the row path and was compared as its printed text:
--    (1, NULL) = (SELECT 1, 2) came back false and (1, NULL) = (SELECT 1, NULL) came back true.
--    A string written in a row carries no type of its own and is read as the type opposite it,
--    which is what fails when the text is not a value of that type.
--
-- 4. One assignment may name several columns: SET (a, b) = (SELECT x, y ...) and
--    SET (a, b) = (1, 'z') were a syntax error at the parenthesis. The source must be a
--    sub-SELECT or a row constructor, of exactly as many columns as are named, and no column may
--    be assigned twice in one statement.
--
-- 5. A name two output columns answer to names neither. Written on a set operation any repeat is
--    ambiguous; written on a plain SELECT it is ambiguous unless the two are the same expression,
--    which is one thing under two names. A quoted alias keeps its case, so ORDER BY foo does not
--    reach a column called "Foo" -- and the name of an arm the operation does not carry is
--    accounted for by naming that arm.
--
-- 6. A VALUES list is a query, so it takes ORDER BY, LIMIT and OFFSET. A list of two rows or more
--    is rewritten as a set operation, whose parsing read them; a list of one row had nothing to
--    read them at all.
--
-- 7. A set operation tells rows apart, so every output column needs an equality to be compared
--    by; json, xml and point have none. It also gives each column one type: a date and a
--    timestamp are one column of timestamps, and a date and a time have no such column.
--
-- 8. A subquery standing where one value stands may have only one column, and that is settled
--    from its select list -- so an entry of an IN list or of an array constructor is judged
--    whether or not the comparison ever reaches it.
--
-- The last section is ordinary SQL, which has to keep working: the cost of a rule that reaches
-- too far is a refused valid statement.

-- setup
DROP TABLE IF EXISTS stn_t CASCADE;
DROP TABLE IF EXISTS stn_u CASCADE;
DROP TABLE IF EXISTS stn_n CASCADE;
DROP TABLE IF EXISTS stn_tgt CASCADE;
DROP TABLE IF EXISTS stn_dept CASCADE;
DROP TABLE IF EXISTS stn_emp CASCADE;
DROP TABLE IF EXISTS stn_ob CASCADE;
DROP TABLE IF EXISTS stn_p CASCADE;
DROP FUNCTION IF EXISTS stn_arr();
DROP FUNCTION IF EXISTS stn_out();
DROP FUNCTION IF EXISTS stn_setofint();

-- stn_t is updated and deleted from by section 4 and by the last section, and an earlier file
-- in this suite leaves a publication FOR ALL TABLES behind, under which a table with no
-- replica identity may not be updated or deleted from (55000). stn_t cannot take a primary
-- key -- section 4 writes a null into a -- so it is given a replica identity of its own.
CREATE TABLE stn_t (a int, b text);
ALTER TABLE stn_t REPLICA IDENTITY FULL;
INSERT INTO stn_t VALUES (1, 'a'), (2, 'b');

CREATE TABLE stn_u (c int, d text);
INSERT INTO stn_u VALUES (3, 'c');

CREATE TABLE stn_n (i int, j int);
INSERT INTO stn_n VALUES (1, NULL), (1, 2), (2, 2);

CREATE TABLE stn_tgt (id int PRIMARY KEY, dept_id int, name text);

CREATE TABLE stn_dept (id int PRIMARY KEY, name text);
INSERT INTO stn_dept VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

CREATE TABLE stn_emp (id int PRIMARY KEY, dept_id int, name text);
INSERT INTO stn_emp VALUES (1, 1, 'e'), (5, 2, 'f');

CREATE TABLE stn_ob (n int, s text);
INSERT INTO stn_ob VALUES (1, 'zz'), (2, 'aa');

CREATE TABLE stn_p (k int, m int);
INSERT INTO stn_p VALUES (1, 2);

CREATE FUNCTION stn_arr() RETURNS int[] AS $$ SELECT ARRAY[1,2] $$ LANGUAGE sql;
CREATE FUNCTION stn_out(OUT x int, OUT y text) RETURNS SETOF record AS $$ SELECT 1, 'a' $$ LANGUAGE sql;
CREATE FUNCTION stn_setofint() RETURNS SETOF int AS $$ SELECT 1 UNION ALL SELECT 2 $$ LANGUAGE sql;

-- ============================================================================
-- 1. The parentheses of a set operation come off one at a time
-- ============================================================================

-- expected: 1, 2, 3
SELECT 1 UNION ((SELECT 2) UNION (SELECT 3)) ORDER BY 1;

-- expected: 1, 2, 3, 4
((SELECT 1) UNION (SELECT 2)) UNION ((SELECT 3) UNION (SELECT 4)) ORDER BY 1;

-- expected: 2
((SELECT 1) UNION (SELECT 2)) INTERSECT ((SELECT 2) UNION (SELECT 5)) ORDER BY 1;

-- expected: 1
SELECT 1 EXCEPT ((SELECT 2) UNION (SELECT 3)) ORDER BY 1;

-- note: any depth of parentheses, closing where each of them opened
-- expected: 1, 2, 3
SELECT 1 UNION (((SELECT 2) UNION (SELECT 3))) ORDER BY 1;

-- expected: 1, 2
((((SELECT 1)) UNION ((SELECT 2)))) ORDER BY 1;

-- expected: 1, 2, 3
(SELECT 1) UNION ((SELECT 2) UNION ((SELECT 3))) ORDER BY 1;

-- expected: 2
(SELECT 1 UNION SELECT 2) INTERSECT ((SELECT 2) UNION (SELECT 9)) ORDER BY 1;

-- expected: 1
(SELECT 1 UNION SELECT 2) EXCEPT ((SELECT 2) UNION (SELECT 9)) ORDER BY 1;

-- note: a parenthesised arm still keeps its own LIMIT
-- expected: 1, 3
(SELECT a FROM stn_t ORDER BY a LIMIT 1) UNION (SELECT c FROM stn_u) ORDER BY 1;

-- ============================================================================
-- 2. An INSERT is not an arm of a set operation
-- ============================================================================

INSERT INTO stn_tgt(id) (SELECT 1) UNION (SELECT 2);

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM stn_tgt ORDER BY id;

DELETE FROM stn_tgt;
INSERT INTO stn_tgt(id, name) (SELECT 1, 'x') UNION (SELECT 2, 'y');

-- begin-expected
-- columns: id | name
-- row: 1, x
-- row: 2, y
-- end-expected
SELECT id, name FROM stn_tgt ORDER BY id;

DELETE FROM stn_tgt;
INSERT INTO stn_tgt(id) ((SELECT 1) UNION (SELECT 2));

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM stn_tgt ORDER BY id;

DELETE FROM stn_tgt;
INSERT INTO stn_tgt(id) (SELECT 1) UNION ALL (SELECT 2) ORDER BY 1;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM stn_tgt ORDER BY id;

DELETE FROM stn_tgt;

-- note: the arms differ in width, so there is no statement to run
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
INSERT INTO stn_tgt(id) SELECT 1, 2 UNION SELECT 3;

-- note: and nothing was written before the complaint
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM stn_tgt;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "UNION"
-- end-expected-error
UPDATE stn_t SET a = 9 UNION SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "UNION"
-- end-expected-error
DELETE FROM stn_t UNION SELECT 1;

-- ============================================================================
-- 3. Two rows are unequal when some pair of members settles it
-- ============================================================================

-- note: 1 = 1, and NULL against 2 settles nothing
-- expected: NULL
SELECT ((1, NULL) = (SELECT 1, 2))::text;

-- expected: NULL
SELECT ((1, NULL) = (SELECT 1, NULL))::text;

-- expected: NULL
SELECT ((1, NULL) <> (SELECT 1, 2))::text;

-- expected: NULL
SELECT ((1, NULL) = (1, 2))::text;

-- note: 1 against 3 differs, which settles it however many members are null
-- expected: false
SELECT ((NULL, 1) = (2, 3))::text;

-- expected: false
SELECT ((NULL, 1) = (NULL, 3))::text;

-- expected: true
SELECT ((NULL, 1) <> (2, 3))::text;

-- expected: false
SELECT ((1, NULL) = (SELECT 2, NULL))::text;

-- begin-expected
-- columns: i | j
-- row: 2, 2
-- end-expected
SELECT i, j FROM stn_n WHERE (i, j) <> (SELECT 1, 2) ORDER BY i, j NULLS LAST;

-- note: an array is one value, and a null in it is a part of that value
-- expected: true
SELECT (ARRAY[NULL, 1] = ARRAY[NULL, 1])::text;

-- expected: false
SELECT (ARRAY[NULL, 1] <> ARRAY[NULL, 1])::text;

-- expected: false
SELECT (ARRAY[NULL, 1] = ARRAY[2, 3])::text;

-- note: 'a' has no type of its own and is read as the integer opposite it
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "a"
-- end-expected-error
SELECT ((1, 'a') = (SELECT 1, 2))::text;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "a"
-- end-expected-error
SELECT ((1, 'a') = (1, 1))::text;

-- note: and text that is a value of that type is simply read as one
-- expected: true
SELECT ((1, '1') = (1, 1))::text;

-- expected: false
SELECT ((1, 'a') = (1, 'b'))::text;

-- note: rows of different widths have no comparison between them
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unequal number of entries in row expressions
-- end-expected-error
SELECT (ROW(1,2) IN (ROW(1,2,3)))::text;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: unequal number of entries in row expressions
-- end-expected-error
SELECT (ROW(1,2) IN (ROW(1,2), ROW(1,2,3)))::text;

-- note: nor has a row against a single value
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: record = integer
-- end-expected-error
SELECT ((1,2) IS DISTINCT FROM (SELECT 1))::text;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: record = integer
-- end-expected-error
SELECT ((1,2) IS NOT DISTINCT FROM (SELECT 1))::text;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: record = integer
-- end-expected-error
SELECT ((1,2) IS DISTINCT FROM 1)::text;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = record
-- end-expected-error
SELECT (1 IS DISTINCT FROM (1,2))::text;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: record = integer
-- end-expected-error
SELECT count(*) FROM stn_dept d WHERE (d.id, d.name) IN ((1));

-- ============================================================================
-- 4. One assignment may name several columns
-- ============================================================================

INSERT INTO stn_tgt(id, dept_id, name) VALUES (610, 1, 'a');
UPDATE stn_tgt SET (dept_id, name) = (2, 'b') WHERE id = 610;

-- begin-expected
-- columns: id | dept_id | name
-- row: 610, 2, b
-- end-expected
SELECT id, dept_id, name FROM stn_tgt ORDER BY id;

UPDATE stn_tgt SET (dept_id, name) = (SELECT id, name FROM stn_dept WHERE id = 3) WHERE id = 610;

-- begin-expected
-- columns: id | dept_id | name
-- row: 610, 3, c
-- end-expected
SELECT id, dept_id, name FROM stn_tgt ORDER BY id;

DELETE FROM stn_tgt;

UPDATE stn_t SET (a, b) = (SELECT 9, 'z') WHERE a = 1;

-- begin-expected
-- columns: a | b
-- row: 2, b
-- row: 9, z
-- end-expected
SELECT a, b FROM stn_t ORDER BY a;

UPDATE stn_t SET (a, b) = ROW(7, 'q') WHERE a = 9;

-- begin-expected
-- columns: a | b
-- row: 2, b
-- row: 7, q
-- end-expected
SELECT a, b FROM stn_t ORDER BY a;

-- note: one column named, and ROW() is still a row
UPDATE stn_t SET (a) = ROW(5) WHERE a = 7;

-- begin-expected
-- columns: a | b
-- row: 2, b
-- row: 5, q
-- end-expected
SELECT a, b FROM stn_t ORDER BY a;

UPDATE stn_t SET (a, b) = (SELECT c, d FROM stn_u WHERE c = 3) WHERE a = 5;

-- begin-expected
-- columns: a | b
-- row: 2, b
-- row: 3, c
-- end-expected
SELECT a, b FROM stn_t ORDER BY a;

-- note: a source that finds no row leaves every column null
UPDATE stn_t SET (a, b) = (SELECT 9, 'z' WHERE false) WHERE a = 3;

-- begin-expected
-- columns: a | b
-- row: 2, b
-- row: NULL, NULL
-- end-expected
SELECT a, b FROM stn_t ORDER BY a NULLS LAST;

DELETE FROM stn_t;
INSERT INTO stn_t VALUES (1, 'a'), (2, 'b');

-- note: (5) is a parenthesised expression and not a row constructor
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression
-- end-expected-error
UPDATE stn_t SET (a) = (5) WHERE a = 1;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression
-- end-expected-error
UPDATE stn_t SET (a, b) = (9) WHERE a = 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: number of columns does not match number of values
-- end-expected-error
UPDATE stn_t SET (a, b) = (1, 'z', 3) WHERE a = 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: number of columns does not match number of values
-- end-expected-error
UPDATE stn_t SET (a, b) = (SELECT 1) WHERE a = 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: number of columns does not match number of values
-- end-expected-error
UPDATE stn_t SET (a, b) = (SELECT 1, 'z', 3) WHERE a = 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: multiple assignments to same column "b"
-- end-expected-error
UPDATE stn_t SET (a, b) = (9, 'z'), b = 'k' WHERE a = 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: multiple assignments to same column "b"
-- end-expected-error
UPDATE stn_t SET b = 'x', b = 'y' WHERE a = 1;

-- note: none of those five ran
-- begin-expected
-- columns: a | b
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT a, b FROM stn_t ORDER BY a;

-- ============================================================================
-- 5. A name two output columns answer to names neither
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42702
-- message-like: ORDER BY "id" is ambiguous
-- end-expected-error
SELECT id, id AS id FROM stn_dept UNION SELECT id, id FROM stn_emp ORDER BY id;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: ORDER BY "a" is ambiguous
-- end-expected-error
SELECT a, a FROM stn_t UNION SELECT c, c FROM stn_u ORDER BY a;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: ORDER BY "k" is ambiguous
-- end-expected-error
SELECT a AS k, b AS k FROM stn_t UNION SELECT c, d FROM stn_u ORDER BY k;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: ORDER BY "n" is ambiguous
-- end-expected-error
SELECT n, s AS n FROM stn_ob ORDER BY n;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: ORDER BY "k" is ambiguous
-- end-expected-error
SELECT a AS k, b AS k FROM stn_t ORDER BY k;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: ORDER BY "a" is ambiguous
-- end-expected-error
SELECT a, a + 1 AS a FROM stn_t ORDER BY a;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: ORDER BY "b" is ambiguous
-- end-expected-error
SELECT a AS b, b FROM stn_t ORDER BY b;

-- note: one expression under two names is one thing, and settles the sort either way
-- begin-expected
-- columns: a | a
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT a, a FROM stn_t ORDER BY a;

-- begin-expected
-- columns: k | k
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT a AS k, a AS k FROM stn_t ORDER BY k;

-- note: a quoted alias keeps its case, so foo is not the column "Foo"
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "foo" does not exist
-- hint-like: Perhaps you meant to reference the column "*SELECT* 1.Foo".
-- end-expected-error
SELECT a AS "Foo" FROM stn_t UNION SELECT c FROM stn_u ORDER BY foo;

-- expected: 1, 2, 3
SELECT a AS "Foo" FROM stn_t UNION SELECT c FROM stn_u ORDER BY "Foo";

-- note: a name of the second arm, which the operation does not carry
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "b" does not exist
-- detail-like: There is a column named "b" in table "*SELECT* 2", but it cannot be referenced from this part of the query.
-- end-expected-error
SELECT id AS a FROM stn_dept UNION SELECT id AS b FROM stn_emp ORDER BY b;

-- note: a schema written in front of an entry that is reached by its alias
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "stn_emp"
-- detail-like: There is an entry for table "stn_emp", but it cannot be referenced from this part of the query.
-- end-expected-error
SELECT pg_catalog.stn_emp.id FROM stn_emp;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "a"
-- detail-like: There is an entry for table "a", but it cannot be referenced from this part of the query.
-- end-expected-error
SELECT public.a.k FROM stn_p a;

-- ============================================================================
-- 6. A VALUES list is a query of its own
-- ============================================================================

-- expected: 7
VALUES (7) ORDER BY 1;

-- expected: 7
VALUES (7) ORDER BY column1;

-- expected: 7
VALUES (7) LIMIT 1;

-- begin-expected
-- columns: column1
-- end-expected
VALUES (7) OFFSET 1;

-- begin-expected
-- columns: column1 | column2
-- row: 1, a
-- end-expected
VALUES (1, 'a') ORDER BY column2;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
VALUES (7) ORDER BY NULL;

-- begin-expected-error
-- sqlstate: 42P10
-- end-expected-error
VALUES (7) ORDER BY 2;

-- expected: 1, 2, 3
VALUES (3), (1), (2) ORDER BY column1;

-- expected: 7, 8
VALUES (7) UNION VALUES (8) ORDER BY 1;

-- ============================================================================
-- 7. What a set operation can tell rows apart by
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not identify an equality operator for type json
-- end-expected-error
SELECT '{}'::json UNION SELECT '[]'::json;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not identify an equality operator for type json
-- end-expected-error
SELECT '{}'::json INTERSECT SELECT '[]'::json;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not identify an equality operator for type json
-- end-expected-error
SELECT '{}'::json EXCEPT SELECT '[]'::json;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not identify an equality operator for type xml
-- end-expected-error
SELECT '<a/>'::xml UNION SELECT '<b/>'::xml;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not identify an equality operator for type point
-- end-expected-error
SELECT point '(1,2)' UNION SELECT point '(3,4)';

-- note: UNION ALL compares nothing, so it takes them
-- begin-expected
-- columns: json
-- row: {}
-- row: []
-- end-expected
SELECT '{}'::json UNION ALL SELECT '[]'::json;

-- note: a date is a timestamp of midnight, so the two arms are one row
-- begin-expected
-- columns: date
-- row: 2020-01-01 00:00:00
-- end-expected
SELECT '2020-01-01'::date UNION SELECT '2020-01-01'::timestamp ORDER BY 1;

-- begin-expected
-- columns: timestamp
-- row: 2020-01-01 00:00:00
-- row: 2020-01-02 00:00:00
-- end-expected
SELECT '2020-01-01'::timestamp UNION SELECT '2020-01-02'::date ORDER BY 1;

-- note: a date and a time of day have no one type between them
-- begin-expected-error
-- sqlstate: 42846
-- message-like: UNION could not convert type time without time zone to date
-- end-expected-error
SELECT '2020-01-01'::date UNION SELECT '10:00'::time;

-- begin-expected-error
-- sqlstate: 42846
-- message-like: INTERSECT could not convert type date to time without time zone
-- end-expected-error
SELECT '10:00'::time INTERSECT SELECT '2020-01-01'::date;

-- ============================================================================
-- 8. A subquery where one value stands
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT 1 WHERE 1 = ANY (ARRAY[(SELECT 1, 2)]);

-- note: the entry at fault is one the comparison never reaches
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT 1 WHERE 1 IN (1, (SELECT 1, 2));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT 1 WHERE 1 IN (1, 2, (SELECT 3, 4));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT ARRAY[1, (SELECT 1, 2)];

-- note: the subquery form of IN still reports its width as a width
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery has too many columns
-- end-expected-error
SELECT 1 WHERE 1 IN (SELECT 1, 2);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery has too few columns
-- end-expected-error
SELECT 1 WHERE (1,2) IN (SELECT 1);

-- ============================================================================
-- The types and shapes a result carries
-- ============================================================================

-- note: ARRAY() over a set operation is an array of what its arms write
-- begin-expected
-- columns: r
-- row: {1,2}
-- end-expected
SELECT ARRAY(SELECT 1 UNION ALL SELECT 2) AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT (SELECT 1 UNION SELECT 1) AS r;

-- note: a function that does not return a set answers with one value, array or not
-- begin-expected
-- columns: stn_arr
-- row: {1,2}
-- end-expected
SELECT * FROM stn_arr();

-- note: a built-in returning record needs its columns named, as any other does
-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is required for functions returning "record"
-- end-expected-error
SELECT * FROM json_to_record('{"a":1}'::json);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is required for functions returning "record"
-- end-expected-error
SELECT * FROM json_to_recordset('[{"a":1}]'::json);

-- expected: 1
SELECT * FROM json_to_record('{"a":1}'::json) AS t(a int);

-- note: OUT parameters name the columns, so the set is what is left to complain about
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: aggregate function calls cannot contain set-returning function calls
-- end-expected-error
SELECT string_agg((stn_out()).y, ',');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: aggregate function calls cannot contain set-returning function calls
-- end-expected-error
SELECT sum(stn_setofint());

-- note: a cast that carries a length is a coercion, whatever the type is
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT c1 FROM (SELECT 1.5::numeric AS c1) x UNION SELECT 2.5 ORDER BY c1::numeric(10,2);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT c1 FROM (SELECT 'x'::varchar(4) AS c1) x UNION SELECT 'y' ORDER BY c1::varchar(4);

-- note: and one that carries no length over a column of that type is still no cast
-- expected: 1.5, 2.5
SELECT c1 FROM (SELECT 1.5::numeric AS c1) x UNION SELECT 2.5 ORDER BY c1::numeric;

-- ============================================================================
-- Ordinary SQL that has to keep working
-- ============================================================================

-- expected: 1, 2
SELECT 1 UNION SELECT 2 ORDER BY 1;

-- expected: 1, 2
(SELECT 1) UNION (SELECT 2) ORDER BY 1;

-- expected: 1, 2, 3
(SELECT 1 UNION SELECT 2) UNION SELECT 3 ORDER BY 1;

-- expected: 1, 2, 3
SELECT 1 UNION (SELECT 2 UNION SELECT 3) ORDER BY 1;

-- expected: 1, 2
((SELECT 1)) UNION ((SELECT 2)) ORDER BY 1;

-- expected: 1, 2
((SELECT 1) UNION (SELECT 2)) ORDER BY 1;

-- expected: 1, 1, 2
((SELECT 1) UNION ALL (SELECT 1)) UNION ALL ((SELECT 2)) ORDER BY 1;

-- expected: 1, 2
WITH w AS ((SELECT 1) UNION (SELECT 2)) SELECT * FROM w ORDER BY 1;

-- expected: 1, 2
SELECT * FROM (SELECT 1 UNION SELECT 2) z ORDER BY 1;

-- begin-expected
-- columns: a | b
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT a, b FROM stn_t ORDER BY a;

-- begin-expected
-- columns: k | b
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT a AS k, b FROM stn_t ORDER BY k;

-- begin-expected
-- columns: a | a2
-- row: 1, 2
-- row: 2, 3
-- end-expected
SELECT a, a + 1 AS a2 FROM stn_t ORDER BY a;

-- expected: 1, 2, 3
SELECT a AS k FROM stn_t UNION SELECT c FROM stn_u ORDER BY k;

-- expected: 1, 2, 3
SELECT a AS k FROM stn_t UNION SELECT c FROM stn_u ORDER BY K;

-- note: an ordinal has no name to be ambiguous about, and neither has a column of the FROM clause
-- begin-expected
-- columns: k | k
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT a AS k, b AS k FROM stn_t ORDER BY 1;

-- begin-expected
-- columns: k | k
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT a AS k, b AS k FROM stn_t ORDER BY a;

-- begin-expected
-- columns: a | b
-- row: 2, b
-- row: 1, a
-- end-expected
SELECT a, b FROM stn_t ORDER BY a DESC, b;

-- expected: 1
SELECT 1 WHERE 1 IN (1, 2, 3);

-- expected: 1
SELECT 1 WHERE 1 IN (SELECT 1);

-- expected: 1
SELECT 1 WHERE 1 = ANY (ARRAY[1, 2]);

-- expected: 1
SELECT 1 WHERE 1 = ANY (SELECT 1 UNION SELECT 2);

-- expected: 1, 2
SELECT a FROM stn_t WHERE (a, b) IN ((1, 'a'), (2, 'b')) ORDER BY 1;

-- expected: true
SELECT ((1, 2) = (SELECT 1, 2))::text;

-- expected: true
SELECT ((1, 2) < (SELECT 1, 3))::text;

-- expected: true
SELECT (ROW(1, 'a') = ROW(1, 'a'))::text;

-- expected: true
SELECT ((1,2) IS DISTINCT FROM (1,3))::text;

-- begin-expected
-- columns: array
-- row: {1,2}
-- end-expected
SELECT ARRAY[1, (SELECT 2)];

-- expected: 1, 2, 3
SELECT * FROM generate_series(1, 3) ORDER BY 1;

-- expected: 1, 2
SELECT * FROM unnest(ARRAY[1, 2]) ORDER BY 1;

-- expected: 1, 2
SELECT * FROM stn_setofint() ORDER BY 1;

-- begin-expected
-- columns: x | y
-- row: 1, a
-- end-expected
SELECT * FROM stn_out();

-- expected: 1, 2, 3
SELECT 1 UNION SELECT 2 UNION SELECT 3 ORDER BY 1;

-- expected: 1, 1.5
SELECT 1 UNION SELECT 1.5 ORDER BY 1;

-- expected: a, b, c
SELECT b FROM stn_t UNION SELECT d FROM stn_u ORDER BY 1;

-- expected: 2020-01-01, 2020-01-02
SELECT '2020-01-01'::date UNION SELECT '2020-01-02'::date ORDER BY 1;

-- expected: 10:00:00, 11:00:00
SELECT '10:00'::time UNION SELECT '11:00'::time ORDER BY 1;

UPDATE stn_t SET a = 9 WHERE a = 1;
UPDATE stn_t SET a = 8, b = 'z' WHERE a = 9;
UPDATE stn_t SET a = (SELECT 7) WHERE a = 8;
UPDATE stn_t SET b = (SELECT d FROM stn_u WHERE c = 3) WHERE a = 7;

-- begin-expected
-- columns: a | b
-- row: 2, b
-- row: 7, c
-- end-expected
SELECT a, b FROM stn_t ORDER BY a;

INSERT INTO stn_tgt(id) SELECT 5;
INSERT INTO stn_tgt(id) (SELECT 6);
INSERT INTO stn_tgt(id) VALUES (7), (8);

-- begin-expected
-- columns: id
-- row: 5
-- row: 6
-- row: 7
-- row: 8
-- end-expected
SELECT id FROM stn_tgt ORDER BY id;

-- cleanup
DROP FUNCTION IF EXISTS stn_setofint();
DROP FUNCTION IF EXISTS stn_out();
DROP FUNCTION IF EXISTS stn_arr();
DROP TABLE IF EXISTS stn_p CASCADE;
DROP TABLE IF EXISTS stn_ob CASCADE;
DROP TABLE IF EXISTS stn_emp CASCADE;
DROP TABLE IF EXISTS stn_dept CASCADE;
DROP TABLE IF EXISTS stn_tgt CASCADE;
DROP TABLE IF EXISTS stn_n CASCADE;
DROP TABLE IF EXISTS stn_u CASCADE;
DROP TABLE IF EXISTS stn_t CASCADE;
