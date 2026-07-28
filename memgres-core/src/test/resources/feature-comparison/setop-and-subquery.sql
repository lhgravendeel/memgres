-- What a set operation may be ordered by, and how wide a subquery standing for one value may be.
--
-- Three things, all measured against PostgreSQL 18.
--
-- 1. The ORDER BY of a set operation sees the output columns and nothing else. Once the arms have
--    been combined there is no relation left to read a name from, so PostgreSQL takes an output
--    column name or its position and refuses everything else -- and the refusal differs by shape:
--    an ordinal outside the select list is out of range, a bare non-integer constant names no
--    column at all, a name written anywhere in the item that the output does not account for is a
--    missing column (or a missing FROM entry when it is qualified), and anything that survives
--    that and is still not a plain column name is an expression the clause does not take. None of
--    this was checked: the sort matched what it could and quietly ignored the rest. The LIMIT and
--    OFFSET beside it were read with neither a sign check nor a rounding, so OFFSET -1 changed the
--    row order instead of raising -- a wrong answer rather than an error.
--
-- 2. A subquery standing where one value is expected may have one column, and that is a property
--    of its select list rather than of the rows it happens to return. The width was read off the
--    first row, so a wide subquery that returned nothing answered NULL, ARRAY(SELECT 1, 2) came
--    back as {1} with the second column dropped, and a query whose real fault was its width was
--    reported as returning more than one row.
--
-- 3. The other half of the same rule ran the wrong way. A row constructor compared against a
--    subquery reads the whole subquery row, so (1, 2) = (SELECT 1, 2) is a row comparison and
--    true; sending it down the scalar path refused it. Six operators take that reading. IS
--    DISTINCT FROM does not, and neither does a subquery written on the left -- both measured,
--    not assumed.
--
-- 4. A bare constant in ORDER BY is an output-column position and nothing else, in a FROM-less
--    SELECT as much as anywhere. That check existed but the FROM-less path never reached it.
--
-- The last section is ordinary SQL that has to keep working: the cost of a rule that reaches too
-- far is a refused valid statement, and every rule added here fires only where PostgreSQL already
-- errors.

-- setup
DROP TABLE IF EXISTS sos_p CASCADE;
DROP TABLE IF EXISTS sos_t CASCADE;

CREATE TABLE sos_t (a int, b text);
INSERT INTO sos_t VALUES (1, 'x'), (2, 'y'), (3, 'z');

CREATE TABLE sos_p (x int, y int);
INSERT INTO sos_p VALUES (1, 2);

-- ============================================================================
-- 1. A set operation's ORDER BY takes an output column or its position
-- ============================================================================

-- note: an ordinal outside the select list is out of range, whichever operation it follows
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 5 is not in select list
-- end-expected-error
SELECT 1 UNION SELECT 2 ORDER BY 5;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 0 is not in select list
-- end-expected-error
SELECT 1 UNION SELECT 2 ORDER BY 0;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position -1 is not in select list
-- end-expected-error
SELECT 1 UNION SELECT 2 ORDER BY -1;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 5 is not in select list
-- end-expected-error
SELECT 1 INTERSECT SELECT 2 ORDER BY 5;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 5 is not in select list
-- end-expected-error
SELECT 1 EXCEPT SELECT 2 ORDER BY 5;

-- note: a chain of set operations is judged by the output of the whole chain
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 5 is not in select list
-- end-expected-error
SELECT 1 UNION SELECT 2 UNION SELECT 3 ORDER BY 5;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 5 is not in select list
-- end-expected-error
SELECT 1 UNION (SELECT 2 UNION SELECT 3) ORDER BY 5;

-- note: and it is judged wherever the set operation stands -- in a CTE
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 5 is not in select list
-- end-expected-error
WITH c AS (SELECT 1 UNION SELECT 2 ORDER BY 5) SELECT * FROM c;

-- note: or in a sub-select
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 5 is not in select list
-- end-expected-error
SELECT * FROM (SELECT 1 UNION SELECT 2 ORDER BY 5) s;

-- note: a name the output does not account for is a missing column, not an unsupported clause
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "a" does not exist
-- end-expected-error
SELECT 1 UNION SELECT 2 ORDER BY a + 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "a" does not exist
-- end-expected-error
SELECT 1 EXCEPT SELECT 2 ORDER BY a + 1;

-- note: the output name comes from the first arm, so an alias given only in the second is unknown
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "z" does not exist
-- end-expected-error
SELECT 1 UNION ALL SELECT 2 AS z ORDER BY z;

-- note: a column of an arm that is not in the output is unknown too
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "b" does not exist
-- end-expected-error
SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY b;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "a" does not exist
-- end-expected-error
SELECT a AS x FROM sos_t UNION ALL SELECT 2 ORDER BY a;

-- note: a qualified name has no FROM entry left to qualify against, even its own table's
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "t"
-- end-expected-error
SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY t.a;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "sos_t"
-- end-expected-error
SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY sos_t.a;

-- note: anything that resolves and is still not a plain column name is an expression
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT 1 UNION SELECT 2 ORDER BY 5 - 4;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT 1 UNION SELECT 2 ORDER BY 1 + 0;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT 1 AS q UNION SELECT 2 ORDER BY upper('a');

-- note: including a column of the output with anything done to it
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY a + 1;

-- note: a cast is an expression too, where the bare literal would have been a constant
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT 1 AS q UNION SELECT 2 ORDER BY 'x'::text;

-- note: COLLATE makes an expression of the column it is written on
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: invalid UNION/INTERSECT/EXCEPT ORDER BY clause
-- end-expected-error
SELECT b FROM sos_t UNION ALL SELECT 'q' ORDER BY b COLLATE "C";

-- note: and it is refused earlier when the thing collated has no collation to give
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type integer
-- end-expected-error
SELECT a AS x FROM sos_t UNION ALL SELECT 2 ORDER BY x COLLATE "C";

-- note: COLLATE binds to the ordinal itself, so the type asked is always integer
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type integer
-- end-expected-error
SELECT b FROM sos_t UNION ALL SELECT 'q' ORDER BY 1 COLLATE "C";

-- note: a bare constant that is not an integer names no column at all
-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT 1 AS q UNION SELECT 2 ORDER BY NULL;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT 1 AS q UNION SELECT 2 ORDER BY 1.5;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT 1 AS q UNION SELECT 2 ORDER BY true;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT 1 AS q UNION SELECT 2 ORDER BY 'x';

-- ============================================================================
-- 2. LIMIT and OFFSET after a set operation are read as a plain SELECT reads them
-- ============================================================================

-- note: this is the one that answered rather than raised, and answered in a different order
-- begin-expected-error
-- sqlstate: 2201X
-- message-like: OFFSET must not be negative
-- end-expected-error
SELECT 1 UNION ALL SELECT 2 ORDER BY 1 OFFSET -1;

-- begin-expected-error
-- sqlstate: 2201W
-- message-like: LIMIT must not be negative
-- end-expected-error
SELECT 1 UNION ALL SELECT 2 ORDER BY 1 LIMIT -1;

-- note: the type it is read as is bigint, and the message says so
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "x"
-- end-expected-error
SELECT 1 UNION ALL SELECT 2 ORDER BY 1 OFFSET 'x';

-- note: a fractional count is cast to bigint, so it rounds rather than truncating
-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM sos_t ORDER BY 1 LIMIT 1.5;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT a FROM sos_t ORDER BY 1 LIMIT 0.5;

-- note: and the sign is judged after the rounding, so this is a limit of zero and not a negative one
-- begin-expected
-- columns: a
-- end-expected
SELECT a FROM sos_t ORDER BY 1 LIMIT -0.4;

-- begin-expected
-- columns: a
-- row: 2
-- row: 3
-- end-expected
SELECT a FROM sos_t ORDER BY 1 OFFSET 0.5;

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT a FROM sos_t ORDER BY 1 OFFSET 1.5;

-- begin-expected
-- columns: a
-- row: 2
-- row: 3
-- end-expected
SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY 1 OFFSET 1.5;

-- ============================================================================
-- 3. A bare constant in a FROM-less SELECT's ORDER BY
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT 1 AS a ORDER BY NULL;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT 1 AS a ORDER BY 'x';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT 1 AS a ORDER BY 1.5;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT 1 AS a ORDER BY true;

-- note: parentheses do not make an expression of a constant
-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT 1 AS a ORDER BY (NULL);

-- note: an integer constant there is still a position, and still has to be in the select list
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY position 5 is not in select list
-- end-expected-error
SELECT 1 AS a ORDER BY 5;

-- note: a set-returning target expands to rows and orders them, and the same rule applies
-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in ORDER BY
-- end-expected-error
SELECT generate_series(1, 3) AS g ORDER BY NULL;

-- ============================================================================
-- 4. A scalar subquery may have one column, whatever it returns
-- ============================================================================

-- note: no row to read the width off, and the width is still wrong
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT (SELECT 1, 2 WHERE false);

-- note: the width is settled before the row count, so this is a width error and not "more than one row"
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT (SELECT a, b FROM sos_t);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT (SELECT * FROM sos_t WHERE false);

-- note: ARRAY collects one column, so a second one has nowhere to go
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT ARRAY(SELECT 1, 2);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT ARRAY(SELECT 1, 2 WHERE false);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT ARRAY(SELECT a, b FROM sos_t);

-- note: the same in every position a scalar subquery can stand in -- WHERE
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT * FROM sos_t WHERE a = (SELECT 1, 2 WHERE false);

-- note: an operand
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT 1 + (SELECT 1, 2 WHERE false);

-- note: a function argument
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT abs((SELECT 1, 2 WHERE false));

-- note: a CASE result
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT CASE WHEN true THEN (SELECT 1, 2 WHERE false) ELSE 0 END;

-- note: an IN list entry
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT 1 IN ((SELECT 1, 2 WHERE false), 3);

-- note: and a null test
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT (SELECT 1, 2 WHERE false) IS NULL;

-- note: a DEFAULT takes no subquery at all, of any width
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in DEFAULT expression
-- end-expected-error
CREATE TABLE sos_d (x int DEFAULT (SELECT 1, 2));

-- ============================================================================
-- 5. A row constructor compared against a subquery reads the whole row
-- ============================================================================

-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT ((1, 2) = (SELECT 1, 2))::text;

-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT ((1, 2) <> (SELECT 1, 3))::text;

-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT ((1, 2) < (SELECT 1, 3))::text;

-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT ((1, 2) <= (SELECT 1, 2))::text;

-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT ((1, 2) >= (SELECT 1, 2))::text;

-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT ((1, 2) > (SELECT 1, 1))::text;

-- note: ROW(...) written out is the same constructor
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (ROW(1, 2) = (SELECT 1, 2))::text;

-- note: a row of one is a row too, and a one-column subquery matches it
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (ROW(1) = (SELECT 1))::text;

-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (ROW('x') = (SELECT b FROM sos_t WHERE a = 1))::text;

-- note: the entries may come from the query's own rows on either side
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT ((x, y) = (SELECT 1, 2))::text FROM sos_p;

-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT ((1, 'x') = (SELECT a, b FROM sos_t WHERE a = 1))::text;

-- note: and it is a boolean like any other, so it stands in WHERE and under NOT
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT 1 WHERE (1, 2) = (SELECT 1, 2);

-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT (NOT ((1, 2) = (SELECT 1, 3)))::text;

-- note: no row makes the comparison unknown rather than false
-- begin-expected
-- columns: text
-- row: NULL
-- end-expected
SELECT ((1, 2) = (SELECT 1, 2 LIMIT 0))::text;

-- note: the widths have to agree, and that is settled before any row is read
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery has too few columns
-- end-expected-error
SELECT ((1, 2) = (SELECT 1))::text;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery has too few columns
-- end-expected-error
SELECT ((1, 2, 3) = (SELECT 1, 2))::text;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery has too few columns
-- end-expected-error
SELECT ((1, 2, 3) = (SELECT x, y FROM sos_p WHERE false))::text;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery has too many columns
-- end-expected-error
SELECT ((1, 2) = (SELECT 1, 2, 3))::text;

-- note: entry by entry the comparison has to have an operator, and that is settled first too
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = text
-- end-expected-error
SELECT ((1, 2) = (SELECT a, b FROM sos_t WHERE a = 1))::text;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = text
-- end-expected-error
SELECT ((1, 2) = (SELECT a, b FROM sos_t WHERE false))::text;

-- note: the widths agree here, so what is left is the row count
-- begin-expected-error
-- sqlstate: 21000
-- message-like: more than one row returned by a subquery used as an expression
-- end-expected-error
SELECT ((1, 2) = (SELECT a, a FROM sos_t))::text;

-- note: IS DISTINCT FROM does not take the row reading -- measured, not assumed
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT ((1, 2) IS DISTINCT FROM (SELECT 1, 2))::text;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT ((1, 2) IS NOT DISTINCT FROM (SELECT 1, 2))::text;

-- note: and neither does a subquery written on the left of the comparison
-- begin-expected-error
-- sqlstate: 42601
-- message-like: subquery must return only one column
-- end-expected-error
SELECT ((SELECT 1, 2) = (1, 2))::text;

-- ============================================================================
-- 6. Ordinary SQL, which has to keep working
-- ============================================================================

-- note: an output column name, and its position, ordered either way
-- begin-expected
-- columns: x
-- row: 1
-- row: 2
-- row: 2
-- row: 3
-- end-expected
SELECT a AS x FROM sos_t UNION ALL SELECT 2 ORDER BY x;

-- begin-expected
-- columns: x
-- row: 1
-- row: 2
-- row: 2
-- row: 3
-- end-expected
SELECT a AS x FROM sos_t UNION ALL SELECT 2 ORDER BY 1;

-- begin-expected
-- columns: x
-- row: 3
-- row: 2
-- row: 2
-- row: 1
-- end-expected
SELECT a AS x FROM sos_t UNION ALL SELECT 2 ORDER BY 1 DESC;

-- begin-expected
-- columns: x
-- row: NULL
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT a AS x FROM sos_t UNION ALL SELECT NULL ORDER BY 1 NULLS FIRST;

-- begin-expected
-- columns: x
-- row: 3
-- row: 2
-- row: 1
-- row: NULL
-- end-expected
SELECT a AS x FROM sos_t UNION ALL SELECT NULL ORDER BY x DESC NULLS LAST;

-- note: a quoted output name, and one that came from an aggregate
-- begin-expected
-- columns: Weird Name
-- row: 1
-- row: 2
-- row: 2
-- row: 3
-- end-expected
SELECT a AS "Weird Name" FROM sos_t UNION ALL SELECT 2 ORDER BY "Weird Name";

-- begin-expected
-- columns: s
-- row: 2
-- row: 6
-- end-expected
SELECT sum(a) AS s FROM sos_t UNION ALL SELECT 2 ORDER BY s;

-- note: several items, by name and by position
-- begin-expected
-- columns: a | b
-- row: 2, q
-- row: 1, x
-- row: 2, y
-- row: 3, z
-- end-expected
SELECT a, b FROM sos_t UNION ALL SELECT 2, 'q' ORDER BY b, a;

-- begin-expected
-- columns: a | b
-- row: 2, q
-- row: 1, x
-- row: 2, y
-- row: 3, z
-- end-expected
SELECT a, b FROM sos_t UNION ALL SELECT 2, 'q' ORDER BY 2, 1;

-- note: USING names the operator to sort by and is accepted here
-- begin-expected
-- columns: a
-- row: 3
-- row: 2
-- row: 2
-- row: 1
-- end-expected
SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY 1 USING >;

-- note: the other two operations, and a VALUES arm
-- begin-expected
-- columns: a
-- row: 1
-- row: 3
-- end-expected
SELECT a FROM sos_t EXCEPT SELECT 2 ORDER BY a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT a FROM sos_t INTERSECT SELECT 2 ORDER BY a DESC;

-- begin-expected
-- columns: column1
-- row: 1
-- row: 2
-- row: 3
-- end-expected
VALUES (1), (2) UNION SELECT 3 ORDER BY 1;

-- note: chains, parenthesised or not
-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT a FROM sos_t UNION ALL (SELECT 2 UNION ALL SELECT 4) ORDER BY a;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 2
-- row: 3
-- row: 9
-- end-expected
(SELECT a FROM sos_t UNION ALL SELECT 2) UNION ALL SELECT 9 ORDER BY a;

-- note: with LIMIT and OFFSET after it, in either spelling
-- begin-expected
-- columns: a
-- row: 2
-- row: 2
-- end-expected
SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY 1 LIMIT 2 OFFSET 1;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY 1 FETCH FIRST 2 ROWS ONLY;

-- note: inside a CTE and inside a sub-select
-- begin-expected
-- columns: a
-- row: 2
-- row: 3
-- end-expected
WITH c AS (SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY a DESC LIMIT 2) SELECT * FROM c ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT * FROM (SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY a LIMIT 2) s ORDER BY 1;

-- note: a FROM-less SELECT ordered by a position, a name and an expression over a constant
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT 1 AS a ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT 1 AS a ORDER BY a DESC;

-- begin-expected
-- columns: a | b
-- row: 1, 2
-- end-expected
SELECT 1 AS a, 2 AS b ORDER BY 2;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT 1 AS a ORDER BY 1 + 0;

-- note: a constant that is cast is an expression and not a constant
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT 1 AS a ORDER BY NULL::int;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT 1 AS a ORDER BY 'x'::text;

-- begin-expected
-- columns: g
-- row: 3
-- row: 2
-- row: 1
-- end-expected
SELECT generate_series(1, 3) AS g ORDER BY 1 DESC;

-- note: one-column scalar subqueries, in the shapes the width rule must not touch
-- begin-expected
-- columns: ok
-- row: 1
-- end-expected
SELECT (SELECT 1) AS ok;

-- begin-expected
-- columns: max
-- row: 3
-- end-expected
SELECT (SELECT max(a) FROM sos_t);

-- begin-expected
-- columns: array
-- row: {1,2,3}
-- end-expected
SELECT ARRAY(SELECT a FROM sos_t ORDER BY a);

-- begin-expected
-- columns: array
-- row: {}
-- end-expected
SELECT ARRAY(SELECT a FROM sos_t WHERE false);

-- note: a one-column subquery that returns many rows is still a row-count error, not a width one
-- begin-expected-error
-- sqlstate: 21000
-- message-like: more than one row returned by a subquery used as an expression
-- end-expected-error
SELECT (SELECT a FROM sos_t);

-- note: IN over a two-column subquery is a row comparison of its own and always was
-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT ((1, 2) IN (SELECT 1, 2))::text;

-- begin-expected
-- columns: text
-- row: false
-- end-expected
SELECT ((1, 2) IN (SELECT a, a FROM sos_t))::text;

-- note: a set operation is an INSERT source like any other
INSERT INTO sos_p SELECT 5, 6 UNION ALL SELECT 7, 8 ORDER BY 1;

-- begin-expected
-- columns: x
-- row: 1
-- row: 5
-- row: 7
-- end-expected
SELECT x FROM sos_p ORDER BY 1;

-- cleanup
DROP TABLE IF EXISTS sos_p CASCADE;
DROP TABLE IF EXISTS sos_t CASCADE;
