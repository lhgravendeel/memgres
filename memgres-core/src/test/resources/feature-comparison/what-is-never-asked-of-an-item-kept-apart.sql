-- ============================================================================
-- What is never asked of a WITH item the query above keeps apart
--
-- An item PostgreSQL keeps apart from the query reading it -- written
-- MATERIALIZED, named more than once, holding a volatile call -- is computed in
-- full when that query first asks it for a row. A query whose qualification
-- cannot admit a row never asks at all, and then the item does not run: a
-- VIRTUAL generated column of the relation under it is never worked out, and a
-- generation expression that raises for one of its rows raises nothing.
--
-- Whether the item is asked is settled by the whole qualification rather than
-- by any one part of it. The parts must all hold, so one of them false before a
-- row is read leaves the rest nothing to decide; what stands under an OR is not
-- such a part, because what it says is read beside what the other side says,
-- row by row. A HAVING clause drops the groups a query answers with as surely
-- as a WHERE drops the rows they are made of, and an inner join's condition
-- decides the whole clause. An outer join's does not: the side it preserves is
-- answered whatever the condition says, and only the other side is never asked.
--
-- Nothing under a query that is never asked runs either, so a chain of items
-- kept apart is unread from the top down, however long it is and whichever
-- links of it PostgreSQL would otherwise pull up.
--
-- The generation expression below is 10/a over a relation holding a row with
-- a = 0, so every statement here is really asking whether the item ran at all.
-- Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE nak_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO nak_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE nak_o (a int, note text);
INSERT INTO nak_o VALUES (5,'x'),(0,'y');
CREATE TABLE nak_w (i int, s text);

-- ============================================================================
-- An item the query asks for a row is computed in full
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE c.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE true;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE true AND c.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c LIMIT 1;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c OFFSET 1;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c(p,q,r) AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c;

-- ============================================================================
-- A qualification that cannot admit a row leaves the item unasked
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE false;

-- a constant-false part beside a row-by-row one still stops the item being
-- asked: the parts must all hold, so the rest have nothing left to decide
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE false AND c.a = 5;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE c.a = 5 AND false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE c.a = 5 AND false AND c.k = 'five';

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE c.a = 5 AND c.k = 'five' AND false;

-- the generated column itself is no more asked for than any other
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE false AND c.g = 2;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE c.g = 2 AND false;

-- under OR it is not such a part: what it says is read beside what the other
-- side says, row by row, so the item is asked
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE false OR c.a = 5;

-- however the constant is spelled
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE 1 = 0;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE 1 = 2;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE NULL;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE NOT true;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE (false);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE NOT (1 = 1);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE false AND false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE 1 = 0 AND c.a = 5;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE NULL AND c.a = 5;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE NOT true AND c.a = 5;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE false AND (c.a = 5 OR c.a = 0);

-- a row count written above it changes nothing about what the qualification
-- admits
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE false LIMIT 1;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE false OFFSET 0;

-- and an aggregate over no rows at all answers for none of them
-- begin-expected
-- columns: max
-- row: NULL
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT max(c.a) FROM c WHERE false;

-- begin-expected
-- columns: a | k
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT c.a, c.k FROM c WHERE false;

-- a row count of none is a query that asks for nothing
-- begin-expected
-- columns: a
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT a FROM c LIMIT 0;

-- the names the item or the FROM item gives its columns change nothing
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c s(p,q,r) WHERE false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c(p,q,r) AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c(p,q,r) AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE false AND p = 5;

-- ============================================================================
-- An item named twice is kept apart, and is asked by the whole query
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS (SELECT * FROM nak_g) SELECT count(*) FROM c x, c y;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS (SELECT * FROM nak_g) SELECT count(*) FROM c x, c y WHERE false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS (SELECT * FROM nak_g) SELECT count(*) FROM c x, c y WHERE false AND x.a = 5;

-- a select list is read only for the rows the WHERE let through, so the
-- reference in it asks for a row no more than the one in the FROM clause does
-- begin-expected
-- columns: count
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT (SELECT count(*) FROM c) FROM c WHERE false;

-- ============================================================================
-- Nothing under a query that is never asked runs either
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g), d AS MATERIALIZED (SELECT * FROM c) SELECT count(*) FROM d;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g), d AS MATERIALIZED (SELECT * FROM c) SELECT count(*) FROM d WHERE false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g), d AS MATERIALIZED (SELECT * FROM c) SELECT count(*) FROM d WHERE false AND d.a = 5;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g), d AS MATERIALIZED (SELECT * FROM c) SELECT count(*) FROM d WHERE d.a = 5 AND false;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g), d AS MATERIALIZED (SELECT * FROM c) SELECT count(*) FROM d WHERE false OR d.a = 5;

-- begin-expected
-- columns: a
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g), d AS MATERIALIZED (SELECT * FROM c) SELECT a FROM d LIMIT 0;

-- however long the chain is
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g), d AS MATERIALIZED (SELECT * FROM c), e AS MATERIALIZED (SELECT * FROM d) SELECT count(*) FROM e;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g), d AS MATERIALIZED (SELECT * FROM c), e AS MATERIALIZED (SELECT * FROM d) SELECT count(*) FROM e WHERE false;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g), d AS MATERIALIZED (SELECT * FROM c), e AS MATERIALIZED (SELECT * FROM d), f AS MATERIALIZED (SELECT * FROM e) SELECT count(*) FROM f;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g), d AS MATERIALIZED (SELECT * FROM c), e AS MATERIALIZED (SELECT * FROM d), f AS MATERIALIZED (SELECT * FROM e) SELECT count(*) FROM f WHERE false;

-- and whichever links of it PostgreSQL would otherwise pull up
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g), d AS NOT MATERIALIZED (SELECT * FROM c) SELECT count(*) FROM d;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g), d AS NOT MATERIALIZED (SELECT * FROM c) SELECT count(*) FROM d WHERE false;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS NOT MATERIALIZED (SELECT * FROM nak_g), d AS MATERIALIZED (SELECT * FROM c) SELECT count(*) FROM d;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS NOT MATERIALIZED (SELECT * FROM nak_g), d AS MATERIALIZED (SELECT * FROM c) SELECT count(*) FROM d WHERE false;

-- a sub-select over the item, qualified inside it and outside it
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT * FROM c WHERE false) x;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT * FROM c) x WHERE false;

-- ============================================================================
-- What else settles that the query answers without asking
-- ============================================================================

-- a HAVING clause drops the groups the query would answer with
-- begin-expected
-- columns: count
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c HAVING false;

-- begin-expected
-- columns: count
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c HAVING false AND count(*) > 0;

-- begin-expected
-- columns: a | count
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT c.a, count(*) FROM c GROUP BY c.a HAVING false;

-- and one that keeps them asks as surely as a WHERE that keeps the row
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c HAVING true;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c HAVING count(*) > 99;

-- an inner join's condition decides the whole clause
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c JOIN nak_o o ON false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c JOIN nak_o o ON c.a = o.a AND false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c CROSS JOIN nak_o o WHERE false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c, nak_o o WHERE o.a = 99 AND false;

-- an outer join's does not, for the side it preserves
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM nak_o o LEFT JOIN c ON false;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c LEFT JOIN nak_o o ON false;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM nak_o o RIGHT JOIN c ON false;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c FULL JOIN nak_o o ON false;

-- ============================================================================
-- An item that writes runs however the query above is answered
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH ins AS (INSERT INTO nak_w VALUES (1,'a') RETURNING *) SELECT count(*) FROM ins WHERE false;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT i FROM nak_w ORDER BY i;

-- begin-expected
-- columns: i | s
-- end-expected
WITH ins AS (INSERT INTO nak_w VALUES (2,'b') RETURNING *) SELECT * FROM ins LIMIT 0;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT i FROM nak_w ORDER BY i;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH del AS (DELETE FROM nak_w WHERE i = 2 RETURNING *) SELECT count(*) FROM del WHERE false;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT i FROM nak_w ORDER BY i;

WITH c AS MATERIALIZED (SELECT * FROM nak_g) INSERT INTO nak_w SELECT a, k FROM c WHERE false;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT i FROM nak_w ORDER BY i;

-- ============================================================================
-- A relation that is not a WITH item reads the same way
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM nak_g WHERE false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM nak_g WHERE false AND a = 5;

-- count(*) names no column of it, so the expression is not reached even here
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM nak_g;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT * FROM nak_g) s WHERE false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM (SELECT * FROM nak_g) s WHERE false AND s.a = 5;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT * FROM nak_g) s;

CREATE VIEW nak_v AS SELECT * FROM nak_g;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM nak_v WHERE false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM nak_v WHERE false AND a = 5;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM nak_v;

-- ============================================================================
-- An item whose own body answers nothing, and a recursive one
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g WHERE false) SELECT count(*) FROM c;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g LIMIT 0) SELECT count(*) FROM c;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH RECURSIVE c AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM c WHERE n < 3) SELECT count(*) FROM c WHERE false;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
WITH RECURSIVE c AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM c WHERE n < 3) SELECT count(*) FROM c;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g UNION ALL SELECT * FROM nak_g) SELECT count(*) FROM c WHERE false;

-- an arm that never asks leaves the item unread, and the other arm reads
-- nothing of it
-- begin-expected
-- columns: count
-- row: 0
-- row: 2
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM nak_g) SELECT count(*) FROM c WHERE false UNION ALL SELECT count(*) FROM nak_o;

-- cleanup
DROP VIEW nak_v;
DROP TABLE nak_w;
DROP TABLE nak_o;
DROP TABLE nak_g;
