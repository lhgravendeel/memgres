-- ============================================================================
-- What narrows a LATERAL item, in the two regimes PostgreSQL runs one under
--
-- Pulled up into the query reading it, the item is an ordinary join, and a
-- constant reaches the item's own scan only through an equivalence class:
-- beside (SELECT * FROM g z WHERE z.a = o.a) read as s, o.a = 5 says s.a = 5,
-- while o.note = 'x' says nothing about s at all. An OR says only what every
-- one of its branches says, so branches that are the same comparison put one
-- value in the class and branches naming different constants put none.
--
-- A LIMIT or an OFFSET written inside the item stops that pullup. PostgreSQL
-- then leaves the item a query of its own and runs it once per row of the
-- relation beside it, after that relation's own scan filter -- and then any
-- restriction on that relation is enough, including one no equality carries
-- into the item.
--
-- The generation expression below is 10/a over a relation holding a row with
-- a = 0, so every statement here is really asking which rows it was worked out
-- for. Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE lnr_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO lnr_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE lnr_o (a int, note text);
INSERT INTO lnr_o VALUES (5,'x'),(0,'y');
CREATE TABLE lnr_o1 (a int, note text);
INSERT INTO lnr_o1 VALUES (5,'x');
CREATE TABLE lnr_o3 (a int, note text);
INSERT INTO lnr_o3 VALUES (5,'x'),(0,'y'),(0,'z');
CREATE TABLE lnr_u (a int, note text);
INSERT INTO lnr_u VALUES (5,'x'),(0,'y');

-- ============================================================================
-- Pulled up: only what an equality carries into the item's own scan
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- nothing carries a constant to the item here, so its expression is worked out
-- for every row it holds -- including the row a is zero in
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- an inner join reads the same way, in either spelling
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o INNER JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o INNER JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o CROSS JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s WHERE o.a = 5 AND s.g = 2;

-- both parts of an AND reach the scan, whichever is written first
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 AND o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x' AND o.a = 5;

-- ============================================================================
-- What an OR says, and what it does not
-- ============================================================================

-- an OR whose branches are the same comparison is that comparison
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a = 5;

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT s.k FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE (o.a = 5 OR o.a = 5) OR o.a = 5;

-- the comma spelling of the same item, with the join condition in the WHERE
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o, LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s WHERE (o.a = 5 OR o.a = 5) AND s.g = 2;

-- what the branches have in common is what the OR says, however each branch is
-- written out and in whichever order
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE (o.a = 5 AND o.note = 'x') OR (o.a = 5 AND o.note = 'q');

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE (o.a = 5 AND o.note = 'x') OR (o.note = 'q' AND o.a = 5);

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR (o.a = 5 AND o.note = 'x');

-- branches that compare the same column with different values put nothing in a
-- class, so the item is not narrowed at all
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a = 6;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a = 0;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a IN (5) OR o.a IN (6);

-- nor does a branch that is not the same comparison at all
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a > 4;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a IS NULL;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.note = 'x';

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x' OR o.note = 'x';

-- what the OR says does not depend on how many rows the relation beside the
-- item holds: one row
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o1 o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o1 o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- and three
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o3 o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o3 o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a IN (5) OR o.a IN (5);

-- ============================================================================
-- A one-element IN is the equality PostgreSQL reads it as
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a IN (5);

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a IN (5) AND o.note = 'x';

-- the constant written on the other side of the comparison says the same
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE 5 = o.a;

-- a longer list is a comparison against an array, which says nothing about one
-- value; nor does NOT IN, nor the ANY spelling
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a IN (5, 6);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a NOT IN (5);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = ANY (ARRAY[5]);

-- the same one-element list wherever a restriction reaches a scan: a relation
-- read on its own, and a derived table
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_g s WHERE s.a IN (5);

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_g s WHERE s.a IN (5) AND s.g = 2;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM lnr_g) s WHERE s.a IN (5);

-- ============================================================================
-- The item's own comparison, written without its relation, and a qualified
-- star in its select list
-- ============================================================================

-- an unqualified name is resolved against the relation nearest it
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o, LATERAL (SELECT * FROM lnr_g z WHERE a = o.a) s WHERE 5 = o.a AND s.g = 2;

-- a star written with the relation before it exposes what a bare one does
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT z.* FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o, LATERAL (SELECT z.* FROM lnr_g z WHERE z.a = o.a) s WHERE o.a IN (5) AND s.g = 2;

-- begin-expected
-- columns: k | g
-- row: five | 2
-- end-expected
SELECT s.k, s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT z.* FROM lnr_g z WHERE z.a = o.a) s ON true WHERE o.a = 5;

-- ============================================================================
-- Not pulled up: a LIMIT or an OFFSET inside the item
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON s.g = 2 WHERE o.a = 5;

-- the restriction no equality carries into the item is now enough
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a OFFSET 0) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a OFFSET 0 LIMIT 1) s ON s.g = 2 WHERE o.note = 'x';

-- and so is a comparison that is not an equality at all
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON s.g = 2 WHERE o.a > 4;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON s.g = 2 WHERE o.note <> 'y';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a OFFSET 0) s ON s.g = 2 WHERE o.a > 4;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON s.g = 2 WHERE o.a IN (5);

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON s.g = 2 WHERE o.note = 'x' OR o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o, LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s WHERE o.note = 'x' AND s.g = 2;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o CROSS JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s WHERE o.note = 'x' AND s.g = 2;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT z.* FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON s.g = 2 WHERE o.note = 'x';

-- with nothing restricting the relation beside it, the item is run for every
-- row of that relation
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON s.g = 2;

-- a restriction that holds of both rows restricts neither of them away
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON s.g = 2 WHERE o.note = 'x' OR o.note = 'y';

-- and one that is not a restriction on that relation on its own says nothing
-- about which of its rows the item is run for
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON s.g = 2 WHERE o.note = 'x' OR s.g = 2;

-- ============================================================================
-- What the query does keep is still worked out
-- ============================================================================

-- begin-expected
-- columns: a | k | g
-- row: 5 | five | 2
-- end-expected
SELECT s.a, s.k, s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON true WHERE o.a = 5;

-- begin-expected
-- columns: a | g
-- row: 5 | 2
-- end-expected
SELECT o.a, s.g FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a > 0 ORDER BY z.a LIMIT 1) s ON true WHERE o.note = 'x';

-- begin-expected
-- columns: a | g
-- row: 5 | 2
-- end-expected
SELECT o.a, s.g FROM lnr_o o, LATERAL (SELECT * FROM lnr_g z WHERE z.a = 5 LIMIT 1) s WHERE o.note = 'x';

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON true WHERE o.note = 'x';

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM lnr_o o LEFT JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- the row beside the item is the padded side of a join above it: an output row
-- either carries that row, and the restriction is false of it, or does not
-- carry it at all
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lnr_o q LEFT JOIN (lnr_o o JOIN LATERAL (SELECT * FROM lnr_g z WHERE z.a = o.a LIMIT 1) s ON s.g = 2) ON q.a = o.a WHERE o.note = 'x';

-- ============================================================================
-- The same class of restriction, on a statement that writes
-- ============================================================================

-- the equalities put lnr_g's a, lnr_u's a and 5 in one class, so the
-- restriction reaches the extra relation's own scan and its expression is
-- never worked out for the row a is zero in
UPDATE lnr_u SET note = 'w' FROM lnr_g t WHERE t.a = lnr_u.a AND lnr_u.a IN (5) AND t.g = 2;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- row: 5 | w
-- end-expected
SELECT a, note FROM lnr_u ORDER BY a;

DELETE FROM lnr_o USING lnr_g t WHERE t.a = lnr_o.a AND lnr_o.a IN (5) AND t.g = 2;

-- begin-expected
-- columns: a | note
-- row: 0 | y
-- end-expected
SELECT a, note FROM lnr_o ORDER BY a;

-- cleanup
DROP TABLE lnr_u;
DROP TABLE lnr_o3;
DROP TABLE lnr_o1;
DROP TABLE lnr_o;
DROP TABLE lnr_g;
