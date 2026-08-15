-- ============================================================================
-- What else keeps a LATERAL item apart, and what a pulled-up one is narrowed by
--
-- Pulled up into the query reading it, an item stops being a query of its own:
-- the relation it stands in front of is one of that query's own relations and
-- is scanned once. A qualification speaking of the item alone is then a filter
-- on that scan and is read for every row the scan visits, while the item's
-- comparison with the row beside it says which rows pair rather than which rows
-- there are. So the scan reaches every row of the relation underneath, however
-- few rows the relation beside it happens to hold. What does narrow it is a
-- constant an equivalence class carries onto the relation, a comparison the
-- item makes with anything but the row beside it, and a join qualification
-- about the item alone.
--
-- An OR restricts a scan only by what every one of its branches says, and two
-- branches are the same branch when they are the same comparison: PostgreSQL
-- reads x IN (5) as x = 5 while it reads the statement, so a one-element list
-- stands beside the written equality, while 5 = x is written the other way
-- round and stands beside neither.
--
-- Whether the item is pulled up at all is settled by what it holds. A LIMIT or
-- an OFFSET is not the only thing that stops it: anything settling which of the
-- item's rows there are, or in what order, before the query above is planned
-- keeps the two apart -- a DISTINCT, a sort, a grouping, a set operation, a
-- locking clause, a WITH item of its own, and a window call, a set-returning
-- call or a volatile one in its select list. Such an item is run once per row
-- of the relation beside it, after that relation's own scan filter, and not at
-- all for a row that filter discards.
--
-- An alias list on the item renames the columns it exposes one for one from the
-- left, and a comparison written inside the item speaks above under the name
-- standing where its column stands.
--
-- The generation expression below is 10/a over a relation holding a row with
-- a = 0, so every statement here is really asking which rows it was worked out
-- for. Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE lqp_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO lqp_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE lqp_o (a int, note text);
INSERT INTO lqp_o VALUES (5,'x'),(0,'y');
CREATE TABLE lqp_o1 (a int, note text);
INSERT INTO lqp_o1 VALUES (5,'x');
CREATE TABLE lqp_u (a int, note text);
INSERT INTO lqp_u VALUES (5,'x'),(0,'y');

-- ============================================================================
-- A pulled-up item is read over the whole relation it stands in front of
-- ============================================================================

-- one row stands beside the item, and it is the row that pairs with a = 5; the
-- scan under the item still visits the row a is zero in
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.k FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT count(*) FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o1 o, LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s WHERE o.note = 'x' AND s.g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o1 o CROSS JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s WHERE s.g = 2;

-- the item's own comparison reads the same written the other way round, and
-- whatever the operator
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE o.a = z.a) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a > o.a) s ON s.g = 2 WHERE o.note = 'x';

-- an item read beside another item, which is a row beside it just the same
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s2.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON true LEFT JOIN LATERAL (SELECT * FROM lqp_g z2 WHERE z2.a = s.a) s2 ON s2.g = 2;

-- the same shapes over a relation of two rows
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2;

-- ============================================================================
-- What does narrow a pulled-up item's own scan
-- ============================================================================

-- a constant an equivalence class carries onto the relation, beside one row and
-- beside two
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- a join qualification about the item alone, in either order
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.a = 5 AND s.g = 2;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 AND s.a = 5;

-- a comparison the item makes with anything but the row beside it
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a AND z.k = 'five') s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a AND z.a = 5) s ON s.g = 2 WHERE o.note = 'x';

-- an item that pairs with nothing at all is narrowed by its own comparisons
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = 5) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a > 0) s ON s.g = 2 WHERE o.note = 'x';

-- a reference in the select list is not a filter on that scan: it stands above
-- the join and is worked out for the rows the query kept
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON true WHERE o.note = 'x';

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT s.k FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON true WHERE o.note = 'x';

-- and a LIMIT keeps the item apart, where any restriction on the row beside it
-- is enough
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o1 o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a LIMIT 1) s ON s.g = 2 WHERE o.note = 'x';

-- ============================================================================
-- An OR whose branches are one comparison written two ways
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a IN (5);

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a IN (5) OR o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE (o.a = 5 AND o.note = 'x') OR o.a IN (5);

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT s.k FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a IN (5);

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a IN (5);

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o, LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s WHERE (o.a = 5 OR o.a IN (5)) AND s.g = 2;

-- branches naming different constants say nothing about one value, nor does a
-- one-element list beside a longer one, nor the comparison written the other
-- way round
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a IN (6);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a IN (5) OR o.a IN (5, 6);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR 5 = o.a;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5 OR o.a = 6;

-- the same reading wherever a restriction reaches a scan: a derived table, and
-- the extra relation of a statement that writes
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM (SELECT * FROM lqp_g) s WHERE (s.a = 5 OR s.a IN (5)) AND s.g = 2;

UPDATE lqp_u SET note = 'w' FROM lqp_g t WHERE t.a = lqp_u.a AND (lqp_u.a = 5 OR lqp_u.a IN (5)) AND t.g = 2;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- row: 5, w
-- end-expected
SELECT a, note FROM lqp_u ORDER BY a;

DELETE FROM lqp_u USING lqp_g t WHERE t.a = lqp_u.a AND (lqp_u.a = 5 OR lqp_u.a IN (5)) AND t.g = 2;

-- begin-expected
-- columns: a | note
-- row: 0, y
-- end-expected
SELECT a, note FROM lqp_u ORDER BY a;

-- ============================================================================
-- An item kept apart for a reason other than a LIMIT or an OFFSET
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT DISTINCT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a ORDER BY z.a) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT z.a, z.k, max(z.g) AS g FROM lqp_g z WHERE z.a = o.a GROUP BY z.a, z.k) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a GROUP BY z.a, z.k, z.g) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT z.a, z.k, max(z.g) AS g FROM lqp_g z WHERE z.a = o.a GROUP BY z.a, z.k HAVING count(*) > 0) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT DISTINCT ON (z.a) z.a, z.k, z.g FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- a WITH item of its own, written MATERIALIZED or not
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (WITH q AS MATERIALIZED (SELECT * FROM lqp_g z WHERE z.a = o.a) SELECT * FROM q) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (WITH q AS (SELECT * FROM lqp_g z WHERE z.a = o.a) SELECT * FROM q) s ON s.g = 2 WHERE o.note = 'x';

-- a set operation, and a UNION ALL answers with both arms' rows
-- begin-expected
-- columns: g
-- row: 2
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a UNION ALL SELECT * FROM lqp_g z2 WHERE z2.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a UNION SELECT * FROM lqp_g z2 WHERE z2.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a EXCEPT SELECT * FROM lqp_g z2 WHERE z2.a = 99) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a INTERSECT SELECT * FROM lqp_g z2 WHERE z2.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- a locking clause
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a FOR UPDATE) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a FOR SHARE) s ON s.g = 2 WHERE o.note = 'x';

-- a window call, a set-returning call or a volatile one in the select list
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT z.a, z.k, z.g, row_number() OVER () AS r FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT z.a, z.k, z.g, generate_series(1,1) AS r FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT z.a, z.k, z.g, random() AS r FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- the same item after a comma, and one narrowed by a constant instead
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o, LATERAL (SELECT DISTINCT * FROM lqp_g z WHERE z.a = o.a) s WHERE o.note = 'x' AND s.g = 2;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o CROSS JOIN LATERAL (SELECT DISTINCT * FROM lqp_g z WHERE z.a = o.a) s WHERE o.note = 'x' AND s.g = 2;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT DISTINCT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT DISTINCT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note <> 'y';

-- with nothing restricting the relation beside it, such an item is run for
-- every row of it -- and so it is for a restriction that holds of both rows,
-- and for one that is not a restriction on that relation alone
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT DISTINCT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a ORDER BY z.a) s ON s.g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a UNION ALL SELECT * FROM lqp_g z2 WHERE z2.a = o.a) s ON s.g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (WITH q AS MATERIALIZED (SELECT * FROM lqp_g z WHERE z.a = o.a) SELECT * FROM q) s ON s.g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT DISTINCT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x' OR o.note = 'y';

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT DISTINCT * FROM lqp_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x' OR s.g = 2;

-- what the query keeps is still answered beside an item nobody asked for a row
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM lqp_o o LEFT JOIN LATERAL (SELECT DISTINCT * FROM lqp_g z WHERE z.a = o.a) s ON true WHERE o.note = 'q';

-- begin-expected
-- columns: a | k
-- row: 5, five
-- end-expected
SELECT o.a, s.k FROM lqp_o o LEFT JOIN LATERAL (SELECT DISTINCT * FROM lqp_g z WHERE z.a = o.a) s ON true WHERE o.note = 'x';

-- begin-expected
-- columns: oa | sa | k | g
-- row: 5, 5, five, 2
-- end-expected
SELECT o.a AS oa, s.a AS sa, s.k, s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT DISTINCT * FROM lqp_g z WHERE z.a = o.a) s ON true WHERE o.note = 'x';

-- ============================================================================
-- An alias list on the item
-- ============================================================================

-- begin-expected
-- columns: w
-- row: 2
-- end-expected
SELECT s.w FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- begin-expected
-- columns: v
-- row: five
-- end-expected
SELECT s.v FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- begin-expected
-- columns: z
-- row: 2
-- end-expected
SELECT s.z FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s(x,y,z) ON s.z = 2 WHERE o.a IN (5);

-- a select list naming the columns rather than starring them
-- begin-expected
-- columns: w
-- row: 2
-- end-expected
SELECT s.w FROM lqp_o o LEFT JOIN LATERAL (SELECT z.a, z.k, z.g FROM lqp_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- begin-expected
-- columns: w
-- row: 2
-- end-expected
SELECT s.w FROM lqp_o o, LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s(u,v,w) WHERE o.a = 5 AND s.w = 2;

-- a list shorter than the relation leaves the columns past it their own names
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s(u,v) ON s.g = 2 WHERE o.a = 5;

-- the renamed column is still worked out for the rows the query keeps
-- begin-expected
-- columns: u | v | w
-- row: 5, five, 2
-- end-expected
SELECT s.u, s.v, s.w FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s(u,v,w) ON true WHERE o.a = 5;

-- with no constant carried onto the item the renaming changes nothing, and a
-- list on an item kept apart is read as it was
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.w FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: w
-- row: 2
-- end-expected
SELECT s.w FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a LIMIT 1) s(u,v,w) ON s.w = 2 WHERE o.note = 'x';

-- begin-expected
-- columns: w
-- row: 2
-- end-expected
SELECT s.w FROM lqp_o o LEFT JOIN LATERAL (SELECT DISTINCT * FROM lqp_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.note = 'x';

-- ============================================================================
-- What a qualification naming the column by itself still raises
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT g FROM lqp_g WHERE g = 2;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT g FROM lqp_g;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM lqp_o o LEFT JOIN LATERAL (SELECT * FROM lqp_g z WHERE z.a = o.a) s ON true;

-- cleanup
DROP TABLE lqp_u;
DROP TABLE lqp_o1;
DROP TABLE lqp_o;
DROP TABLE lqp_g;
