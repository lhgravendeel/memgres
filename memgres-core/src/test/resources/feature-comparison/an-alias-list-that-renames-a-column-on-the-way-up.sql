-- ============================================================================
-- An alias list that renames a column on the way up
--
-- An alias list on a FROM item renames the columns the item exposes one for one
-- from the left. What the query above calls a column is therefore read off the
-- PLACE the column stands in and off nothing else: not off what the item's own
-- select list happened to call it, not off whether the relation underneath is
-- stored or is a view, and not off how many places one of its columns stands
-- in. That matters because the name is what a restriction is carried up under,
-- and a restriction is what decides a row before a VIRTUAL generated column of
-- it is reached.
--
-- A join's condition restricts a scan too. An inner join answers the pairs its
-- condition holds of and no others; an outer join answers a row of the side it
-- preserves whatever the condition says, so nothing about that side is taken
-- from it, while a row of the OTHER side the condition rejects is paired with
-- nothing and carried by no row the query answers.
--
-- And a qualification settled against before a row is read leaves nothing under
-- it read at all. PostgreSQL settles more than a written constant that way: it
-- folds a call it declares IMMUTABLE while it plans, and it reads a
-- qualification naming no column of the query it stands in once, before the
-- scan under it is asked for anything.
--
-- The generation expression below is 10/a over a relation holding a row where a
-- is 0, so every statement here is really asking which rows it was worked out
-- for. Every value was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE sqa_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO sqa_g (a,k) VALUES (5,'five'),(0,'zero');
CREATE TABLE sqa_o (a int, note text);
INSERT INTO sqa_o VALUES (5,'x'),(0,'y');
CREATE TABLE sqa_o1 (a int, note text);
INSERT INTO sqa_o1 VALUES (5,'x');
CREATE VIEW sqa_v AS SELECT * FROM sqa_g;
CREATE VIEW sqa_v2 AS SELECT z.k, z.a, z.g FROM sqa_g z;

-- ============================================================================
-- The place a column stands in, read past the name the select list gave it
-- ============================================================================

-- begin-expected
-- columns: w
-- row: 2
-- end-expected
SELECT s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT z.a AS aa, z.k, z.g FROM sqa_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- begin-expected
-- columns: u | v | w
-- row: 5, five, 2
-- end-expected
SELECT s.u, s.v, s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT z.a AS aa, z.k, z.g FROM sqa_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- begin-expected
-- columns: v
-- row: five
-- end-expected
SELECT s.v FROM sqa_o o LEFT JOIN LATERAL (SELECT z.a AS aa, z.k, z.g FROM sqa_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- the same item written after a comma
-- begin-expected
-- columns: w
-- row: 2
-- end-expected
SELECT s.w FROM sqa_o o, LATERAL (SELECT z.a AS aa, z.k, z.g FROM sqa_g z WHERE z.a = o.a) s(u,v,w) WHERE o.a = 5 AND s.w = 2;

-- with nothing carrying a constant onto the item, the renaming changes nothing
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT z.a AS aa, z.k, z.g FROM sqa_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.note = 'x';

-- with no alias list over it, the select list's own name IS the name above
-- begin-expected
-- columns: aa
-- row: 5
-- end-expected
SELECT s.aa FROM sqa_o o LEFT JOIN LATERAL (SELECT z.a AS aa, z.k, z.g FROM sqa_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM sqa_o o LEFT JOIN LATERAL (SELECT z.a AS aa, z.k, z.g FROM sqa_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM sqa_o o LEFT JOIN LATERAL (SELECT z.a AS aa, z.k, z.g FROM sqa_g z WHERE z.a = o.a) s ON s.g = 2 WHERE o.note = 'x';

-- a select list that gives its columns each other's names is read by place too
-- begin-expected
-- columns: w
-- row: 2
-- end-expected
SELECT s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT z.k AS g, z.a AS k, z.g AS a FROM sqa_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- begin-expected
-- columns: u | v | w
-- row: five, 5, 2
-- end-expected
SELECT s.u, s.v, s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT z.k AS g, z.a AS k, z.g AS a FROM sqa_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- an item that reaches for nothing takes the constant through its condition
-- begin-expected
-- columns: w
-- row: 2
-- end-expected
SELECT s.w FROM sqa_o o LEFT JOIN (SELECT z.a AS aa, z.k, z.g FROM sqa_g z) s(u,v,w) ON s.u = o.a AND s.w = 2 WHERE o.a = 5;

-- ============================================================================
-- The place a column stands in is not read off a view any differently
-- ============================================================================

-- begin-expected
-- columns: w
-- row: 2
-- end-expected
SELECT s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT * FROM sqa_v z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- begin-expected
-- columns: u | v | w
-- row: 5, five, 2
-- end-expected
SELECT s.u, s.v, s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT * FROM sqa_v z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT * FROM sqa_v z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.note = 'x';

-- a view whose body puts the columns in another order puts them there above too
-- begin-expected
-- columns: w
-- row: 2
-- end-expected
SELECT s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT * FROM sqa_v2 z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- begin-expected
-- columns: u | v | w
-- row: five, 5, 2
-- end-expected
SELECT s.u, s.v, s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT * FROM sqa_v2 z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- ============================================================================
-- A column an item exposes twice stands in two places and carries one value
-- ============================================================================

-- begin-expected
-- columns: w
-- row: 2
-- end-expected
SELECT s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT z.a, z.a, z.g FROM sqa_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- begin-expected
-- columns: u | v | w
-- row: 5, 5, 2
-- end-expected
SELECT s.u, s.v, s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT z.a, z.a, z.g FROM sqa_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.a = 5;

-- begin-expected
-- columns: u
-- row: 5
-- end-expected
SELECT s.u FROM sqa_o o LEFT JOIN LATERAL (SELECT z.a, z.a, z.g FROM sqa_g z WHERE z.a = o.a) s(u,v,w) ON s.u = 5 WHERE o.a = 5;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.w FROM sqa_o o LEFT JOIN LATERAL (SELECT z.a, z.a, z.g FROM sqa_g z WHERE z.a = o.a) s(u,v,w) ON s.w = 2 WHERE o.note = 'x';

-- ============================================================================
-- What a join's condition says about the side the join does not preserve
-- ============================================================================

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM sqa_o1 o LEFT JOIN LATERAL (SELECT * FROM sqa_g z) s ON s.a = 5 AND s.g = 2;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM sqa_o1 o LEFT JOIN (SELECT * FROM sqa_g z) s ON s.a = 5 AND s.g = 2;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM sqa_o1 o LEFT JOIN sqa_g s ON s.a = 5 AND s.g = 2;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM sqa_o1 o JOIN sqa_g s ON s.a = 5 AND s.g = 2;

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM sqa_o1 o CROSS JOIN sqa_g s WHERE s.a = 5 AND s.g = 2;

-- begin-expected
-- columns: k
-- row: five
-- end-expected
SELECT s.k FROM sqa_o1 o LEFT JOIN sqa_g s ON s.a = 5 AND s.g = 2;

-- a part naming a plain column of that side restricts it as surely
-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT s.g FROM sqa_o1 o LEFT JOIN sqa_g s ON s.a = 5 AND s.k = 'five';

-- the preserved side still answers its row, padded, when nothing pairs with it
-- begin-expected
-- columns: note | g
-- row: x, NULL
-- end-expected
SELECT o.note, s.g FROM sqa_o1 o LEFT JOIN sqa_g s ON s.a = 9 AND s.g = 2;

-- a condition naming the preserved side alone says nothing about that side
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM sqa_o1 o LEFT JOIN sqa_g s ON o.note = 'q' AND s.a = 5;

-- a condition about the generated column alone restricts nothing
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM sqa_o1 o LEFT JOIN sqa_g s ON s.g = 2;

-- an OR restricts a scan only by what every one of its branches says
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM sqa_o1 o LEFT JOIN sqa_g s ON s.a = 5 OR s.k = 'zero';

-- the side a RIGHT JOIN preserves is the one nothing is taken about
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT s.g FROM sqa_o1 o RIGHT JOIN sqa_g s ON s.a = 5 AND s.g = 2;

-- ============================================================================
-- A qualification settled before a row is read
-- ============================================================================

-- a sub-select reading no relation is settled once, before the item under the
-- qualification is asked for anything -- and a MATERIALIZED item nothing asks a
-- row of is never computed at all
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE (SELECT false);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE (SELECT false) AND c.a = 5;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE (SELECT 1) = 2;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE EXISTS (SELECT 1 WHERE false);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE NOT EXISTS (SELECT 1);

-- one that settles true settles nothing, and the item is computed in full
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE (SELECT true);

-- a call PostgreSQL declares IMMUTABLE is folded while it plans, so a
-- qualification written out of one is settled as surely as a constant is
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE lower('A') = 'b';

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE lower('A') = 'b' OR false;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE abs(-1) = 2;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE length('abc') = 4;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE substr('abc',1,1) = 'z';

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE md5('a') = 'x';

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE 1 + 1 = 3;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE upper('a') = 'B' AND c.a = 5;

-- one that folds to true settles nothing
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH c AS MATERIALIZED (SELECT * FROM sqa_g) SELECT count(*) FROM c WHERE lower('A') = 'a';

-- the same settled qualification over a stored relation
-- begin-expected
-- columns: g
-- end-expected
SELECT g FROM sqa_g WHERE lower('A') = 'b';

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT g FROM sqa_g WHERE lower('A') = 'b' OR g = 2;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM sqa_g WHERE (SELECT false);

-- and over the two write paths that bring in a second relation
UPDATE sqa_o1 SET note = 'z' FROM sqa_g t WHERE t.a = sqa_o1.a AND lower('A') = 'b';

-- begin-expected
-- columns: a | note
-- row: 5, x
-- end-expected
SELECT a, note FROM sqa_o1 ORDER BY a;

DELETE FROM sqa_o1 USING sqa_g t WHERE t.a = sqa_o1.a AND (SELECT false);

-- begin-expected
-- columns: a | note
-- row: 5, x
-- end-expected
SELECT a, note FROM sqa_o1 ORDER BY a;

-- cleanup
DROP VIEW sqa_v2;
DROP VIEW sqa_v;
DROP TABLE sqa_o1;
DROP TABLE sqa_o;
DROP TABLE sqa_g;
