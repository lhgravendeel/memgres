-- ============================================================================
-- Rows matched by value, not by what they print
--
-- Two places pair rows off by value rather than by name: the sides of a join,
-- and the partitions a window is cut into. Both must decide equality the way
-- the = operator decides it. Printing a value and comparing the print is not
-- that rule -- a numeric 1.0 and a numeric 1.00 are one number written two
-- ways, and a character(n) is compared without the blanks its declaration
-- padded it out to.
--
-- A composite key is the same question asked of several values at once, so the
-- parts of one may not be run together in a way that lets a value carrying the
-- separator reach into its neighbour.
--
-- A join large enough to index its right side must find exactly what the same
-- join walked pair by pair would have found: an index decides how much work is
-- done, never which rows come back.
--
-- Parentheses around a join do not close it to a LATERAL item inside it, which
-- still reads the relations standing to the left of those parentheses -- while
-- an ordinary arm reaching out the same way is still refused.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE jk_one (n numeric(10,1), t text);
CREATE TABLE jk_two (n numeric(10,2), t text);
INSERT INTO jk_one VALUES (1.0, 'L'), (3.0, 'Lonly');
INSERT INTO jk_two VALUES (1.00, 'R'), (4.00, 'Ronly');
CREATE TABLE jk_nat_one (n numeric(10,1));
CREATE TABLE jk_nat_two (n numeric(10,2));
INSERT INTO jk_nat_one VALUES (1.0);
INSERT INTO jk_nat_two VALUES (1.00);
CREATE TABLE jk_c3 (n char(3), t text);
CREATE TABLE jk_c6 (n char(6), t text);
INSERT INTO jk_c3 VALUES ('a', 'L');
INSERT INTO jk_c6 VALUES ('a', 'R');
CREATE TABLE jk_wide_one (n numeric(10,1), i int);
CREATE TABLE jk_wide_two (n numeric(10,2), i int);
INSERT INTO jk_wide_one SELECT 1.0, g FROM generate_series(1,40) g;
INSERT INTO jk_wide_two SELECT 1.00, g FROM generate_series(1,40) g;
CREATE TABLE jk_uuid_one (u uuid, t text);
CREATE TABLE jk_uuid_two (u uuid, t text);
INSERT INTO jk_uuid_one SELECT '11111111-1111-1111-1111-111111111111'::uuid, g::text FROM generate_series(1,40) g;
INSERT INTO jk_uuid_two SELECT '11111111-1111-1111-1111-111111111111'::uuid, g::text FROM generate_series(1,40) g;
CREATE TABLE jk_part (n numeric(10,2));
INSERT INTO jk_part VALUES (1.0), (1.00), (2.0);
CREATE TABLE jk_sep (a text, b text);
INSERT INTO jk_sep VALUES ('a', 'b'), ('a' || chr(1) || 'b', '');
CREATE TABLE jk_lat_a (id int, v int);
CREATE TABLE jk_lat_b (id int, w int);
INSERT INTO jk_lat_a VALUES (1, 10), (2, 20);
INSERT INTO jk_lat_b VALUES (1, 100), (2, 200);

-- ============================================================================
-- The same number written to two scales is one key
-- ============================================================================

-- begin-expected
-- columns: n | t | t
-- row: 1.0 | L | R
-- end-expected
SELECT n, jk_one.t, jk_two.t FROM jk_one JOIN jk_two USING (n);

-- and a join spelling out the comparison finds it too
-- begin-expected
-- columns: t | t
-- row: L | R
-- end-expected
SELECT jk_one.t, jk_two.t FROM jk_one JOIN jk_two ON jk_one.n = jk_two.n;

-- as does one that named the column for itself
-- begin-expected
-- columns: n
-- row: 1.0
-- end-expected
SELECT n FROM jk_nat_one NATURAL JOIN jk_nat_two;

-- An outer join finds the match rather than padding its row out with nulls.
-- begin-expected
-- columns: n | t | t
-- row: 1.0 | L | R
-- row: 3.0 | Lonly | NULL
-- end-expected
SELECT n, jk_one.t, jk_two.t FROM jk_one LEFT JOIN jk_two USING (n) ORDER BY n;

-- The merged column answers with whichever side is not null, and a RIGHT join
-- asks the right side first -- that is the side whose rows are all kept. The
-- two values are equal and are not written alike, so which is asked shows.
-- begin-expected
-- columns: n | t | t
-- row: 1.00 | L | R
-- row: 4.00 | NULL | Ronly
-- end-expected
SELECT n, jk_one.t, jk_two.t FROM jk_one RIGHT JOIN jk_two USING (n) ORDER BY n;

-- Every other join type asks the left side first.
-- begin-expected
-- columns: n | t | t
-- row: 1.0 | L | R
-- row: 3.0 | Lonly | NULL
-- row: 4.00 | NULL | Ronly
-- end-expected
SELECT n, jk_one.t, jk_two.t FROM jk_one FULL JOIN jk_two USING (n) ORDER BY n;

-- which is a matter of the join type and not of the order the tables are written in
-- begin-expected
-- columns: n
-- row: 1.00
-- end-expected
SELECT n FROM jk_two JOIN jk_one USING (n) WHERE n = 1;

-- begin-expected
-- columns: n
-- row: 1.0
-- end-expected
SELECT n FROM jk_two RIGHT JOIN jk_one USING (n) WHERE n = 1;

-- ============================================================================
-- A character(n) is compared without its padding
-- ============================================================================

-- begin-expected
-- columns: t | t
-- row: L | R
-- end-expected
SELECT jk_c3.t, jk_c6.t FROM jk_c3 JOIN jk_c6 USING (n);

-- begin-expected
-- columns: t | t
-- row: L | R
-- end-expected
SELECT jk_c3.t, jk_c6.t FROM jk_c3 JOIN jk_c6 ON jk_c3.n = jk_c6.n;

-- Ignoring the padding to compare does not change what the merged column is:
-- it is still a character, and still reports the length a character reports.
-- begin-expected
-- columns: pg_typeof | length
-- row: character | 1
-- end-expected
SELECT pg_typeof(n)::text, length(n) FROM jk_c3 JOIN jk_c6 USING (n);

-- ============================================================================
-- A join big enough to index finds what the walked one finds
-- ============================================================================

-- Forty rows each side is sixteen hundred pairs, past the point where the join
-- indexes its right side instead of taking every pair in turn.
-- begin-expected
-- columns: count
-- row: 1600
-- end-expected
SELECT count(*) FROM jk_wide_one JOIN jk_wide_two USING (n);

-- begin-expected
-- columns: count
-- row: 1600
-- end-expected
SELECT count(*) FROM jk_wide_one LEFT JOIN jk_wide_two USING (n);

-- begin-expected
-- columns: count
-- row: 1600
-- end-expected
SELECT count(*) FROM jk_wide_one JOIN jk_wide_two ON jk_wide_one.n = jk_wide_two.n;

-- begin-expected
-- columns: count
-- row: 1600
-- end-expected
SELECT count(*) FROM jk_wide_one LEFT JOIN jk_wide_two ON jk_wide_one.n = jk_wide_two.n;

-- The rest of the condition is still applied to every candidate the index hands back.
-- begin-expected
-- columns: count
-- row: 40
-- end-expected
SELECT count(*) FROM jk_wide_one a JOIN jk_wide_two b ON a.n = b.n AND a.i = b.i;

-- A type the index has no spelling of shares one bucket with every other such
-- value; the comparison then tells them apart, so the answer is unchanged.
-- begin-expected
-- columns: count
-- row: 1600
-- end-expected
SELECT count(*) FROM jk_uuid_one JOIN jk_uuid_two USING (u);

-- and a condition that is not a truth value is refused whichever way the join ran
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean
-- end-expected-error
SELECT count(*) FROM jk_wide_one a JOIN jk_wide_two b ON a.i;

-- ============================================================================
-- A window is partitioned by the same rule
-- ============================================================================

-- begin-expected
-- columns: n | count
-- row: 1.00 | 2
-- row: 1.00 | 2
-- row: 2.00 | 1
-- end-expected
SELECT n, count(*) OVER (PARTITION BY n) FROM jk_part ORDER BY n;

-- A composite key may not run its parts together: one row's key here is
-- ('a', 'b') and the other's is ('a' || chr(1) || 'b', ''), which are two keys.
-- begin-expected
-- columns: length | count
-- row: 1 | 1
-- row: 3 | 1
-- end-expected
SELECT length(a), count(*) OVER (PARTITION BY a, b) FROM jk_sep ORDER BY 1;

-- A null is not a value and never equals one, and two of them still fall in the
-- same partition: PARTITION BY groups the way GROUP BY groups.
-- begin-expected
-- columns: v | count
-- row: NULL | 2
-- row: NULL | 2
-- row: 1 | 1
-- end-expected
SELECT v, count(*) OVER (PARTITION BY v) FROM (VALUES (NULL::int), (NULL), (1)) t(v) ORDER BY v NULLS FIRST;

-- ============================================================================
-- A LATERAL item inside a parenthesised join reads past the parentheses
-- ============================================================================

-- begin-expected
-- columns: id | w | x
-- row: 1 | 100 | 110
-- row: 2 | 200 | 220
-- end-expected
SELECT a.id, b.w, s.x FROM jk_lat_a a JOIN (jk_lat_b b JOIN LATERAL (SELECT a.v + b.w AS x) s ON true) ON a.id = b.id ORDER BY 1;

-- the same across a comma rather than a written join
-- begin-expected
-- columns: id | w | x
-- row: 1 | 100 | 110
-- row: 2 | 200 | 220
-- end-expected
SELECT a.id, b.w, s.x FROM jk_lat_a a, (jk_lat_b b JOIN LATERAL (SELECT a.v + b.w AS x) s ON true) WHERE a.id = b.id ORDER BY 1;

-- and for a function in FROM, which is lateral without saying so
-- begin-expected
-- columns: id | g
-- row: 1 | 1
-- row: 2 | 1
-- row: 2 | 2
-- end-expected
SELECT a.id, t.g FROM jk_lat_a a JOIN (jk_lat_b b CROSS JOIN LATERAL generate_series(1, a.v / 10) t(g)) ON a.id = b.id ORDER BY 1, 2;

-- An outer join over such an arm keeps all its left rows.
-- begin-expected
-- columns: id | x
-- row: 1 | 10
-- row: 2 | 20
-- end-expected
SELECT a.id, s.x FROM jk_lat_a a LEFT JOIN (jk_lat_b b JOIN LATERAL (SELECT a.v AS x) s ON true) ON a.id = b.id ORDER BY 1;

-- Only a lateral item may read out. An ordinary arm reaching for a name to its
-- left is still refused, which is what makes the rule above a rule and not a
-- relaxation.
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT a.id FROM jk_lat_a a JOIN (jk_lat_b b JOIN jk_lat_b c ON a.v = c.w) ON a.id = b.id;

-- teardown
DROP TABLE jk_one;
DROP TABLE jk_two;
DROP TABLE jk_nat_one;
DROP TABLE jk_nat_two;
DROP TABLE jk_c3;
DROP TABLE jk_c6;
DROP TABLE jk_wide_one;
DROP TABLE jk_wide_two;
DROP TABLE jk_uuid_one;
DROP TABLE jk_uuid_two;
DROP TABLE jk_part;
DROP TABLE jk_sep;
DROP TABLE jk_lat_a;
DROP TABLE jk_lat_b;
