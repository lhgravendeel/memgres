-- Syntax PostgreSQL 18 accepts: a column constraint deferred to commit, assignment to a slice of
-- an array, an expression where LIMIT wants a count, and a key that is unique per period.

-- A deferred key is checked at commit, so a transient duplicate is allowed inside the transaction.
DROP TABLE IF EXISTS psg_d CASCADE;
CREATE TABLE psg_d (id int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED);
BEGIN;
INSERT INTO psg_d VALUES (1);
INSERT INTO psg_d VALUES (1);
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value
-- end-expected-error
COMMIT;
ROLLBACK;
DROP TABLE psg_d;

DROP TABLE IF EXISTS psg_d2 CASCADE;
CREATE TABLE psg_d2 (id int UNIQUE DEFERRABLE INITIALLY DEFERRED);
DROP TABLE psg_d2;

DROP TABLE IF EXISTS psg_d3 CASCADE;
DROP TABLE IF EXISTS psg_dp CASCADE;
CREATE TABLE psg_dp (id int PRIMARY KEY);
CREATE TABLE psg_d3 (p int REFERENCES psg_dp(id) DEFERRABLE INITIALLY DEFERRED);
DROP TABLE psg_d3;
DROP TABLE psg_dp;

-- NOT DEFERRABLE is the default and checks at once.
DROP TABLE IF EXISTS psg_d4 CASCADE;
CREATE TABLE psg_d4 (id int PRIMARY KEY NOT DEFERRABLE);
INSERT INTO psg_d4 VALUES (1);
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value
-- end-expected-error
INSERT INTO psg_d4 VALUES (1);
DROP TABLE psg_d4;

-- Assigning to a slice replaces that stretch of the array.
DROP TABLE IF EXISTS psg_a CASCADE;
CREATE TABLE psg_a (id int PRIMARY KEY, a int[]);
INSERT INTO psg_a VALUES (1, ARRAY[1,2,3]);
UPDATE psg_a SET a[1:2] = ARRAY[7,8];

-- begin-expected
-- columns: a
-- row: {7,8,3}
-- end-expected
SELECT a::text AS a FROM psg_a;

-- Reaching past the end extends the array.
UPDATE psg_a SET a[4:5] = ARRAY[9,10];

-- begin-expected
-- columns: a
-- row: {7,8,3,9,10}
-- end-expected
SELECT a::text AS a FROM psg_a;

-- A single element still assigns as an element.
UPDATE psg_a SET a[2] = 99;

-- begin-expected
-- columns: a
-- row: {7,99,3,9,10}
-- end-expected
SELECT a::text AS a FROM psg_a;

DROP TABLE psg_a;

DROP TABLE IF EXISTS psg_a4 CASCADE;
CREATE TABLE psg_a4 (id int PRIMARY KEY, a text[]);
INSERT INTO psg_a4 VALUES (1, ARRAY['a','b','c']);
UPDATE psg_a4 SET a[2:3] = ARRAY['x','y'];

-- begin-expected
-- columns: a
-- row: {a,x,y}
-- end-expected
SELECT a::text AS a FROM psg_a4;

DROP TABLE psg_a4;

-- LIMIT and OFFSET take any value expression, not only a literal.
-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM (SELECT generate_series(1,10) g LIMIT 2+1) s;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM (SELECT generate_series(1,10) g ORDER BY g LIMIT 2 OFFSET 4*2) s;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM (SELECT generate_series(1,10) g LIMIT greatest(1,2)) s;

-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*) AS n FROM (SELECT generate_series(1,10) g LIMIT 4) s;

-- begin-expected
-- columns: n
-- row: 10
-- end-expected
SELECT count(*) AS n FROM (SELECT generate_series(1,10) g LIMIT ALL) s;

-- WITHOUT OVERLAPS makes the key unique per period. The scalar part of the key is compared by a
-- GiST index, which needs btree_gist to know how to compare an integer.
CREATE EXTENSION IF NOT EXISTS btree_gist;
DROP TABLE IF EXISTS psg_t CASCADE;
CREATE TABLE psg_t (id int, valid daterange, PRIMARY KEY (id, valid WITHOUT OVERLAPS));
INSERT INTO psg_t VALUES (1,'[2020-01-01,2021-01-01)');

-- begin-expected-error
-- sqlstate: 23P01
-- message-like: conflicting key value violates exclusion constraint
-- end-expected-error
INSERT INTO psg_t VALUES (1,'[2020-06-01,2022-01-01)');

-- Periods that merely touch do not overlap.
INSERT INTO psg_t VALUES (1,'[2021-01-01,2022-01-01)');

-- A different key never conflicts.
INSERT INTO psg_t VALUES (2,'[2020-06-01,2022-01-01)');

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM psg_t;

DROP TABLE psg_t;
