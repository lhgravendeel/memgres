-- ============================================================================
-- The command identifiers a definition spends
--
-- PostgreSQL hands the rest of a statement a fresh command identifier whenever
-- it has to make what it has just written visible to the work that follows:
-- the relation before the routine filling it opens it, the column defaults
-- before the constraints checked against them, each index before the next one
-- is built. A stretch of writes between two of those points costs one
-- identifier however many catalogue rows it wrote -- which is why two CHECK
-- constraints cost what one costs -- and a stretch that wrote nothing costs
-- none. Retiring a relation is the other way about: its catalogue rows are
-- found and deleted one at a time, so there each row is an identifier of its
-- own.
--
-- cmin reports the counter as it stood when a row version was written, so a
-- row written after a definition reads how much that definition cost. The
-- shape of the definition is what decides it: a relation whose widest row
-- would not fit in a quarter of a page is given a TOAST table, which is four
-- identifiers more.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

CREATE TABLE zzp5tr_cmi_p (i int);

-- ============================================================================
-- What a column's width costs: varchar(501) still fits in a quarter page and
-- varchar(502) does not, so the second one earns a TOAST table
-- ============================================================================
BEGIN;
CREATE TABLE zzp5tr_cmi_a1 (i int);
INSERT INTO zzp5tr_cmi_p VALUES (1);
CREATE TABLE zzp5tr_cmi_a2 (t text);
INSERT INTO zzp5tr_cmi_p VALUES (2);
CREATE TABLE zzp5tr_cmi_a3 (v varchar(10));
INSERT INTO zzp5tr_cmi_p VALUES (3);
CREATE TABLE zzp5tr_cmi_a4 (n numeric);
INSERT INTO zzp5tr_cmi_p VALUES (4);
CREATE TABLE zzp5tr_cmi_a5 (n numeric(5,2));
INSERT INTO zzp5tr_cmi_p VALUES (5);
CREATE TABLE zzp5tr_cmi_a6 (v varchar(501));
INSERT INTO zzp5tr_cmi_p VALUES (6);
CREATE TABLE zzp5tr_cmi_a7 (v varchar(502));
INSERT INTO zzp5tr_cmi_p VALUES (7);
CREATE TABLE zzp5tr_cmi_a8 (a int[]);
INSERT INTO zzp5tr_cmi_p VALUES (8);
CREATE TABLE zzp5tr_cmi_a9 (u uuid);
INSERT INTO zzp5tr_cmi_p VALUES (9);

-- begin-expected
-- columns: i | cmin
-- row: 1 | 1
-- row: 2 | 7
-- row: 3 | 9
-- row: 4 | 15
-- row: 5 | 17
-- row: 6 | 19
-- row: 7 | 25
-- row: 8 | 31
-- row: 9 | 33
-- end-expected
SELECT i, cmin::text AS cmin FROM zzp5tr_cmi_p ORDER BY i;

ROLLBACK;

-- ============================================================================
-- What the keys, the defaults and a sequence cost: a key builds an index and
-- then files the constraint owning it, the defaults are written together and
-- the constraints checked against them together, and a serial writes a
-- sequence and says which column owns it
-- ============================================================================
BEGIN;
CREATE TABLE zzp5tr_cmi_b1 (i int primary key);
INSERT INTO zzp5tr_cmi_p VALUES (1);
CREATE TABLE zzp5tr_cmi_b2 (i int unique);
INSERT INTO zzp5tr_cmi_p VALUES (2);
CREATE TABLE zzp5tr_cmi_b3 (i int check (i > 0), j int check (j > 0));
INSERT INTO zzp5tr_cmi_p VALUES (3);
CREATE TABLE zzp5tr_cmi_b4 (i int default 1, j int default 2);
INSERT INTO zzp5tr_cmi_p VALUES (4);
CREATE TABLE zzp5tr_cmi_b5 (i int not null, j int not null);
INSERT INTO zzp5tr_cmi_p VALUES (5);
CREATE TABLE zzp5tr_cmi_b6 (i int not null default 1);
INSERT INTO zzp5tr_cmi_p VALUES (6);
CREATE TABLE zzp5tr_cmi_b7 (i int primary key, j int unique);
INSERT INTO zzp5tr_cmi_p VALUES (7);
CREATE TABLE zzp5tr_cmi_b8 (t text primary key);
INSERT INTO zzp5tr_cmi_p VALUES (8);
CREATE TABLE zzp5tr_cmi_b9 (i serial);
INSERT INTO zzp5tr_cmi_p VALUES (9);
CREATE TABLE zzp5tr_cmi_b10 (i int generated always as identity);
INSERT INTO zzp5tr_cmi_p VALUES (10);
CREATE TABLE zzp5tr_cmi_b11 (i int, exclude (i with =));
INSERT INTO zzp5tr_cmi_p VALUES (11);

-- begin-expected
-- columns: i | cmin
-- row: 1 | 4
-- row: 2 | 8
-- row: 3 | 11
-- row: 4 | 14
-- row: 5 | 17
-- row: 6 | 21
-- row: 7 | 28
-- row: 8 | 37
-- row: 9 | 44
-- row: 10 | 50
-- row: 11 | 54
-- end-expected
SELECT i, cmin::text AS cmin FROM zzp5tr_cmi_p ORDER BY i;

ROLLBACK;

-- ============================================================================
-- Foreign keys, partitioning, and a relation a query builds
-- ============================================================================
CREATE TABLE zzp5tr_cmi_r (i int primary key);
BEGIN;

-- a key writes its row and the triggers enforcing it, and marks the relation
-- it points at as carrying triggers -- which the second key pointing at the
-- same relation no longer has to do
CREATE TABLE zzp5tr_cmi_c1 (i int references zzp5tr_cmi_r(i));
INSERT INTO zzp5tr_cmi_p VALUES (1);
CREATE TABLE zzp5tr_cmi_c2 (i int references zzp5tr_cmi_r(i), j int references zzp5tr_cmi_r(i));
INSERT INTO zzp5tr_cmi_p VALUES (2);
CREATE TABLE zzp5tr_cmi_c3 (i int) PARTITION BY RANGE (i);
INSERT INTO zzp5tr_cmi_p VALUES (3);
CREATE TABLE zzp5tr_cmi_c4 PARTITION OF zzp5tr_cmi_c3 FOR VALUES FROM (1) TO (10);
INSERT INTO zzp5tr_cmi_p VALUES (4);

-- the rows a query writes into the relation it built take an identifier of
-- their own, after the relation's catalogue rows and its TOAST table
CREATE TABLE zzp5tr_cmi_c6 AS SELECT 1 AS i;
INSERT INTO zzp5tr_cmi_p VALUES (5);
CREATE TABLE zzp5tr_cmi_c7 AS SELECT 'x'::text AS t;
INSERT INTO zzp5tr_cmi_p VALUES (6);
CREATE TABLE zzp5tr_cmi_c8 AS SELECT 1 AS i WITH NO DATA;
INSERT INTO zzp5tr_cmi_p VALUES (7);

-- begin-expected
-- columns: i | cmin
-- row: 1 | 7
-- row: 2 | 18
-- row: 3 | 21
-- row: 4 | 25
-- row: 5 | 28
-- row: 6 | 35
-- row: 7 | 37
-- end-expected
SELECT i, cmin::text AS cmin FROM zzp5tr_cmi_p ORDER BY i;

ROLLBACK;

-- ============================================================================
-- Retiring a relation costs three for the relation, its composite type and
-- that type's array type, and then one for every catalogue row describing it
-- ============================================================================
CREATE TABLE zzp5tr_cmi_d1 (i int);
CREATE TABLE zzp5tr_cmi_d2 (t text);
CREATE TABLE zzp5tr_cmi_d3 (i int primary key);
CREATE TABLE zzp5tr_cmi_d4 (i int unique);
CREATE TABLE zzp5tr_cmi_d5 (i int check (i > 0), j int check (j > 0));
CREATE TABLE zzp5tr_cmi_d6 (i int default 1, j int default 2);
CREATE TABLE zzp5tr_cmi_d7 (i int not null);
CREATE TABLE zzp5tr_cmi_d8 (i serial);
CREATE TABLE zzp5tr_cmi_d9 (i int primary key, t text);
CREATE INDEX zzp5tr_cmi_d1x ON zzp5tr_cmi_d1 (i);
BEGIN;
DROP TABLE zzp5tr_cmi_d2;
INSERT INTO zzp5tr_cmi_p VALUES (1);
DROP TABLE zzp5tr_cmi_d3;
INSERT INTO zzp5tr_cmi_p VALUES (2);
DROP TABLE zzp5tr_cmi_d4;
INSERT INTO zzp5tr_cmi_p VALUES (3);

-- here each of the two CHECKs is a row of its own, and so is each default
DROP TABLE zzp5tr_cmi_d5;
INSERT INTO zzp5tr_cmi_p VALUES (4);
DROP TABLE zzp5tr_cmi_d6;
INSERT INTO zzp5tr_cmi_p VALUES (5);
DROP TABLE zzp5tr_cmi_d7;
INSERT INTO zzp5tr_cmi_p VALUES (6);
DROP TABLE zzp5tr_cmi_d8;
INSERT INTO zzp5tr_cmi_p VALUES (7);
DROP TABLE zzp5tr_cmi_d9;
INSERT INTO zzp5tr_cmi_p VALUES (8);
DROP TABLE zzp5tr_cmi_d1;
INSERT INTO zzp5tr_cmi_p VALUES (9);

-- begin-expected
-- columns: i | cmin
-- row: 1 | 5
-- row: 2 | 12
-- row: 3 | 18
-- row: 4 | 24
-- row: 5 | 30
-- row: 6 | 35
-- row: 7 | 42
-- row: 8 | 51
-- row: 9 | 56
-- end-expected
SELECT i, cmin::text AS cmin FROM zzp5tr_cmi_p ORDER BY i;

ROLLBACK;

-- ============================================================================
-- Indexes, views and materialized views
-- ============================================================================
CREATE TABLE zzp5tr_cmi_e (i int, t text);
CREATE INDEX zzp5tr_cmi_ex ON zzp5tr_cmi_e (i);
CREATE VIEW zzp5tr_cmi_ev AS SELECT * FROM zzp5tr_cmi_e;
CREATE MATERIALIZED VIEW zzp5tr_cmi_em AS SELECT i FROM zzp5tr_cmi_e;
CREATE MATERIALIZED VIEW zzp5tr_cmi_emt AS SELECT t FROM zzp5tr_cmi_e;
BEGIN;
CREATE INDEX zzp5tr_cmi_ex2 ON zzp5tr_cmi_e (t);
INSERT INTO zzp5tr_cmi_p VALUES (1);
DROP INDEX zzp5tr_cmi_ex;
INSERT INTO zzp5tr_cmi_p VALUES (2);

-- a refresh fills a relation of its own and swaps it into place, so
-- everything behind the view is built a second time
REFRESH MATERIALIZED VIEW zzp5tr_cmi_em;
INSERT INTO zzp5tr_cmi_p VALUES (3);
REFRESH MATERIALIZED VIEW zzp5tr_cmi_emt;
INSERT INTO zzp5tr_cmi_p VALUES (4);
DROP MATERIALIZED VIEW zzp5tr_cmi_em;
INSERT INTO zzp5tr_cmi_p VALUES (5);
DROP MATERIALIZED VIEW zzp5tr_cmi_emt;
INSERT INTO zzp5tr_cmi_p VALUES (6);
CREATE MATERIALIZED VIEW zzp5tr_cmi_em2 AS SELECT i FROM zzp5tr_cmi_e;
INSERT INTO zzp5tr_cmi_p VALUES (7);
CREATE MATERIALIZED VIEW zzp5tr_cmi_em3 AS SELECT t FROM zzp5tr_cmi_e;
INSERT INTO zzp5tr_cmi_p VALUES (8);
CREATE MATERIALIZED VIEW zzp5tr_cmi_em4 AS SELECT i FROM zzp5tr_cmi_e WITH NO DATA;
INSERT INTO zzp5tr_cmi_p VALUES (9);
CREATE VIEW zzp5tr_cmi_ev2 AS SELECT * FROM zzp5tr_cmi_e;
INSERT INTO zzp5tr_cmi_p VALUES (10);
DROP VIEW zzp5tr_cmi_ev;
INSERT INTO zzp5tr_cmi_p VALUES (11);

-- begin-expected
-- columns: i | cmin
-- row: 1 | 2
-- row: 2 | 4
-- row: 3 | 11
-- row: 4 | 26
-- row: 5 | 31
-- row: 6 | 38
-- row: 7 | 47
-- row: 8 | 68
-- row: 9 | 71
-- row: 10 | 74
-- row: 11 | 79
-- end-expected
SELECT i, cmin::text AS cmin FROM zzp5tr_cmi_p ORDER BY i;

ROLLBACK;

-- ============================================================================
-- TRUNCATE, sequences and types
-- ============================================================================
CREATE TABLE zzp5tr_cmi_f (i int, t text);
CREATE INDEX zzp5tr_cmi_fx ON zzp5tr_cmi_f (i);
CREATE TABLE zzp5tr_cmi_g (i serial);
CREATE TABLE zzp5tr_cmi_g2 (i serial, j serial);
BEGIN;

-- TRUNCATE gives the relation storage it has never held a row in, and the
-- TOAST table behind it and every index on it are built again beside it
TRUNCATE zzp5tr_cmi_f;
INSERT INTO zzp5tr_cmi_p VALUES (1);
TRUNCATE zzp5tr_cmi_g;
INSERT INTO zzp5tr_cmi_p VALUES (2);
TRUNCATE zzp5tr_cmi_g2 RESTART IDENTITY;
INSERT INTO zzp5tr_cmi_p VALUES (3);
CREATE SEQUENCE zzp5tr_cmi_s;
INSERT INTO zzp5tr_cmi_p VALUES (4);
DROP SEQUENCE zzp5tr_cmi_s;
INSERT INTO zzp5tr_cmi_p VALUES (5);
CREATE TYPE zzp5tr_cmi_ty AS ENUM ('a','b');
INSERT INTO zzp5tr_cmi_p VALUES (6);
DROP TYPE zzp5tr_cmi_ty;
INSERT INTO zzp5tr_cmi_p VALUES (7);
CREATE TYPE zzp5tr_cmi_tc AS (a int);
INSERT INTO zzp5tr_cmi_p VALUES (8);
DROP TYPE zzp5tr_cmi_tc;
INSERT INTO zzp5tr_cmi_p VALUES (9);
CREATE DOMAIN zzp5tr_cmi_td AS int;
INSERT INTO zzp5tr_cmi_p VALUES (10);
DROP DOMAIN zzp5tr_cmi_td;
INSERT INTO zzp5tr_cmi_p VALUES (11);

-- begin-expected
-- columns: i | cmin
-- row: 1 | 6
-- row: 2 | 8
-- row: 3 | 12
-- row: 4 | 15
-- row: 5 | 17
-- row: 6 | 19
-- row: 7 | 22
-- row: 8 | 24
-- row: 9 | 28
-- row: 10 | 30
-- row: 11 | 33
-- end-expected
SELECT i, cmin::text AS cmin FROM zzp5tr_cmi_p ORDER BY i;

ROLLBACK;

-- ============================================================================
-- A savepoint takes no identifier of its own, and one a rolled-back savepoint
-- spent is not handed out again
-- ============================================================================
BEGIN;
INSERT INTO zzp5tr_cmi_p VALUES (1);
SAVEPOINT zzp5tr_sp;
CREATE TABLE zzp5tr_cmi_h1 (t text);
ROLLBACK TO SAVEPOINT zzp5tr_sp;
INSERT INTO zzp5tr_cmi_p VALUES (2);
SAVEPOINT zzp5tr_sp2;
INSERT INTO zzp5tr_cmi_p VALUES (3);
RELEASE SAVEPOINT zzp5tr_sp2;
INSERT INTO zzp5tr_cmi_p VALUES (4);

-- begin-expected
-- columns: i | cmin
-- row: 1 | 0
-- row: 2 | 6
-- row: 3 | 7
-- row: 4 | 8
-- end-expected
SELECT i, cmin::text AS cmin FROM zzp5tr_cmi_p ORDER BY i;

ROLLBACK;

-- ============================================================================
-- The row written into a relation the same transaction defined reads how much
-- that definition cost before it
-- ============================================================================
BEGIN;
CREATE TABLE zzp5tr_cmi_i1 (t text);
INSERT INTO zzp5tr_cmi_i1 VALUES ('a');

-- begin-expected
-- columns: cmin
-- row: 5
-- end-expected
SELECT cmin::text AS cmin FROM zzp5tr_cmi_i1;

ROLLBACK;

BEGIN;
CREATE TABLE zzp5tr_cmi_i2 (i int);
INSERT INTO zzp5tr_cmi_i2 VALUES (0);
CREATE TABLE zzp5tr_cmi_i3 AS SELECT 'x'::text AS t;

-- begin-expected
-- columns: cmin
-- row: 7
-- end-expected
SELECT cmin::text AS cmin FROM zzp5tr_cmi_i3;

ROLLBACK;

DROP TABLE zzp5tr_cmi_f;
DROP TABLE zzp5tr_cmi_g;
DROP TABLE zzp5tr_cmi_g2;
DROP MATERIALIZED VIEW zzp5tr_cmi_em;
DROP MATERIALIZED VIEW zzp5tr_cmi_emt;
DROP VIEW zzp5tr_cmi_ev;
DROP TABLE zzp5tr_cmi_e;
DROP TABLE zzp5tr_cmi_d1;
DROP TABLE zzp5tr_cmi_d2;
DROP TABLE zzp5tr_cmi_d3;
DROP TABLE zzp5tr_cmi_d4;
DROP TABLE zzp5tr_cmi_d5;
DROP TABLE zzp5tr_cmi_d6;
DROP TABLE zzp5tr_cmi_d7;
DROP TABLE zzp5tr_cmi_d8;
DROP TABLE zzp5tr_cmi_d9;
DROP TABLE zzp5tr_cmi_r;
DROP TABLE zzp5tr_cmi_p;
