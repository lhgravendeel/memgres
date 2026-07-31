-- ============================================================================
-- Feature Comparison: DDL residuals -- column lists, keys, collations, TRUNCATE
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Four groups from the DDL sweep.
--
-- CREATE TABLE t (a, b) AS query is ordinary SQL that memgres could not parse at
-- all: the names rename the query's columns left to right, fewer names than
-- columns is allowed, and more is 42601.
--
-- A VIRTUAL generated column is computed on read and never stored, so there is
-- nothing for an index to hold; PostgreSQL refuses a primary key or a unique
-- constraint over one, and the ALTER TABLE path did not.
--
-- A column and a domain each carry one collation, so a second COLLATE clause is
-- a syntax error rather than an override, and NOT NULL written twice on a domain
-- is 42P17 rather than merely redundant.
--
-- And TRUNCATE naming a relation that is not a table is 42809, a different
-- complaint from a name that resolves to nothing.
-- ============================================================================

-- ============================================================================
-- 1. CREATE TABLE ... (columns) AS query
-- ============================================================================
DROP TABLE IF EXISTS dvr_q1 CASCADE;
CREATE TABLE dvr_q1 (p, q) AS SELECT 1, 2;

-- begin-expected
-- columns: p, q
-- row: 1, 2
-- end-expected
SELECT p::text AS p, q::text AS q FROM dvr_q1;

DROP TABLE IF EXISTS dvr_q2 CASCADE;
CREATE TABLE dvr_q2 (p) AS SELECT 1;

-- begin-expected
-- columns: p
-- row: 1
-- end-expected
SELECT p::text AS p FROM dvr_q2;

-- fewer names than columns leaves the rest as the query named them
DROP TABLE IF EXISTS dvr_q3 CASCADE;
CREATE TABLE dvr_q3 (p) AS SELECT 1 AS one, 2 AS two;

-- begin-expected
-- columns: a, b
-- row: p, two
-- end-expected
SELECT (SELECT column_name FROM information_schema.columns
         WHERE table_name = 'dvr_q3' AND ordinal_position = 1)::text AS a,
       (SELECT column_name FROM information_schema.columns
         WHERE table_name = 'dvr_q3' AND ordinal_position = 2)::text AS b;

DROP TABLE IF EXISTS dvr_q5 CASCADE;
CREATE TABLE dvr_q5 (p, q) AS SELECT 1, 2 WITH NO DATA;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM dvr_q5;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: too many column names were specified
-- end-expected-error
CREATE TABLE dvr_q4 (p, q) AS SELECT 1;

-- an ordinary column definition list is still read as one
DROP TABLE IF EXISTS dvr_plain CASCADE;
CREATE TABLE dvr_plain (a int, b text);

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM information_schema.columns WHERE table_name = 'dvr_plain';

-- ============================================================================
-- 2. No key over a VIRTUAL generated column
-- ============================================================================
DROP TABLE IF EXISTS dvr_g2 CASCADE;
CREATE TABLE dvr_g2 (a int, b int GENERATED ALWAYS AS (a) VIRTUAL);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: primary keys on virtual generated columns are not supported
-- end-expected-error
ALTER TABLE dvr_g2 ADD CONSTRAINT dvr_g2pk PRIMARY KEY (b);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unique constraints on virtual generated columns are not supported
-- end-expected-error
ALTER TABLE dvr_g2 ADD UNIQUE (b);

-- a key over the stored column is fine
ALTER TABLE dvr_g2 ADD CONSTRAINT dvr_g2pk PRIMARY KEY (a);

-- begin-expected
-- columns: conname
-- row: dvr_g2pk
-- end-expected
SELECT conname::text AS conname FROM pg_constraint
 WHERE conrelid = 'dvr_g2'::regclass AND contype = 'p';

-- ============================================================================
-- 3. One collation per column, one NOT NULL per domain
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42601
-- message-like: multiple COLLATE clauses not allowed
-- end-expected-error
CREATE TABLE dvr_cc (t text COLLATE "C" COLLATE "C");

-- begin-expected-error
-- sqlstate: 42601
-- message-like: multiple COLLATE clauses not allowed
-- end-expected-error
CREATE DOMAIN dvr_d7 AS text COLLATE "C" COLLATE "C";

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: redundant NOT NULL constraint definition
-- end-expected-error
CREATE DOMAIN dvr_d9 AS int NOT NULL NOT NULL;

-- one of each is ordinary
DROP TABLE IF EXISTS dvr_ok CASCADE;
CREATE TABLE dvr_ok (t text COLLATE "C");
DROP DOMAIN IF EXISTS dvr_dok CASCADE;
CREATE DOMAIN dvr_dok AS int NOT NULL;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM information_schema.columns
 WHERE table_name = 'dvr_ok' AND column_name = 't';

-- ============================================================================
-- 4. TRUNCATE names what the relation is not
-- ============================================================================
DROP VIEW IF EXISTS dvr_kv CASCADE;
DROP TABLE IF EXISTS dvr_kt CASCADE;
CREATE TABLE dvr_kt (i int);
CREATE VIEW dvr_kv AS SELECT * FROM dvr_kt;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "dvr_kv" is not a table
-- end-expected-error
TRUNCATE dvr_kv;

DROP MATERIALIZED VIEW IF EXISTS dvr_km CASCADE;
CREATE MATERIALIZED VIEW dvr_km AS SELECT * FROM dvr_kt;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "dvr_km" is not a table
-- end-expected-error
TRUNCATE dvr_km;

-- a name that resolves to nothing is the other complaint
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "dvr_nosuch" does not exist
-- end-expected-error
TRUNCATE dvr_nosuch;

-- and a real table truncates
INSERT INTO dvr_kt VALUES (1),(2);
TRUNCATE dvr_kt;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM dvr_kt;

DROP MATERIALIZED VIEW IF EXISTS dvr_km CASCADE;
DROP VIEW IF EXISTS dvr_kv CASCADE;
DROP TABLE IF EXISTS dvr_kt CASCADE;
DROP TABLE IF EXISTS dvr_g2 CASCADE;
DROP TABLE IF EXISTS dvr_ok CASCADE;
DROP DOMAIN IF EXISTS dvr_dok CASCADE;
DROP TABLE IF EXISTS dvr_q1 CASCADE;
DROP TABLE IF EXISTS dvr_q2 CASCADE;
DROP TABLE IF EXISTS dvr_q3 CASCADE;
DROP TABLE IF EXISTS dvr_q5 CASCADE;
DROP TABLE IF EXISTS dvr_plain CASCADE;
