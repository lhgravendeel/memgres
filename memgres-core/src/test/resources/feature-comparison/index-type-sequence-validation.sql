-- ============================================================================
-- Feature Comparison: definition-time validation of indexes, types, domains
--                     and sequences
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Covers what CREATE INDEX / CREATE TYPE / CREATE DOMAIN / CREATE SEQUENCE
-- reject before anything is stored: unknown access methods and operator
-- classes, what an access method is capable of, aggregates and subqueries in
-- index expressions and predicates, non-immutable predicates, INCLUDE rules,
-- duplicate composite attributes and enum labels, range subtypes, shell types,
-- dropping a type a column depends on, conflicting domain constraints, and the
-- sequence option crosschecks (INCREMENT, MINVALUE/MAXVALUE, START, RESTART,
-- CACHE, and the AS data type). Also the statements that must keep working.
-- ============================================================================

DROP TABLE IF EXISTS dit_t CASCADE;
DROP VIEW IF EXISTS dit_v CASCADE;
DROP TYPE IF EXISTS dit_ct CASCADE;
DROP SEQUENCE IF EXISTS dit_s CASCADE;

CREATE TABLE dit_t (x int, y text, z int);
INSERT INTO dit_t VALUES (1, 'a', 1), (2, 'b', 2);
CREATE VIEW dit_v AS SELECT * FROM dit_t;
CREATE INDEX dit_ix_exist ON dit_t (x);
CREATE TYPE dit_ct AS (a int, b text);

-- ============================================================================
-- CREATE INDEX: relation kind, access method, and its capabilities
-- ============================================================================

CREATE INDEX dit_i ON dit_v (x);
CREATE INDEX dit_i ON dit_t USING nosuchmethod (x);
CREATE INDEX dit_i ON dit_v USING nosuchmethod (x);
CREATE UNIQUE INDEX dit_i ON dit_t USING hash (x);
CREATE INDEX dit_i ON dit_t USING hash (x) INCLUDE (y);
CREATE INDEX dit_i ON dit_t USING hash (x, y);
CREATE INDEX dit_i ON dit_t USING hash (x DESC);
CREATE INDEX dit_i ON dit_t USING hash (x NULLS FIRST);
CREATE UNIQUE INDEX dit_i ON dit_t USING hash (x, y);
CREATE INDEX dit_i ON dit_t USING hash (x, y) INCLUDE (z);
CREATE INDEX dit_i ON dit_t USING hash (nosuchcol, y);
CREATE INDEX dit_i ON dit_t USING hash (x DESC) INCLUDE (y);
CREATE UNIQUE INDEX dit_i ON dit_t USING gin (x);
CREATE INDEX dit_i ON dit_t USING gin (x) INCLUDE (y);
CREATE UNIQUE INDEX dit_i ON dit_t USING brin (x);
CREATE INDEX dit_i ON dit_t USING brin (x) INCLUDE (y);
CREATE UNIQUE INDEX dit_i ON dit_t USING gist (x);
CREATE INDEX dit_i ON dit_t USING gin (x DESC);

-- The access method is resolved before the index name is chosen
CREATE INDEX dit_ix_exist ON dit_t USING nosuchmethod (x);
CREATE INDEX dit_ix_exist ON dit_t (nosuchcol);
CREATE INDEX dit_ix_exist ON dit_t (y);

-- ============================================================================
-- CREATE INDEX: operator classes
-- ============================================================================

CREATE INDEX dit_i ON dit_t (x nosuch_ops);
CREATE INDEX dit_i ON dit_t USING gin (x nosuch_ops);
CREATE INDEX dit_i ON dit_t (x text_pattern_ops);
CREATE INDEX dit_i ON dit_t USING hash (x text_pattern_ops);
CREATE INDEX dit_i ON dit_t (x COLLATE "C");

-- ============================================================================
-- CREATE INDEX: expressions and predicates
-- ============================================================================

CREATE INDEX dit_i ON dit_t ((SELECT 1));
CREATE INDEX dit_i ON dit_t ((count(x)));
CREATE INDEX dit_i ON dit_t ((sum(x)));
CREATE INDEX dit_i ON dit_t ((avg(x)));
CREATE INDEX dit_i ON dit_t USING nosuchmethod ((count(x)));
CREATE INDEX dit_i ON dit_t ((random()));
CREATE INDEX dit_i ON dit_t (x) WHERE x IN (SELECT 1);
CREATE INDEX dit_i ON dit_t (x) WHERE EXISTS (SELECT 1);
CREATE INDEX dit_i ON dit_t (x nosuch_ops) WHERE x IN (SELECT 1);
CREATE INDEX dit_i ON dit_t (x) WHERE x > random();
CREATE INDEX dit_i ON dit_t (nosuchcol) WHERE x > random();
CREATE INDEX dit_i ON dit_t (x) WHERE nosuchcol > 1;

-- ============================================================================
-- CREATE INDEX: INCLUDE rules and the definitions that must keep working
-- ============================================================================

CREATE INDEX dit_i ON dit_t (x) INCLUDE (nosuchcol);
CREATE INDEX dit_i ON dit_t (x) INCLUDE ((y || 'a'));
CREATE INDEX dit_i1 ON dit_t (x) INCLUDE (y);
CREATE INDEX dit_i2 ON dit_t (x, y) INCLUDE (z);
CREATE UNIQUE INDEX dit_i3 ON dit_t (x) INCLUDE (y);
CREATE INDEX dit_i4 ON dit_t USING hash (x);
CREATE INDEX dit_i5 ON dit_t USING gin (x);
CREATE INDEX dit_i6 ON dit_t (lower(y));
CREATE INDEX dit_i7 ON dit_t ((x + z));
CREATE INDEX dit_i8 ON dit_t (x DESC NULLS LAST);
CREATE INDEX dit_i9 ON dit_t (y text_pattern_ops);
CREATE INDEX dit_i10 ON dit_t (y varchar_pattern_ops);
CREATE INDEX dit_i11 ON dit_t USING btree (x int4_ops);
CREATE INDEX dit_i12 ON dit_t USING hash (x int4_ops);
CREATE INDEX dit_i13 ON dit_t (x) WHERE x > 1;
CREATE INDEX dit_i14 ON dit_t USING hash (x) WHERE x > 1;
CREATE INDEX dit_i15 ON dit_t (y COLLATE "C" text_pattern_ops);
CREATE INDEX dit_i16 ON dit_t (y DESC NULLS FIRST, x ASC);
CREATE INDEX dit_i17 ON dit_t ((y || 'a') text_pattern_ops);

SELECT indexname FROM pg_indexes WHERE tablename = 'dit_t' ORDER BY indexname;

-- ============================================================================
-- CREATE TYPE
-- ============================================================================

CREATE TYPE dit_ct2 AS (a int, a text);
CREATE TYPE dit_ct2 AS (a int, A text);
CREATE TYPE dit_ct2 AS (a int, a nosuchtype);
CREATE TYPE dit_ct2 AS (a nosuchtype);
CREATE TYPE dit_ct AS (a int);
CREATE TYPE dit_en2 AS ENUM ('a', 'a');
CREATE TYPE dit_en2 AS ENUM ('a', 'b', 'a');
CREATE TYPE dit_rg2 AS RANGE (COLLATION = "C");
CREATE TYPE dit_rg2 AS RANGE (SUBTYPE = nosuchtype);

ALTER TYPE dit_ct DROP ATTRIBUTE nosuchattr;
ALTER TYPE dit_ct ADD ATTRIBUTE a int;
ALTER TYPE dit_ct RENAME ATTRIBUTE nosuchattr TO zz;

-- The definitions that must keep working
CREATE TYPE dit_ct3 AS ("A" int, a text);
CREATE TYPE dit_en3 AS ENUM ('a', 'b');
CREATE TYPE dit_rg3 AS RANGE (SUBTYPE = int4);
ALTER TYPE dit_ct ADD ATTRIBUTE c int;
ALTER TYPE dit_ct DROP ATTRIBUTE c;

-- Shell types
CREATE TYPE dit_shell;
CREATE TYPE dit_shell;
CREATE TABLE dit_shelluse (c dit_shell);
DROP TYPE dit_shell;
CREATE TYPE dit_shell2;
CREATE TYPE dit_shell2 AS (a int);
DROP TYPE dit_shell2;

-- Dropping a type a column depends on
CREATE TYPE dit_ct4 AS (a int);
CREATE TABLE dit_uses_ct4 (c dit_ct4);
DROP TYPE dit_ct4;
DROP TYPE dit_ct4 CASCADE;
CREATE TYPE dit_en4 AS ENUM ('a');
CREATE TABLE dit_uses_en4 (c dit_en4);
DROP TYPE dit_en4;

-- ============================================================================
-- CREATE DOMAIN
-- ============================================================================

CREATE DOMAIN dit_d2 AS int4 NOT NULL NULL;
CREATE DOMAIN dit_d2 AS int4 NULL NOT NULL;
CREATE DOMAIN dit_d2 AS int4 DEFAULT 3 DEFAULT 3;
CREATE DOMAIN dit_d2 AS int4 UNIQUE;
CREATE DOMAIN dit_d2 AS int4 PRIMARY KEY;
CREATE DOMAIN dit_d2 AS int4 DEFAULT 3 UNIQUE;
CREATE DOMAIN dit_d2 AS int4 CONSTRAINT c REFERENCES dit_t(x);
CREATE DOMAIN dit_d2 AS int4 CHECK (x > 0);
CREATE DOMAIN dit_d2 AS int4 CHECK (nosuchcol > 0);
CREATE DOMAIN dit_d2 AS int4 NOT NULL CHECK (x > 0);
CREATE DOMAIN dit_d2 AS nosuchtype;
CREATE DOMAIN dit_d2 AS int4 COLLATE "C";

-- The definitions that must keep working
CREATE DOMAIN dit_d3 AS int4 CHECK (VALUE > 0);
CREATE DOMAIN dit_d4 AS int4 CHECK (value > 0);
CREATE DOMAIN dit_d5 AS int4 NOT NULL DEFAULT 3;
CREATE DOMAIN dit_d6 AS int4 DEFAULT 3 NOT NULL;
CREATE DOMAIN dit_d7 AS int4 NULL;
CREATE DOMAIN dit_d8 AS int4 CONSTRAINT c CHECK (VALUE > 0);
CREATE DOMAIN dit_d9 AS text COLLATE "C" NOT NULL;

-- ============================================================================
-- CREATE SEQUENCE option validation
-- ============================================================================

CREATE SEQUENCE dit_s2 AS text;
CREATE SEQUENCE dit_s2 AS numeric;
CREATE SEQUENCE dit_s2 AS text CACHE 0;
CREATE SEQUENCE dit_s2 INCREMENT 0;
CREATE SEQUENCE dit_s2 INCREMENT 0 CACHE 0;
CREATE SEQUENCE dit_s2 CACHE 0;
CREATE SEQUENCE dit_s2 CACHE -1;
CREATE SEQUENCE dit_s2 MINVALUE 10 MAXVALUE 5;
CREATE SEQUENCE dit_s2 MINVALUE 5 MAXVALUE 5;
CREATE SEQUENCE dit_s2 MAXVALUE 5 MINVALUE 10 START 7;
CREATE SEQUENCE dit_s2 CACHE 0 MINVALUE 10 MAXVALUE 5;
CREATE SEQUENCE dit_s2 MINVALUE 5 MAXVALUE 10 START 1;
CREATE SEQUENCE dit_s2 MINVALUE 5 MAXVALUE 10 START 20;
CREATE SEQUENCE dit_s2 START 0 INCREMENT 1 MINVALUE 1;
CREATE SEQUENCE dit_s2 INCREMENT -1 START 5;
CREATE SEQUENCE dit_s2 AS smallint MAXVALUE 100000;
CREATE SEQUENCE dit_s2 AS smallint MINVALUE -100000;
CREATE SEQUENCE dit_s2 AS smallint MAXVALUE 100000 MINVALUE -100000;
CREATE SEQUENCE dit_s2 AS smallint START 40000;
CREATE SEQUENCE dit_s2 AS int MAXVALUE 3000000000;
CREATE SEQUENCE dit_s2 OWNED BY dit_t.nosuchcol;
CREATE SEQUENCE dit_s2 OWNED BY dit_nosuchtable.x;
SELECT pg_get_serial_sequence('dit_t', 'nosuchcol');
SELECT pg_get_serial_sequence('dit_nosuchtable', 'x');
SELECT pg_get_serial_sequence('dit_t', 'x');

-- The definitions that must keep working
CREATE SEQUENCE dit_s3 MINVALUE 5 MAXVALUE 10 START 7;
CREATE SEQUENCE dit_s4 AS smallint;
CREATE SEQUENCE dit_s5 AS int2;
CREATE SEQUENCE dit_s6 CACHE 1;
CREATE SEQUENCE dit_s7 INCREMENT -1;
CREATE SEQUENCE dit_s8 AS smallint INCREMENT -1;
CREATE SEQUENCE dit_s9 AS bigint MAXVALUE 100;
CREATE SEQUENCE dit_s10 OWNED BY dit_t.x;
CREATE SEQUENCE dit_s11 OWNED BY NONE;
CREATE SEQUENCE dit_s12 MINVALUE 5;

SELECT min_value, max_value, start_value FROM pg_sequences WHERE sequencename = 'dit_s7';
SELECT max_value FROM pg_sequences WHERE sequencename = 'dit_s4';
SELECT start_value FROM pg_sequences WHERE sequencename = 'dit_s12';
SELECT nextval('dit_s4');

-- ============================================================================
-- ALTER SEQUENCE
-- ============================================================================

CREATE SEQUENCE dit_as1 MAXVALUE 100;
ALTER SEQUENCE dit_as1 RESTART WITH 1000;
ALTER SEQUENCE dit_as1 INCREMENT 0;
ALTER SEQUENCE dit_as1 CACHE 0;
ALTER SEQUENCE dit_as1 RESTART WITH 50;
ALTER SEQUENCE dit_as1 MAXVALUE 200 RESTART WITH 150;
SELECT nextval('dit_as1');

CREATE SEQUENCE dit_as2 MINVALUE 10 MAXVALUE 100;
ALTER SEQUENCE dit_as2 START WITH 5;

CREATE SEQUENCE dit_as3;
ALTER SEQUENCE dit_as3 MINVALUE 10;
ALTER SEQUENCE dit_as3 AS smallint;
SELECT max_value FROM pg_sequences WHERE sequencename = 'dit_as3';

CREATE SEQUENCE dit_as4 AS smallint;
ALTER SEQUENCE dit_as4 MAXVALUE 100000;

ALTER SEQUENCE dit_nosuchseq RESTART;
ALTER SEQUENCE IF EXISTS dit_nosuchseq RESTART;
ALTER SEQUENCE IF EXISTS dit_nosuchseq RESTART WITH 5;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS dit_uses_en4 CASCADE;
DROP TABLE IF EXISTS dit_uses_ct4 CASCADE;
DROP TABLE IF EXISTS dit_t CASCADE;
DROP VIEW IF EXISTS dit_v CASCADE;
DROP TYPE IF EXISTS dit_ct CASCADE;
DROP TYPE IF EXISTS dit_ct3 CASCADE;
DROP TYPE IF EXISTS dit_en3 CASCADE;
DROP TYPE IF EXISTS dit_en4 CASCADE;
DROP TYPE IF EXISTS dit_rg3 CASCADE;
DROP DOMAIN IF EXISTS dit_d3 CASCADE;
DROP DOMAIN IF EXISTS dit_d4 CASCADE;
DROP DOMAIN IF EXISTS dit_d5 CASCADE;
DROP DOMAIN IF EXISTS dit_d6 CASCADE;
DROP DOMAIN IF EXISTS dit_d7 CASCADE;
DROP DOMAIN IF EXISTS dit_d8 CASCADE;
DROP DOMAIN IF EXISTS dit_d9 CASCADE;
DROP SEQUENCE IF EXISTS dit_s3 CASCADE;
DROP SEQUENCE IF EXISTS dit_s4 CASCADE;
DROP SEQUENCE IF EXISTS dit_s5 CASCADE;
DROP SEQUENCE IF EXISTS dit_s6 CASCADE;
DROP SEQUENCE IF EXISTS dit_s7 CASCADE;
DROP SEQUENCE IF EXISTS dit_s8 CASCADE;
DROP SEQUENCE IF EXISTS dit_s9 CASCADE;
DROP SEQUENCE IF EXISTS dit_s10 CASCADE;
DROP SEQUENCE IF EXISTS dit_s11 CASCADE;
DROP SEQUENCE IF EXISTS dit_s12 CASCADE;
DROP SEQUENCE IF EXISTS dit_as1 CASCADE;
DROP SEQUENCE IF EXISTS dit_as2 CASCADE;
DROP SEQUENCE IF EXISTS dit_as3 CASCADE;
DROP SEQUENCE IF EXISTS dit_as4 CASCADE;
