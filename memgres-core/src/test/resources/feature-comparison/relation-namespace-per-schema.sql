-- ============================================================================
-- Feature Comparison: a sequence and an index each belong to ONE schema
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Memgres kept sequences and index metadata in maps keyed by the bare name
-- alone, one map for the whole database. That is wrong in both directions:
-- two schemas could not each hold a sequence or an index of the same name,
-- and a qualified name that named the wrong schema still found the object --
-- so DROP SEQUENCE b.s destroyed the sequence really living in a, and two
-- tables in different schemas with a serial column shared one counter.
-- ============================================================================

DROP SCHEMA IF EXISTS rnps_a CASCADE;
DROP SCHEMA IF EXISTS rnps_b CASCADE;
DROP SEQUENCE IF EXISTS rnps_pubseq;
CREATE SCHEMA rnps_a;
CREATE SCHEMA rnps_b;
SET search_path = public;

-- ============================================================================
-- 1. Two schemas each hold a sequence of the same name
-- ============================================================================

CREATE SEQUENCE rnps_a.sq;
CREATE SEQUENCE rnps_b.sq;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname = 'sq';

-- begin-expected
-- columns: nextval
-- row: 1
-- end-expected
SELECT nextval('rnps_a.sq');

-- begin-expected
-- columns: nextval
-- row: 2
-- end-expected
SELECT nextval('rnps_a.sq');

-- rnps_b's counter is its own and has not moved
-- begin-expected
-- columns: nextval
-- row: 1
-- end-expected
SELECT nextval('rnps_b.sq');

-- ============================================================================
-- 2. A qualified name reaches the schema it names and no other
-- ============================================================================

CREATE SEQUENCE rnps_a.s1;
SET search_path = rnps_b;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: sequence "s1" does not exist
-- end-expected-error
DROP SEQUENCE rnps_b.s1;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: sequence "s1" does not exist
-- end-expected-error
DROP SEQUENCE s1;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname = 's1';

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "rnps_b.s1" does not exist
-- end-expected-error
SELECT nextval('rnps_b.s1');

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "s1" does not exist
-- end-expected-error
SELECT nextval('s1');

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "rnps_b.s1" does not exist
-- end-expected-error
SELECT setval('rnps_b.s1', 10);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "rnps_b.s1" does not exist
-- end-expected-error
SELECT currval('rnps_b.s1');

-- The sequence that was named all along is untouched
-- begin-expected
-- columns: nextval
-- row: 1
-- end-expected
SELECT nextval('rnps_a.s1');

SET search_path = public;

-- ============================================================================
-- 3. Two serial tables in different schemas keep separate counters
-- ============================================================================

CREATE TABLE rnps_a.ser (id serial PRIMARY KEY);
CREATE TABLE rnps_b.ser (id serial PRIMARY KEY);
INSERT INTO rnps_a.ser DEFAULT VALUES;
INSERT INTO rnps_b.ser DEFAULT VALUES;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM rnps_a.ser;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM rnps_b.ser;

-- begin-expected
-- columns: nspname
-- row: rnps_a
-- row: rnps_b
-- end-expected
SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'ser_id_seq' ORDER BY 1;

-- begin-expected
-- columns: schemaname
-- row: rnps_a
-- row: rnps_b
-- end-expected
SELECT schemaname FROM pg_sequences WHERE sequencename = 'ser_id_seq' ORDER BY 1;

-- ============================================================================
-- 4. ALTER SEQUENCE stays in the schema that holds the sequence
-- ============================================================================

CREATE SEQUENCE rnps_a.s2;
SET search_path = rnps_b;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "s2" does not exist
-- end-expected-error
ALTER SEQUENCE s2 RENAME TO s2r;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "rnps_b.s2" does not exist
-- end-expected-error
ALTER SEQUENCE rnps_b.s2 RENAME TO s2r;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "rnps_b.s2" does not exist
-- end-expected-error
ALTER SEQUENCE rnps_b.s2 RESTART WITH 100;

SET search_path = public;
ALTER SEQUENCE rnps_a.s2 RENAME TO s2r;

-- begin-expected
-- columns: nspname, relname
-- row: rnps_a, s2r
-- end-expected
SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname LIKE 's2%' ORDER BY 1, 2;

-- ============================================================================
-- 5. SET SCHEMA really moves the sequence
-- ============================================================================

CREATE SEQUENCE rnps_a.mv;
ALTER SEQUENCE rnps_a.mv SET SCHEMA rnps_b;

-- begin-expected
-- columns: nspname
-- row: rnps_b
-- end-expected
SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'mv';

-- begin-expected
-- columns: nextval
-- row: 1
-- end-expected
SELECT nextval('rnps_b.mv');

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "rnps_a.mv" does not exist
-- end-expected-error
SELECT nextval('rnps_a.mv');

-- ============================================================================
-- 6. An unqualified sequence still resolves through the search path
-- ============================================================================

CREATE SEQUENCE rnps_pubseq;
SET search_path = rnps_b, public;

-- only public holds it, so the path reaches it there
-- begin-expected
-- columns: nextval
-- row: 1
-- end-expected
SELECT nextval('rnps_pubseq');

CREATE SEQUENCE rnps_b.rnps_pubseq;

-- now rnps_b holds one too, and rnps_b comes first
-- begin-expected
-- columns: nextval
-- row: 1
-- end-expected
SELECT nextval('rnps_pubseq');

-- begin-expected
-- columns: nextval
-- row: 2
-- end-expected
SELECT nextval('public.rnps_pubseq');

SET search_path = public;
DROP SEQUENCE rnps_pubseq;

-- ============================================================================
-- 7. Two schemas each hold an index of the same name
-- ============================================================================

CREATE TABLE rnps_a.ib (a int);
CREATE TABLE rnps_b.ib (a int);
CREATE INDEX rnps_ix1 ON rnps_a.ib (a);
CREATE INDEX rnps_ix1 ON rnps_b.ib (a);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname = 'rnps_ix1';

-- begin-expected
-- columns: schemaname, tablename, indexname
-- row: rnps_a, ib, rnps_ix1
-- row: rnps_b, ib, rnps_ix1
-- end-expected
SELECT schemaname, tablename, indexname FROM pg_indexes
WHERE indexname = 'rnps_ix1' ORDER BY 1;

CREATE UNIQUE INDEX rnps_ux1 ON rnps_a.ib (a);
CREATE UNIQUE INDEX rnps_ux1 ON rnps_b.ib (a);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname = 'rnps_ux1';

-- ============================================================================
-- 8. ALTER INDEX and DROP INDEX reach only the schema they name
-- ============================================================================

CREATE TABLE rnps_a.gb (a int);
CREATE INDEX rnps_i1 ON rnps_a.gb (a);
SET search_path = rnps_b;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "rnps_b.rnps_i1" does not exist
-- end-expected-error
ALTER INDEX rnps_b.rnps_i1 RENAME TO rnps_i1r;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "rnps_i1" does not exist
-- end-expected-error
ALTER INDEX rnps_i1 RENAME TO rnps_i1r;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: index "rnps_i1" does not exist
-- end-expected-error
DROP INDEX rnps_b.rnps_i1;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: index "rnps_i1" does not exist
-- end-expected-error
DROP INDEX rnps_i1;

-- begin-expected
-- columns: nspname, relname
-- row: rnps_a, rnps_i1
-- end-expected
SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname LIKE 'rnps_i1%' ORDER BY 1, 2;

SET search_path = public;
ALTER INDEX rnps_a.rnps_i1 RENAME TO rnps_i1r;

-- begin-expected
-- columns: nspname, relname
-- row: rnps_a, rnps_i1r
-- end-expected
SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname LIKE 'rnps_i1%' ORDER BY 1, 2;

DROP INDEX rnps_a.rnps_i1r;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname = 'rnps_i1r';

-- ============================================================================
-- 9. The converse: within one schema every relation kind shares the namespace
-- ============================================================================

CREATE INDEX rnps_ixn ON rnps_a.gb (a);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "rnps_ixn" already exists
-- end-expected-error
CREATE TABLE rnps_a.rnps_ixn (a int);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "gb" already exists
-- end-expected-error
CREATE INDEX gb ON rnps_a.gb (a);

CREATE SEQUENCE rnps_a.rnps_sqn;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "rnps_sqn" already exists
-- end-expected-error
CREATE INDEX rnps_sqn ON rnps_a.gb (a);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "rnps_ixn" already exists
-- end-expected-error
CREATE SEQUENCE rnps_a.rnps_ixn;

-- A quoted name differing only in case is a second index
CREATE INDEX rnps_cix ON rnps_a.gb (a);
CREATE INDEX "rnps_cIX" ON rnps_a.gb (a);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname IN ('rnps_cix', 'rnps_cIX');

-- ============================================================================
-- 10. A sequence and an index go with the schema that holds them
-- ============================================================================

CREATE SEQUENCE rnps_a.dead;
DROP SCHEMA rnps_a CASCADE;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname = 'dead';

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "dead" does not exist
-- end-expected-error
SELECT nextval('dead');

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname = 'rnps_ix1';

DROP SCHEMA rnps_b CASCADE;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname = 'rnps_ix1';
