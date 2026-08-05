-- ============================================================================
-- Feature Comparison: what a relation's schema means once names may repeat
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Giving sequences and indexes a schema of their own made a name collision
-- possible that could not happen before, and the code that moves an object
-- with its table did not check for it: ALTER TABLE a.t SET SCHEMA b overwrote
-- whatever b already held under the moved index's or sequence's name, counter
-- and all. PostgreSQL refuses the whole statement and leaves both schemas as
-- it found them.
--
-- The same schema-awareness has to reach the places that only READ a name.
-- A column default draws from one particular sequence wherever it lives, so a
-- cross-schema dependency blocks a DROP and follows a SET SCHEMA; a qualified
-- read of a sequence-as-relation and pg_get_indexdef answer about the object
-- the name really reaches; a qualified ALTER INDEX reaches a constraint-backed
-- index; and pg_temp is the alias a session's temporary schema answers to.
--
-- Finally: a relation of the wrong kind is refused for what it is (42809),
-- not reported missing (42P01), and a wrong-kind DROP names the statement
-- that would have worked.
-- ============================================================================

DROP SCHEMA IF EXISTS rfu_a CASCADE;
DROP SCHEMA IF EXISTS rfu_b CASCADE;
DROP TABLE IF EXISTS public.rfu_mt;
DROP SEQUENCE IF EXISTS public.rfu_pubseq;
DROP SEQUENCE IF EXISTS public.rfu_moving;
CREATE SCHEMA rfu_a;
CREATE SCHEMA rfu_b;
SET search_path = public;

-- ============================================================================
-- 1. ALTER TABLE ... SET SCHEMA refuses to overwrite an index in the target
-- ============================================================================

CREATE TABLE rfu_a.t1 (c int);
CREATE TABLE rfu_b.other (c int);
CREATE INDEX rfu_ix ON rfu_a.t1 (c);
CREATE INDEX rfu_ix ON rfu_b.other (c);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "rfu_ix" already exists in schema "rfu_b"
-- end-expected-error
ALTER TABLE rfu_a.t1 SET SCHEMA rfu_b;

-- Nothing moved: both indexes are still where they were.
-- begin-expected
-- columns: schemaname, tablename, indexname
-- row: rfu_a, t1, rfu_ix
-- row: rfu_b, other, rfu_ix
-- end-expected
SELECT schemaname, tablename, indexname FROM pg_indexes
 WHERE indexname = 'rfu_ix' ORDER BY 1, 2;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname = 'rfu_ix';

DROP TABLE rfu_a.t1;
DROP TABLE rfu_b.other;

-- ============================================================================
-- 2. ... and refuses to overwrite a sequence, whose counter it would destroy
-- ============================================================================

CREATE TABLE rfu_a.t2 (id serial, v text);
CREATE SEQUENCE rfu_b.t2_id_seq;
SELECT setval('rfu_b.t2_id_seq', 900);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "t2_id_seq" already exists in schema "rfu_b"
-- end-expected-error
ALTER TABLE rfu_a.t2 SET SCHEMA rfu_b;

-- The 900 is the whole point: an overwriting move lost it and reset it to 1.
-- begin-expected
-- columns: last_value
-- row: 900
-- end-expected
SELECT last_value FROM rfu_b.t2_id_seq;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname = 't2_id_seq';

DROP TABLE rfu_a.t2;
DROP SEQUENCE rfu_b.t2_id_seq;

-- ============================================================================
-- 3. A move whose names are free carries the sequence and the indexes along
-- ============================================================================

CREATE TABLE rfu_a.t3 (id serial, v text);
CREATE INDEX rfu_vx ON rfu_a.t3 (v);
INSERT INTO rfu_a.t3 (v) VALUES ('a');
ALTER TABLE rfu_a.t3 SET SCHEMA rfu_b;
INSERT INTO rfu_b.t3 (v) VALUES ('b');

-- begin-expected
-- columns: id, v
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT id, v FROM rfu_b.t3 ORDER BY id;

-- begin-expected
-- columns: schemaname, tablename, indexname
-- row: rfu_b, t3, rfu_vx
-- end-expected
SELECT schemaname, tablename, indexname FROM pg_indexes WHERE indexname = 'rfu_vx';

-- begin-expected
-- columns: last_value
-- row: 2
-- end-expected
SELECT last_value FROM rfu_b.t3_id_seq;

DROP TABLE rfu_b.t3;

-- ============================================================================
-- 4. A table in one schema may depend on a sequence in another
-- ============================================================================

CREATE SEQUENCE public.rfu_pubseq;
CREATE TABLE rfu_a.dep (id int DEFAULT nextval('rfu_pubseq'), v text);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop sequence rfu_pubseq because other objects depend on it
-- end-expected-error
DROP SEQUENCE public.rfu_pubseq;

-- The sequence survived, so the INSERT still has a number to draw.
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname = 'rfu_pubseq';

INSERT INTO rfu_a.dep (v) VALUES ('a');

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) AS count FROM rfu_a.dep;

-- CASCADE clears the default rather than stranding it as text nothing can read
DROP SEQUENCE public.rfu_pubseq CASCADE;

-- begin-expected
-- columns: column_default
-- row: NULL
-- end-expected
SELECT column_default FROM information_schema.columns
 WHERE table_schema = 'rfu_a' AND table_name = 'dep' AND column_name = 'id';

INSERT INTO rfu_a.dep (v) VALUES ('b');

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM rfu_a.dep;

DROP TABLE rfu_a.dep;

-- ============================================================================
-- 5. Moving a sequence takes the defaults that draw from it along
-- ============================================================================

CREATE SEQUENCE public.rfu_moving;
CREATE TABLE public.rfu_mt (id int DEFAULT nextval('rfu_moving'), v text);
INSERT INTO public.rfu_mt (v) VALUES ('a');
ALTER SEQUENCE public.rfu_moving SET SCHEMA rfu_a;
INSERT INTO public.rfu_mt (v) VALUES ('b');

-- begin-expected
-- columns: id, v
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT id, v FROM public.rfu_mt ORDER BY id;

-- begin-expected
-- columns: column_default
-- row: nextval('rfu_a.rfu_moving'::regclass)
-- end-expected
SELECT column_default FROM information_schema.columns
 WHERE table_schema = 'public' AND table_name = 'rfu_mt' AND column_name = 'id';

DROP TABLE public.rfu_mt;
DROP SEQUENCE rfu_a.rfu_moving;

-- ============================================================================
-- 6. Reading a sequence as a relation answers about the schema it names
-- ============================================================================

CREATE SEQUENCE public.rfu_rs;
CREATE SEQUENCE rfu_a.rfu_rs;
SELECT setval('rfu_a.rfu_rs', 100);
SELECT setval('public.rfu_rs', 5);

-- begin-expected
-- columns: last_value
-- row: 100
-- end-expected
SELECT last_value FROM rfu_a.rfu_rs;

-- begin-expected
-- columns: last_value, log_cnt, is_called
-- row: 100, 0, true
-- end-expected
SELECT last_value, log_cnt, is_called FROM rfu_a.rfu_rs;

-- begin-expected
-- columns: last_value
-- row: 5
-- end-expected
SELECT last_value FROM public.rfu_rs;

-- A schema that holds no sequence of the name answers with nothing at all,
-- not with a row borrowed from public.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "rfu_b.rfu_rs" does not exist
-- end-expected-error
SELECT last_value FROM rfu_b.rfu_rs;

DROP SEQUENCE public.rfu_rs;
DROP SEQUENCE rfu_a.rfu_rs;

-- ============================================================================
-- 7. pg_get_indexdef answers about the index its OID names
-- ============================================================================

CREATE TABLE rfu_a.t7 (a int, b int);
CREATE TABLE rfu_b.t7 (a int, b int);
CREATE INDEX rfu_i7 ON rfu_a.t7 (a);
CREATE UNIQUE INDEX rfu_i7 ON rfu_b.t7 (b);

-- begin-expected
-- columns: pg_get_indexdef
-- row: CREATE UNIQUE INDEX rfu_i7 ON rfu_b.t7 USING btree (b)
-- end-expected
SELECT pg_get_indexdef('rfu_b.rfu_i7'::regclass);

-- begin-expected
-- columns: pg_get_indexdef
-- row: CREATE INDEX rfu_i7 ON rfu_a.t7 USING btree (a)
-- end-expected
SELECT pg_get_indexdef('rfu_a.rfu_i7'::regclass);

DROP TABLE rfu_a.t7;
DROP TABLE rfu_b.t7;

-- ============================================================================
-- 8. A qualified ALTER INDEX reaches a constraint-backed index
-- ============================================================================

CREATE TABLE rfu_a.t8 (id int CONSTRAINT rfu_pk PRIMARY KEY, u int CONSTRAINT rfu_uq UNIQUE);

ALTER INDEX rfu_a.rfu_pk RENAME TO rfu_pk2;

-- begin-expected
-- columns: schemaname, indexname
-- row: rfu_a, rfu_pk2
-- end-expected
SELECT schemaname, indexname FROM pg_indexes WHERE indexname IN ('rfu_pk', 'rfu_pk2');

-- The qualifier is still honoured: a schema holding no such index is a miss.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "rfu_b.rfu_uq" does not exist
-- end-expected-error
ALTER INDEX rfu_b.rfu_uq RENAME TO rfu_uq2;

-- begin-expected
-- columns: schemaname, indexname
-- row: rfu_a, rfu_uq
-- end-expected
SELECT schemaname, indexname FROM pg_indexes WHERE indexname IN ('rfu_uq', 'rfu_uq2');

DROP TABLE rfu_a.t8;

-- ============================================================================
-- 9. pg_temp is the alias this session's temporary schema answers to
-- ============================================================================

CREATE TEMP SEQUENCE rfu_tseq;

-- begin-expected
-- columns: nextval
-- row: 1
-- end-expected
SELECT nextval('rfu_tseq');

-- begin-expected
-- columns: nextval
-- row: 2
-- end-expected
SELECT nextval('pg_temp.rfu_tseq');

DROP SEQUENCE pg_temp.rfu_tseq;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "rfu_tseq" does not exist
-- end-expected-error
SELECT nextval('rfu_tseq');

-- ============================================================================
-- 10. A relation of the wrong kind is refused for what it is
-- ============================================================================

CREATE TABLE rfu_a.wb (a int);
CREATE INDEX rfu_wi ON rfu_a.wb (a);
CREATE SEQUENCE rfu_a.rfu_ws;
CREATE VIEW rfu_a.rfu_wv AS SELECT 1 AS x;
CREATE MATERIALIZED VIEW rfu_a.rfu_wm AS SELECT 1 AS x;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "wb" is not a view
-- end-expected-error
ALTER VIEW rfu_a.wb ALTER COLUMN a SET DEFAULT 1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "wb" is not a materialized view
-- end-expected-error
ALTER MATERIALIZED VIEW rfu_a.wb RENAME TO wb2;

-- A plain view is the wrong kind for it too.
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "rfu_wv" is not a materialized view
-- end-expected-error
ALTER MATERIALIZED VIEW rfu_a.rfu_wv RENAME TO rfu_wv2;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "wb" is not an index
-- end-expected-error
ALTER INDEX rfu_a.wb SET (fillfactor = 50);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "wb" is not an index
-- end-expected-error
ALTER INDEX rfu_a.wb RESET (fillfactor);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "rfu_wi" is not a table
-- end-expected-error
TRUNCATE rfu_a.rfu_wi;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "rfu_ws" is not a table
-- end-expected-error
TRUNCATE rfu_a.rfu_ws;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "rfu_wi"
-- end-expected-error
SELECT * FROM rfu_a.rfu_wi;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "rfu_wi"
-- end-expected-error
INSERT INTO rfu_a.rfu_wi VALUES (1);

-- ============================================================================
-- 11. A wrong-kind DROP is refused for the kind the object really is
--
-- PostgreSQL adds a Hint naming the DROP that would have worked ("Use DROP
-- INDEX to remove an index."), and so does memgres now; the corpus compares
-- primary messages only, so the Hint text itself is asserted in
-- RelationNamespaceFollowUpTest instead.
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "rfu_wi" is not a table
-- end-expected-error
DROP TABLE rfu_a.rfu_wi;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "rfu_ws" is not a table
-- end-expected-error
DROP TABLE rfu_a.rfu_ws;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "rfu_wv" is not a table
-- end-expected-error
DROP TABLE rfu_a.rfu_wv;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "rfu_wm" is not a table
-- end-expected-error
DROP TABLE rfu_a.rfu_wm;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "wb" is not an index
-- end-expected-error
DROP INDEX rfu_a.wb;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "wb" is not a sequence
-- end-expected-error
DROP SEQUENCE rfu_a.wb;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "rfu_wm" is not a view
-- end-expected-error
DROP VIEW rfu_a.rfu_wm;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "rfu_wv" is not a materialized view
-- end-expected-error
DROP MATERIALIZED VIEW rfu_a.rfu_wv;

DROP SCHEMA rfu_a CASCADE;
DROP SCHEMA rfu_b CASCADE;
