-- ============================================================================
-- Feature Comparison: residual DDL validation
-- Target: PostgreSQL 18 vs Memgres
--
-- A schema definition is written once and lived with for years, so the checks
-- PostgreSQL makes while it is being written are what stop a migration from
-- succeeding here and failing there. This file covers the ones that were still
-- missing: which relations share a name, what a view really depends on, whether
-- the object an ALTER names exists at all, whether the rows already stored can
-- satisfy a rule about to be declared over them, and the order a multi-action
-- ALTER TABLE settles its actions in.
--
-- Each section pairs the refusals with the neighbouring definitions that must
-- keep being accepted: a rule that fires on correct SQL costs more than the
-- permissiveness it removes.
-- ============================================================================

DROP TABLE IF EXISTS d11_ns_t CASCADE;
DROP TABLE IF EXISTS d11_dep CASCADE;
DROP TABLE IF EXISTS d11_dep2 CASCADE;
DROP TABLE IF EXISTS d11_real CASCADE;
DROP SCHEMA IF EXISTS d11_s CASCADE;

-- ============================================================================
-- SECTION A: DROP TABLE asks what the view reads, not what its text says
-- ============================================================================

CREATE TABLE d11_dep (a int PRIMARY KEY);
CREATE TABLE d11_dep2 (a int PRIMARY KEY);
CREATE VIEW d11_v_lit AS SELECT a, 'd11_dep' AS why FROM d11_dep2;

-- The name occurs in a string literal. Nothing reads the table.
DROP TABLE d11_dep;

CREATE TABLE d11_pre (a int PRIMARY KEY);
CREATE TABLE d11_pre_long (a int PRIMARY KEY);
CREATE VIEW d11_v_pre AS SELECT a FROM d11_pre_long;

-- A relation whose name merely starts the same way is a different relation.
DROP TABLE d11_pre;

CREATE TABLE d11_col (a int PRIMARY KEY);
CREATE TABLE d11_colhost (id int PRIMARY KEY, d11_col int);
CREATE VIEW d11_v_col AS SELECT d11_col FROM d11_colhost;

-- The name is a column somewhere else.
DROP TABLE d11_col;

CREATE TABLE d11_cte (a int PRIMARY KEY);
CREATE TABLE d11_cte_src (a int PRIMARY KEY);
CREATE VIEW d11_v_cte AS WITH d11_cte AS (SELECT a FROM d11_cte_src) SELECT * FROM d11_cte;

-- A WITH of that name shadows the table for the whole query.
DROP TABLE d11_cte;

CREATE SCHEMA d11_s;
CREATE TABLE d11_s.d11_sch (a int PRIMARY KEY);
CREATE TABLE d11_sch (a int PRIMARY KEY);
CREATE VIEW d11_v_sch AS SELECT a FROM d11_s.d11_sch;

-- The view reads the other schema's table of that name.
DROP TABLE d11_sch;

CREATE TABLE d11_gone (a int PRIMARY KEY);
CREATE VIEW d11_v_gone AS SELECT a FROM d11_gone;
DROP VIEW d11_v_gone;

-- The dependent view has already been dropped.
DROP TABLE d11_gone;

-- A dependency reached through an alias still blocks the drop.
CREATE TABLE d11_real (a int PRIMARY KEY);
CREATE VIEW d11_v_real AS SELECT z.a FROM d11_real AS z;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table d11_real because other objects depend on it
-- end-expected-error
DROP TABLE d11_real;

-- ... and one reached only inside a scalar subquery does too.
CREATE TABLE d11_sub (a int PRIMARY KEY);
CREATE VIEW d11_v_sub AS SELECT (SELECT count(*) FROM d11_sub) AS c;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table d11_sub because other objects depend on it
-- end-expected-error
DROP TABLE d11_sub;

-- CASCADE reaches the view over the view as well as the view over the table.
CREATE TABLE d11_t1 (a int PRIMARY KEY);
CREATE VIEW d11_v1 AS SELECT a FROM d11_t1;
CREATE VIEW d11_v2 AS SELECT a FROM d11_v1;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table d11_t1 because other objects depend on it
-- end-expected-error
DROP TABLE d11_t1;

DROP TABLE d11_t1 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM information_schema.views WHERE table_name IN ('d11_v1','d11_v2');

-- ============================================================================
-- SECTION B: tables, views, sequences, indexes and matviews share one name
-- ============================================================================

CREATE TABLE d11_ns_t (a int PRIMARY KEY);
CREATE VIEW d11_ns_v AS SELECT a FROM d11_ns_t;
CREATE SEQUENCE d11_ns_s;
CREATE INDEX d11_ns_i ON d11_ns_t (a);
CREATE MATERIALIZED VIEW d11_ns_m AS SELECT a FROM d11_ns_t;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d11_ns_v" already exists
-- end-expected-error
CREATE TABLE d11_ns_v (x int);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d11_ns_s" already exists
-- end-expected-error
CREATE TABLE d11_ns_s (x int);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d11_ns_i" already exists
-- end-expected-error
CREATE TABLE d11_ns_i (x int);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d11_ns_m" already exists
-- end-expected-error
CREATE TABLE d11_ns_m (x int);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d11_ns_t" already exists
-- end-expected-error
CREATE VIEW d11_ns_t AS SELECT 1 AS x;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d11_ns_i" already exists
-- end-expected-error
CREATE VIEW d11_ns_i AS SELECT 1 AS x;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d11_ns_t" already exists
-- end-expected-error
CREATE SEQUENCE d11_ns_t;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d11_ns_v" already exists
-- end-expected-error
CREATE SEQUENCE d11_ns_v;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d11_ns_s" already exists
-- end-expected-error
CREATE INDEX d11_ns_s ON d11_ns_t (a);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d11_ns_t" already exists
-- end-expected-error
CREATE MATERIALIZED VIEW d11_ns_t AS SELECT 1 AS x;

-- IF NOT EXISTS still means "there is one already, carry on".
CREATE TABLE IF NOT EXISTS d11_ns_v (x int);

-- DROP names a kind, and IF EXISTS does not turn the wrong kind into nothing.

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d11_ns_i" is not a table
-- end-expected-error
DROP TABLE d11_ns_i;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d11_ns_i" is not a table
-- end-expected-error
DROP TABLE IF EXISTS d11_ns_i;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d11_ns_v" is not a table
-- end-expected-error
DROP TABLE d11_ns_v;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d11_ns_m" is not a table
-- end-expected-error
DROP TABLE d11_ns_m;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d11_ns_t" is not a view
-- end-expected-error
DROP VIEW d11_ns_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d11_ns_i" is not a view
-- end-expected-error
DROP VIEW d11_ns_i;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d11_ns_t" is not a sequence
-- end-expected-error
DROP SEQUENCE d11_ns_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d11_ns_v" is not a sequence
-- end-expected-error
DROP SEQUENCE d11_ns_v;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d11_ns_t" is not an index
-- end-expected-error
DROP INDEX d11_ns_t;

-- A rename lands in the same namespace as a create.

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d11_ns_v" already exists
-- end-expected-error
ALTER TABLE d11_ns_t RENAME TO d11_ns_v;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d11_ns_t" already exists
-- end-expected-error
ALTER VIEW d11_ns_v RENAME TO d11_ns_t;

-- ... and every ordinary drop and rename must keep working.
DROP VIEW d11_ns_v;
DROP MATERIALIZED VIEW d11_ns_m;
DROP INDEX d11_ns_i;
DROP SEQUENCE d11_ns_s;
DROP TABLE IF EXISTS d11_ns_nosuch;
DROP VIEW IF EXISTS d11_ns_nosuch;
DROP SEQUENCE IF EXISTS d11_ns_nosuch;
DROP INDEX IF EXISTS d11_ns_nosuch;
ALTER TABLE d11_ns_t RENAME TO d11_ns_t2;
ALTER TABLE d11_ns_t2 RENAME TO d11_ns_t;
CREATE VIEW d11_ns_v AS SELECT a FROM d11_ns_t;
CREATE OR REPLACE VIEW d11_ns_v AS SELECT a FROM d11_ns_t;
CREATE SEQUENCE d11_ns_s;
CREATE INDEX d11_ns_i ON d11_ns_t (a);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM information_schema.views WHERE table_name = 'd11_ns_v';

-- ============================================================================
-- SECTION C: ALTER on an object that was never created
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "d11_nosuch" for encoding "UTF8" does not exist
-- end-expected-error
ALTER COLLATION d11_nosuch RENAME TO d11_o;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: conversion "d11_nosuch" does not exist
-- end-expected-error
ALTER CONVERSION d11_nosuch RENAME TO d11_o;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: language "d11_nosuch" does not exist
-- end-expected-error
ALTER LANGUAGE d11_nosuch OWNER TO CURRENT_USER;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: tablespace "d11_nosuch" does not exist
-- end-expected-error
ALTER TABLESPACE d11_nosuch RENAME TO d11_o;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: rule "d11_nosuch" for relation "d11_ns_t" does not exist
-- end-expected-error
ALTER RULE d11_nosuch ON d11_ns_t RENAME TO d11_o;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: trigger "d11_nosuch" for table "d11_ns_t" does not exist
-- end-expected-error
ALTER TRIGGER d11_nosuch ON d11_ns_t RENAME TO d11_o;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function d11_nosuch() does not exist
-- end-expected-error
ALTER FUNCTION d11_nosuch() RENAME TO d11_o;

-- Renaming onto a name another object already answers to.
CREATE TYPE d11_ta AS ENUM ('x');
CREATE TYPE d11_tb AS ENUM ('y');

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "d11_tb" already exists
-- end-expected-error
ALTER TYPE d11_ta RENAME TO d11_tb;

ALTER TYPE d11_ta RENAME TO d11_tc;

-- The same statements on objects that do exist must keep working.
CREATE FUNCTION d11_tf() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;
CREATE TRIGGER d11_gt BEFORE INSERT ON d11_ns_t FOR EACH ROW EXECUTE FUNCTION d11_tf();
ALTER TRIGGER d11_gt ON d11_ns_t RENAME TO d11_gt2;
CREATE RULE d11_r1 AS ON INSERT TO d11_ns_t DO INSTEAD NOTHING;
ALTER RULE d11_r1 ON d11_ns_t RENAME TO d11_r2;
DROP RULE d11_r2 ON d11_ns_t;
ALTER LANGUAGE plpgsql OWNER TO CURRENT_USER;

-- begin-expected
-- columns: tgname
-- row: d11_gt2
-- end-expected
SELECT tgname FROM pg_trigger WHERE tgrelid = 'd11_ns_t'::regclass AND NOT tgisinternal ORDER BY tgname;

-- ============================================================================
-- SECTION D: ALTER TYPE attributes, and ALTER DOMAIN against stored rows
-- ============================================================================

CREATE TYPE d11_ct AS (a int, b text);

-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "a" of relation "d11_ct" already exists
-- end-expected-error
ALTER TYPE d11_ct ADD ATTRIBUTE a int;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "d11_ct" does not exist
-- end-expected-error
ALTER TYPE d11_ct DROP ATTRIBUTE nosuch;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
ALTER TYPE d11_ct RENAME ATTRIBUTE nosuch TO z;

-- IF EXISTS makes the missing attribute a notice instead.
ALTER TYPE d11_ct DROP ATTRIBUTE IF EXISTS nosuch;
ALTER TYPE d11_ct ADD ATTRIBUTE c int;
ALTER TYPE d11_ct RENAME ATTRIBUTE c TO d;
ALTER TYPE d11_ct ALTER ATTRIBUTE d TYPE bigint;
ALTER TYPE d11_ct DROP ATTRIBUTE d;

-- A domain is declared over columns that already hold values.
CREATE DOMAIN d11_dom AS int;
CREATE TABLE d11_domt (id int PRIMARY KEY, a d11_dom);
INSERT INTO d11_domt VALUES (1, NULL);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "a" of table "d11_domt" contains null values
-- end-expected-error
ALTER DOMAIN d11_dom SET NOT NULL;

DELETE FROM d11_domt WHERE id = 1;
ALTER DOMAIN d11_dom SET NOT NULL;
ALTER DOMAIN d11_dom DROP NOT NULL;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "nosuch" of domain "d11_dom" does not exist
-- end-expected-error
ALTER DOMAIN d11_dom DROP CONSTRAINT nosuch;

ALTER DOMAIN d11_dom DROP CONSTRAINT IF EXISTS nosuch;
INSERT INTO d11_domt VALUES (2, 5);
ALTER DOMAIN d11_dom ADD CONSTRAINT d11_dc CHECK (VALUE > 10) NOT VALID;

-- Marking it valid is a claim about the rows let through while it was not.
-- begin-expected-error
-- sqlstate: 23514
-- message-like: column "a" of table "d11_domt" contains values that violate the new constraint
-- end-expected-error
ALTER DOMAIN d11_dom VALIDATE CONSTRAINT d11_dc;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: column "a" of table "d11_domt" contains values that violate the new constraint
-- end-expected-error
ALTER DOMAIN d11_dom ADD CONSTRAINT d11_dc2 CHECK (VALUE > 10);

DELETE FROM d11_domt WHERE id = 2;
ALTER DOMAIN d11_dom VALIDATE CONSTRAINT d11_dc;
ALTER DOMAIN d11_dom DROP CONSTRAINT d11_dc;
INSERT INTO d11_domt VALUES (3, 20);

-- begin-expected
-- columns: id | a
-- row: 3, 20
-- end-expected
SELECT id, a FROM d11_domt ORDER BY id;

-- ============================================================================
-- SECTION E: ALTER COLUMN changes that contradict the column
-- ============================================================================

CREATE TABLE d11_cc (id int PRIMARY KEY,
                     v int GENERATED ALWAYS AS (id * 2) STORED,
                     w int,
                     x int GENERATED ALWAYS AS IDENTITY);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: column "id" is in a primary key
-- end-expected-error
ALTER TABLE d11_cc ALTER COLUMN id DROP NOT NULL;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: column "v" of relation "d11_cc" is a generated column
-- end-expected-error
ALTER TABLE d11_cc ALTER COLUMN v SET DEFAULT 5;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: column "v" of relation "d11_cc" is a generated column
-- end-expected-error
ALTER TABLE d11_cc ALTER COLUMN v DROP DEFAULT;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: is not an identity column
-- end-expected-error
ALTER TABLE d11_cc ALTER COLUMN w DROP IDENTITY;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "d11_nosuchschema" does not exist
-- end-expected-error
ALTER TABLE d11_cc SET SCHEMA d11_nosuchschema;

-- The same actions on the columns they suit must keep working.
ALTER TABLE d11_cc ALTER COLUMN w DROP IDENTITY IF EXISTS;
ALTER TABLE d11_cc ALTER COLUMN w SET DEFAULT 5;
ALTER TABLE d11_cc ALTER COLUMN w DROP DEFAULT;
ALTER TABLE d11_cc ALTER COLUMN w SET NOT NULL;
ALTER TABLE d11_cc ALTER COLUMN w DROP NOT NULL;
-- Dropping the identity leaves the column's NOT NULL behind, as PostgreSQL does.
ALTER TABLE d11_cc ALTER COLUMN x DROP IDENTITY;
ALTER TABLE d11_cc ALTER COLUMN x DROP NOT NULL;
INSERT INTO d11_cc (id, w) VALUES (1, 7);

-- begin-expected
-- columns: id | v | w
-- row: 1, 2, 7
-- end-expected
SELECT id, v, w FROM d11_cc ORDER BY id;

-- Once the key is gone the column may be made nullable.
ALTER TABLE d11_cc DROP CONSTRAINT d11_cc_pkey;
ALTER TABLE d11_cc ALTER COLUMN id DROP NOT NULL;

-- ============================================================================
-- SECTION F: a multi-action ALTER TABLE is one statement, not a script
-- ============================================================================

CREATE TABLE d11_ma (id int PRIMARY KEY, a int);
INSERT INTO d11_ma VALUES (1, 1);

-- The constraint is written first and still reads a column added later.
ALTER TABLE d11_ma ADD CONSTRAINT d11_ck CHECK (b > 0), ADD COLUMN b int NOT NULL DEFAULT 5;

-- begin-expected
-- columns: id | a | b
-- row: 1, 1, 5
-- end-expected
SELECT id, a, b FROM d11_ma ORDER BY id;

-- Everything dropped is dropped first, so this one looks for a column that is
-- not there yet.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "c" of relation "d11_ma" does not exist
-- end-expected-error
ALTER TABLE d11_ma ADD COLUMN c text DEFAULT 'z', DROP COLUMN c;

-- ... and the same two actions the other way round replace the column.
ALTER TABLE d11_ma DROP COLUMN b, ADD COLUMN b int DEFAULT 9;

-- begin-expected
-- columns: column_name
-- row: a
-- row: b
-- row: id
-- end-expected
SELECT column_name FROM information_schema.columns WHERE table_name = 'd11_ma' ORDER BY column_name;

-- Neighbouring multi-action shapes must keep working.
ALTER TABLE d11_ma ADD COLUMN d int, ALTER COLUMN d SET DEFAULT 3;
ALTER TABLE d11_ma ADD COLUMN e int, ADD CONSTRAINT d11_ck2 CHECK (e IS NULL OR e > 0);
ALTER TABLE d11_ma ALTER COLUMN a TYPE bigint, ADD COLUMN f bigint;
ALTER TABLE d11_ma DROP CONSTRAINT d11_ck2, ADD CONSTRAINT d11_ck2 CHECK (e IS NULL OR e >= 0);

-- begin-expected
-- columns: column_name
-- row: a
-- row: b
-- row: d
-- row: e
-- row: f
-- row: id
-- end-expected
SELECT column_name FROM information_schema.columns WHERE table_name = 'd11_ma' ORDER BY column_name;

-- ============================================================================
-- SECTION G: a child inherits one column, so its parents have to agree
-- ============================================================================

CREATE TABLE d11_p1 (shared int, x int);
CREATE TABLE d11_p2 (shared bigint, y int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: inherited column "shared" has a type conflict
-- end-expected-error
CREATE TABLE d11_c1 () INHERITS (d11_p1, d11_p2);

CREATE TABLE d11_p3 (tomorrow date DEFAULT '2001-01-01');
CREATE TABLE d11_p4 (tomorrow date DEFAULT '2002-02-02');

-- begin-expected-error
-- sqlstate: 42611
-- message-like: column "tomorrow" inherits conflicting default values
-- end-expected-error
CREATE TABLE d11_c2 () INHERITS (d11_p3, d11_p4);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "shared" has a type conflict
-- end-expected-error
CREATE TABLE d11_c3 (shared bigint) INHERITS (d11_p1);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: would be inherited from more than once
-- end-expected-error
CREATE TABLE d11_c9 () INHERITS (d11_p1, d11_p1);

-- Parents that do agree merge the column, as before.
CREATE TABLE d11_p5 (shared int, z int);
CREATE TABLE d11_c4 () INHERITS (d11_p1, d11_p5);
CREATE TABLE d11_c5 (shared int) INHERITS (d11_p1);
CREATE TABLE d11_p7 (tomorrow date DEFAULT '2001-01-01');
CREATE TABLE d11_c7 () INHERITS (d11_p3, d11_p7);
CREATE TABLE d11_c8 (tomorrow date DEFAULT '2009-09-09') INHERITS (d11_p3, d11_p4);

-- begin-expected
-- columns: column_name
-- row: shared
-- row: x
-- row: z
-- end-expected
SELECT column_name FROM information_schema.columns WHERE table_name = 'd11_c4' ORDER BY column_name;

-- A NOT NULL declared on the parent reaches the child's rows.
CREATE TABLE d11_p6 (id int PRIMARY KEY, a int NOT NULL);
CREATE TABLE d11_c6 () INHERITS (d11_p6);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "a" of relation "d11_c6" violates not-null constraint
-- end-expected-error
INSERT INTO d11_c6 (id, a) VALUES (1, NULL);

-- ============================================================================
-- SECTION H: a partition key must answer the same way every time
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in partition key expression must be marked IMMUTABLE
-- end-expected-error
CREATE TABLE d11_pk1 (i int) PARTITION BY RANGE ((random()));

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in partition key expression must be marked IMMUTABLE
-- end-expected-error
CREATE TABLE d11_pk4 (i timestamptz) PARTITION BY RANGE ((now()));

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in partition key expression must be marked IMMUTABLE
-- end-expected-error
CREATE TABLE d11_pk7 (t timestamptz) PARTITION BY RANGE ((t::date));

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in partition key expression must be marked IMMUTABLE
-- end-expected-error
CREATE TABLE d11_pk10 (i int) PARTITION BY RANGE ((i + random()::int));

-- Immutable expression keys must keep being accepted.
CREATE TABLE d11_pk2 (i int) PARTITION BY RANGE ((i * 2));
CREATE TABLE d11_pk3 (i int) PARTITION BY LIST ((i % 4));
CREATE TABLE d11_pk5 (i int) PARTITION BY RANGE ((abs(i)));
CREATE TABLE d11_pk6 (t text) PARTITION BY RANGE ((lower(t)));
CREATE TABLE d11_pk8 (i int) PARTITION BY HASH ((i + 1));
CREATE TABLE d11_pk9 (i int) PARTITION BY RANGE (i);
CREATE TABLE d11_pk9a PARTITION OF d11_pk9 FOR VALUES FROM (0) TO (10);
INSERT INTO d11_pk9 VALUES (1), (2);

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT i FROM d11_pk9 ORDER BY i;

-- ============================================================================
-- SECTION I: ALTER TABLE ALTER CONSTRAINT knows which kind it is looking at
-- ============================================================================

CREATE TABLE d11_ac_p (id int PRIMARY KEY);
CREATE TABLE d11_ac (id int PRIMARY KEY, p int, q int,
                     CONSTRAINT d11_fk FOREIGN KEY (p) REFERENCES d11_ac_p(id));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: constraint declared INITIALLY DEFERRED must be DEFERRABLE
-- end-expected-error
ALTER TABLE d11_ac ALTER CONSTRAINT d11_fk NOT DEFERRABLE INITIALLY DEFERRED;

-- Only a not-null constraint has inheritability to alter.
-- begin-expected-error
-- sqlstate: 42809
-- message-like: constraint "d11_fk" of relation "d11_ac" is not a not-null constraint
-- end-expected-error
ALTER TABLE d11_ac ALTER CONSTRAINT d11_fk NO INHERIT;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: constraints cannot be altered to be NOT VALID
-- end-expected-error
ALTER TABLE d11_ac ALTER CONSTRAINT d11_fk NOT VALID;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "d11_nosuch" of relation "d11_ac" does not exist
-- end-expected-error
ALTER TABLE d11_ac ALTER CONSTRAINT d11_nosuch NOT DEFERRABLE;

ALTER TABLE d11_ac ALTER CONSTRAINT d11_fk DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE d11_ac ALTER CONSTRAINT d11_fk NOT DEFERRABLE;

-- A named NOT NULL constraint is the kind INHERIT / NO INHERIT is for.
ALTER TABLE d11_ac ADD CONSTRAINT d11_nn NOT NULL q;
ALTER TABLE d11_ac ALTER CONSTRAINT d11_nn NO INHERIT;
ALTER TABLE d11_ac ALTER CONSTRAINT d11_nn INHERIT;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "q" of relation "d11_ac" violates not-null constraint
-- end-expected-error
INSERT INTO d11_ac (id, q) VALUES (1, NULL);

ALTER TABLE d11_ac DROP CONSTRAINT d11_nn;
INSERT INTO d11_ac (id, q) VALUES (2, NULL);

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
SELECT id FROM d11_ac ORDER BY id;

-- A check constraint is neither of those kinds.
ALTER TABLE d11_ac ADD CONSTRAINT d11_ck3 CHECK (q IS NULL OR q > 0);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: constraint "d11_ck3" of relation "d11_ac" is not a not-null constraint
-- end-expected-error
ALTER TABLE d11_ac ALTER CONSTRAINT d11_ck3 NO INHERIT;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: constraint "d11_ck3" of relation "d11_ac" is not a foreign key constraint
-- end-expected-error
ALTER TABLE d11_ac ALTER CONSTRAINT d11_ck3 DEFERRABLE;

-- ============================================================================
-- SECTION J: CREATE TRIGGER, and errors raised inside a WHEN condition
-- ============================================================================

CREATE TABLE d11_tt (id int PRIMARY KEY, v int, d int);
CREATE VIEW d11_tv AS SELECT id, v FROM d11_tt;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: INSTEAD OF triggers cannot have column lists
-- end-expected-error
CREATE TRIGGER d11_g8 INSTEAD OF UPDATE OF id ON d11_tv FOR EACH ROW EXECUTE FUNCTION d11_tf();

CREATE TRIGGER d11_g1 BEFORE INSERT ON d11_tt FOR EACH ROW EXECUTE FUNCTION d11_tf();

-- begin-expected-error
-- sqlstate: 42710
-- message-like: trigger "d11_g1" for relation "d11_tt" already exists
-- end-expected-error
CREATE TRIGGER d11_g1 AFTER UPDATE ON d11_tt FOR EACH ROW EXECUTE FUNCTION d11_tf();

-- The same trigger name on another relation is a different trigger.
CREATE TRIGGER d11_g1 BEFORE INSERT ON d11_ma FOR EACH ROW EXECUTE FUNCTION d11_tf();

-- begin-expected
-- columns: tgname
-- row: d11_g1
-- end-expected
SELECT tgname FROM pg_trigger WHERE tgrelid = 'd11_tt'::regclass AND NOT tgisinternal ORDER BY tgname;

-- An error raised while deciding whether to fire belongs to the statement.
CREATE TRIGGER d11_g20 BEFORE INSERT ON d11_tt FOR EACH ROW WHEN (1/NEW.d > 0) EXECUTE FUNCTION d11_tf();

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
INSERT INTO d11_tt VALUES (1, 1, 0);

INSERT INTO d11_tt VALUES (2, 1, 1);

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
SELECT id FROM d11_tt ORDER BY id;

-- ============================================================================
-- SECTION K: REFRESH MATERIALIZED VIEW CONCURRENTLY prerequisites
-- ============================================================================

CREATE TABLE d11_mvt (id int PRIMARY KEY, v int);
INSERT INTO d11_mvt VALUES (1,1),(2,2);
CREATE MATERIALIZED VIEW d11_mv AS SELECT id, v FROM d11_mvt;

-- Row-by-row replacement needs a unique index to match rows by.
-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot refresh materialized view "public.d11_mv" concurrently
-- end-expected-error
REFRESH MATERIALIZED VIEW CONCURRENTLY d11_mv;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: CONCURRENTLY and WITH NO DATA options cannot be used together
-- end-expected-error
REFRESH MATERIALIZED VIEW CONCURRENTLY d11_mv WITH NO DATA;

-- A partial index does not cover every row, so it cannot identify them all.
CREATE UNIQUE INDEX d11_mv_pui ON d11_mv (id) WHERE v > 1;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot refresh materialized view "public.d11_mv" concurrently
-- end-expected-error
REFRESH MATERIALIZED VIEW CONCURRENTLY d11_mv;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: "d11_mvt" is not a materialized view
-- end-expected-error
REFRESH MATERIALIZED VIEW CONCURRENTLY d11_mvt;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "d11_nosuchmv" does not exist
-- end-expected-error
REFRESH MATERIALIZED VIEW CONCURRENTLY d11_nosuchmv;

-- A full unique index makes it work.
CREATE UNIQUE INDEX d11_mv_ui ON d11_mv (id);
REFRESH MATERIALIZED VIEW CONCURRENTLY d11_mv;

-- begin-expected
-- columns: id | v
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT id, v FROM d11_mv ORDER BY id;

REFRESH MATERIALIZED VIEW d11_mv;

-- begin-expected
-- columns: id | v
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT id, v FROM d11_mv ORDER BY id;

-- ============================================================================
-- SECTION L: PERMISSIVE policies are OR-ed, RESTRICTIVE ones AND-ed
-- ============================================================================

DROP TABLE IF EXISTS d11_rls CASCADE;
DROP ROLE IF EXISTS d11_role;
CREATE ROLE d11_role LOGIN;
CREATE TABLE d11_rls (id int PRIMARY KEY, s int);
INSERT INTO d11_rls VALUES (1, 1), (2, 200);
ALTER TABLE d11_rls ENABLE ROW LEVEL SECURITY;
GRANT SELECT, INSERT, UPDATE, DELETE ON d11_rls TO d11_role;
CREATE POLICY d11_pa ON d11_rls AS PERMISSIVE FOR INSERT WITH CHECK (id > 0);
CREATE POLICY d11_pb ON d11_rls AS PERMISSIVE FOR INSERT WITH CHECK (id > 100);
SET ROLE d11_role;

-- A second permissive policy widens what may be written, and 5 satisfies the
-- first one. AND-ing them would refuse a row the first policy plainly allows.
INSERT INTO d11_rls VALUES (5, 5);
INSERT INTO d11_rls VALUES (500, 5);

-- begin-expected-error
-- sqlstate: 42501
-- message-like: new row violates row-level security policy for table "d11_rls"
-- end-expected-error
INSERT INTO d11_rls VALUES (-5, 5);

RESET ROLE;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 5
-- row: 500
-- end-expected
SELECT id FROM d11_rls ORDER BY id;

-- A restrictive policy takes away, and the one that refused is named.
CREATE POLICY d11_pc ON d11_rls AS RESTRICTIVE FOR INSERT WITH CHECK (id < 1000);
SET ROLE d11_role;

-- begin-expected-error
-- sqlstate: 42501
-- message-like: new row violates row-level security policy "d11_pc" for table "d11_rls"
-- end-expected-error
INSERT INTO d11_rls VALUES (2000, 5);

INSERT INTO d11_rls VALUES (6, 6);
RESET ROLE;

-- The read path OR-s permissive policies too.
CREATE POLICY d11_ps1 ON d11_rls AS PERMISSIVE FOR SELECT USING (id = 1);
CREATE POLICY d11_ps2 ON d11_rls AS PERMISSIVE FOR SELECT USING (id = 6);
SET ROLE d11_role;

-- begin-expected
-- columns: id
-- row: 1
-- row: 6
-- end-expected
SELECT id FROM d11_rls ORDER BY id;

RESET ROLE;

-- UPDATE reads through USING and writes through WITH CHECK; both are OR-ed.
CREATE POLICY d11_pu1 ON d11_rls AS PERMISSIVE FOR UPDATE USING (id = 1) WITH CHECK (s = 10);
CREATE POLICY d11_pu2 ON d11_rls AS PERMISSIVE FOR UPDATE USING (id = 6) WITH CHECK (s = 20);
SET ROLE d11_role;
UPDATE d11_rls SET s = 10 WHERE id = 6;
UPDATE d11_rls SET s = 20 WHERE id = 1;

-- begin-expected-error
-- sqlstate: 42501
-- message-like: new row violates row-level security policy for table "d11_rls"
-- end-expected-error
UPDATE d11_rls SET s = 30 WHERE id = 1;

RESET ROLE;

-- begin-expected
-- columns: id | s
-- row: 1, 20
-- row: 2, 200
-- row: 5, 5
-- row: 6, 10
-- row: 500, 5
-- end-expected
SELECT id, s FROM d11_rls ORDER BY id;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS d11_rls CASCADE;
DROP OWNED BY d11_role;
DROP ROLE IF EXISTS d11_role;
DROP MATERIALIZED VIEW IF EXISTS d11_mv CASCADE;
DROP TABLE IF EXISTS d11_mvt CASCADE;
DROP VIEW IF EXISTS d11_tv CASCADE;
DROP TABLE IF EXISTS d11_tt CASCADE;
DROP TABLE IF EXISTS d11_ac CASCADE;
DROP TABLE IF EXISTS d11_ac_p CASCADE;
DROP TABLE IF EXISTS d11_c4 CASCADE;
DROP TABLE IF EXISTS d11_c5 CASCADE;
DROP TABLE IF EXISTS d11_c6 CASCADE;
DROP TABLE IF EXISTS d11_c7 CASCADE;
DROP TABLE IF EXISTS d11_c8 CASCADE;
DROP TABLE IF EXISTS d11_p1 CASCADE;
DROP TABLE IF EXISTS d11_p2 CASCADE;
DROP TABLE IF EXISTS d11_p3 CASCADE;
DROP TABLE IF EXISTS d11_p4 CASCADE;
DROP TABLE IF EXISTS d11_p5 CASCADE;
DROP TABLE IF EXISTS d11_p6 CASCADE;
DROP TABLE IF EXISTS d11_p7 CASCADE;
DROP TABLE IF EXISTS d11_pk2 CASCADE;
DROP TABLE IF EXISTS d11_pk3 CASCADE;
DROP TABLE IF EXISTS d11_pk5 CASCADE;
DROP TABLE IF EXISTS d11_pk6 CASCADE;
DROP TABLE IF EXISTS d11_pk8 CASCADE;
DROP TABLE IF EXISTS d11_pk9 CASCADE;
DROP TABLE IF EXISTS d11_ma CASCADE;
DROP TABLE IF EXISTS d11_cc CASCADE;
DROP TABLE IF EXISTS d11_domt CASCADE;
DROP DOMAIN IF EXISTS d11_dom CASCADE;
DROP TYPE IF EXISTS d11_ct CASCADE;
DROP TYPE IF EXISTS d11_tb CASCADE;
DROP TYPE IF EXISTS d11_tc CASCADE;
DROP VIEW IF EXISTS d11_ns_v CASCADE;
DROP SEQUENCE IF EXISTS d11_ns_s CASCADE;
DROP TABLE IF EXISTS d11_ns_t CASCADE;
DROP FUNCTION IF EXISTS d11_tf() CASCADE;
DROP SCHEMA IF EXISTS d11_s CASCADE;
DROP VIEW IF EXISTS d11_v_lit CASCADE;
DROP VIEW IF EXISTS d11_v_pre CASCADE;
DROP VIEW IF EXISTS d11_v_col CASCADE;
DROP VIEW IF EXISTS d11_v_cte CASCADE;
DROP VIEW IF EXISTS d11_v_sch CASCADE;
DROP VIEW IF EXISTS d11_v_real CASCADE;
DROP VIEW IF EXISTS d11_v_sub CASCADE;
DROP TABLE IF EXISTS d11_dep2 CASCADE;
DROP TABLE IF EXISTS d11_pre_long CASCADE;
DROP TABLE IF EXISTS d11_colhost CASCADE;
DROP TABLE IF EXISTS d11_cte_src CASCADE;
DROP TABLE IF EXISTS d11_real CASCADE;
DROP TABLE IF EXISTS d11_sub CASCADE;
