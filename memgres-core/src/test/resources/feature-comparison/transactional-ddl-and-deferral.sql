-- ============================================================================
-- Feature Comparison: transactional DDL and deferred constraints
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- DDL is transactional. A schema, a type, a domain or anything else a
-- transaction creates goes away again when that transaction rolls back --
-- leaving it behind makes the next CREATE of that name fail for an object
-- nobody successfully created.
--
-- SET CONSTRAINTS outside a transaction block is its own transaction, so the
-- mode it sets ends with the statement. Letting it persist made a later
-- transaction check a deferred constraint immediately, which is the opposite
-- of what was asked for and had nothing to do with the statement that asked.
--
-- SET CONSTRAINTS ... IMMEDIATE runs the checks postponed so far, so a
-- violation is reported by the statement that asked for the check rather than
-- surviving to COMMIT -- or vanishing if the transaction rolls back.
-- ============================================================================

SET search_path = public;

-- ============================================================================
-- What a transaction creates, a rollback takes away
-- ============================================================================

BEGIN;

CREATE SCHEMA b10_s;

ROLLBACK;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM pg_namespace WHERE nspname = 'b10_s';

-- the name is free again, which is the point
CREATE SCHEMA b10_s;

DROP SCHEMA b10_s;

BEGIN;

CREATE TYPE b10_c AS (x int);

ROLLBACK;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM pg_type WHERE typname = 'b10_c';

BEGIN;

CREATE DOMAIN b10_d AS int;

ROLLBACK;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM pg_type WHERE typname = 'b10_d';

BEGIN;

CREATE TYPE b10_e AS ENUM ('x');

ROLLBACK;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM pg_type WHERE typname = 'b10_e';

CREATE TABLE b10_base (i int);

BEGIN;

CREATE INDEX b10_ix ON b10_base (i);

ROLLBACK;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM pg_class WHERE relname = 'b10_ix';

BEGIN;

CREATE FUNCTION b10_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;

ROLLBACK;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM pg_proc WHERE proname = 'b10_f';

BEGIN;

ALTER TABLE b10_base ADD COLUMN j int;

ROLLBACK;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM information_schema.columns WHERE table_name = 'b10_base' AND column_name = 'j';

DROP TABLE b10_base;

-- ============================================================================
-- SET CONSTRAINTS outside a transaction block ends with the statement
-- ============================================================================

CREATE TABLE b10_dp (id int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED);

SET CONSTRAINTS ALL IMMEDIATE;

-- the deferred key is still deferred: the mode above went with its own statement
BEGIN;

INSERT INTO b10_dp VALUES (1);

INSERT INTO b10_dp VALUES (1);

ROLLBACK;

-- and inside a block it applies where it was asked for
BEGIN;

INSERT INTO b10_dp VALUES (2);

INSERT INTO b10_dp VALUES (2);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "b10_dp_pkey"
-- end-expected-error
SET CONSTRAINTS ALL IMMEDIATE;

ROLLBACK;

-- with nothing pending it fires nothing
BEGIN;

INSERT INTO b10_dp VALUES (3);

SET CONSTRAINTS ALL IMMEDIATE;

COMMIT;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT count(*)::text AS r FROM b10_dp;

-- a deferred violation that reaches COMMIT is reported there
BEGIN;

INSERT INTO b10_dp VALUES (4);

INSERT INTO b10_dp VALUES (4);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "b10_dp_pkey"
-- end-expected-error
COMMIT;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT count(*)::text AS r FROM b10_dp;

DROP TABLE b10_dp;

-- ============================================================================
-- A deferred foreign key is checked at the end, and the control statements
-- around it name constraints that have to exist and be deferrable
-- ============================================================================

CREATE TABLE b10_p (id int PRIMARY KEY);

CREATE TABLE b10_c2 (p int, CONSTRAINT b10_fk FOREIGN KEY (p) REFERENCES b10_p(id) DEFERRABLE INITIALLY DEFERRED);

CREATE TABLE b10_nd (p int, CONSTRAINT b10_nfk FOREIGN KEY (p) REFERENCES b10_p(id));

-- children before parents is the whole point of deferring
BEGIN;

INSERT INTO b10_c2 VALUES (77);

INSERT INTO b10_p VALUES (77);

COMMIT;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT count(*)::text AS r FROM b10_c2;

-- and a parent that never arrives is reported at COMMIT
BEGIN;

INSERT INTO b10_c2 VALUES (88);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "b10_c2" violates foreign key constraint "b10_fk"
-- end-expected-error
COMMIT;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT count(*)::text AS r FROM b10_c2;

-- ...or at the point the check is asked for
BEGIN;

INSERT INTO b10_c2 VALUES (99);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "b10_c2" violates foreign key constraint "b10_fk"
-- end-expected-error
SET CONSTRAINTS ALL IMMEDIATE;

ROLLBACK;

SET CONSTRAINTS b10_fk DEFERRED;

SET CONSTRAINTS b10_fk IMMEDIATE;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "no_such_constraint" does not exist
-- end-expected-error
SET CONSTRAINTS no_such_constraint DEFERRED;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: constraint "b10_nfk" is not deferrable
-- end-expected-error
SET CONSTRAINTS b10_nfk DEFERRED;

DROP TABLE b10_c2;

DROP TABLE b10_nd;

DROP TABLE b10_p;

-- ============================================================================
-- Savepoints are a stack, not a set of unique names
-- ============================================================================

CREATE TABLE b10_sv (i int);

BEGIN;

INSERT INTO b10_sv VALUES (1);

SAVEPOINT s1;

INSERT INTO b10_sv VALUES (2);

SAVEPOINT s1;

INSERT INTO b10_sv VALUES (3);

RELEASE SAVEPOINT s1;

ROLLBACK TO SAVEPOINT s1;

COMMIT;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT string_agg(i::text, ',' ORDER BY i) AS r FROM b10_sv;

DROP TABLE b10_sv;

