-- ============================================================================
-- Feature Comparison: SERIAL Column Sequence Lifecycle
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests that SERIAL columns create real, usable sequences that are tightly
-- coupled with the column's default value generation. Covers:
--   - Implicit sequence creation at CREATE TABLE time
--   - nextval/currval/setval on the implicit sequence
--   - ALTER SEQUENCE affecting INSERT behavior
--   - pg_get_serial_sequence introspection
--   - DROP TABLE cascading to implicit sequence
--   - DROP sequence while table exists
--   - BIGSERIAL and SMALLSERIAL variants
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS serial_test CASCADE;
CREATE SCHEMA serial_test;
SET search_path = serial_test, public;

-- ============================================================================
-- 1. Basic SERIAL creates an implicit sequence
-- ============================================================================

CREATE TABLE t1 (id serial PRIMARY KEY, val text);

-- begin-expected
-- columns: pg_get_serial_sequence
-- row: serial_test.t1_id_seq
-- end-expected
SELECT pg_get_serial_sequence('t1', 'id');

-- The sequence should exist and be queryable
-- begin-expected
-- columns: last_value
-- row: 1
-- end-expected
SELECT last_value FROM serial_test.t1_id_seq;

-- ============================================================================
-- 2. INSERT uses the sequence (nextval is called implicitly)
-- ============================================================================

INSERT INTO t1 (val) VALUES ('first');

-- begin-expected
-- columns: id, val
-- row: 1, first
-- end-expected
SELECT id, val FROM t1;

-- currval should reflect the value used
-- begin-expected
-- columns: currval
-- row: 1
-- end-expected
SELECT currval('serial_test.t1_id_seq');

-- Second insert increments
INSERT INTO t1 (val) VALUES ('second');

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
SELECT id FROM t1 WHERE val = 'second';

-- begin-expected
-- columns: currval
-- row: 2
-- end-expected
SELECT currval('serial_test.t1_id_seq');

-- ============================================================================
-- 3. setval affects the next INSERT
-- ============================================================================

SELECT setval('serial_test.t1_id_seq', 100);

INSERT INTO t1 (val) VALUES ('after_setval');

-- begin-expected
-- columns: id
-- row: 101
-- end-expected
SELECT id FROM t1 WHERE val = 'after_setval';

-- ============================================================================
-- 4. ALTER SEQUENCE ... RESTART WITH affects INSERT
-- ============================================================================

CREATE TABLE t2 (id serial PRIMARY KEY, val text);

INSERT INTO t2 (val) VALUES ('a');

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM t2 WHERE val = 'a';

ALTER SEQUENCE serial_test.t2_id_seq RESTART WITH 50;

INSERT INTO t2 (val) VALUES ('b');

-- begin-expected
-- columns: id
-- row: 50
-- end-expected
SELECT id FROM t2 WHERE val = 'b';

-- ============================================================================
-- 5. nextval called directly advances the sequence, skipping a value
-- ============================================================================

CREATE TABLE t3 (id serial PRIMARY KEY, val text);

INSERT INTO t3 (val) VALUES ('row1');

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM t3 WHERE val = 'row1';

-- Manually advance the sequence (skips 2)
SELECT nextval('serial_test.t3_id_seq');

INSERT INTO t3 (val) VALUES ('row2');

-- begin-expected
-- columns: id
-- row: 3
-- end-expected
SELECT id FROM t3 WHERE val = 'row2';

-- ============================================================================
-- 6. Querying the sequence directly after INSERTs
-- ============================================================================

CREATE TABLE t4 (id serial PRIMARY KEY, val text);

INSERT INTO t4 (val) VALUES ('x'), ('y'), ('z');

-- begin-expected
-- columns: last_value
-- row: 3
-- end-expected
SELECT last_value FROM serial_test.t4_id_seq;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*) AS cnt FROM t4;

-- ============================================================================
-- 7. BIGSERIAL creates a sequence too
-- ============================================================================

CREATE TABLE t5 (id bigserial PRIMARY KEY, val text);

-- begin-expected
-- columns: pg_get_serial_sequence
-- row: serial_test.t5_id_seq
-- end-expected
SELECT pg_get_serial_sequence('t5', 'id');

INSERT INTO t5 (val) VALUES ('big1');

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM t5 WHERE val = 'big1';

-- begin-expected
-- columns: last_value
-- row: 1
-- end-expected
SELECT last_value FROM serial_test.t5_id_seq;

-- ============================================================================
-- 8. SMALLSERIAL creates a sequence too
-- ============================================================================

CREATE TABLE t6 (id smallserial PRIMARY KEY, val text);

-- begin-expected
-- columns: pg_get_serial_sequence
-- row: serial_test.t6_id_seq
-- end-expected
SELECT pg_get_serial_sequence('t6', 'id');

INSERT INTO t6 (val) VALUES ('small1');

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM t6 WHERE val = 'small1';

-- ============================================================================
-- 9. DROP TABLE cascades to its implicit sequence
-- ============================================================================

CREATE TABLE t7 (id serial PRIMARY KEY, val text);
INSERT INTO t7 (val) VALUES ('before_drop');

-- Sequence should exist
-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM pg_sequences WHERE schemaname = 'serial_test' AND sequencename = 't7_id_seq';

DROP TABLE t7;

-- Sequence should be gone
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*) AS cnt FROM pg_sequences WHERE schemaname = 'serial_test' AND sequencename = 't7_id_seq';

-- ============================================================================
-- 10. DROP SEQUENCE on an implicit serial sequence — PG rejects without CASCADE
-- ============================================================================

CREATE TABLE t8 (id serial PRIMARY KEY, val text);
INSERT INTO t8 (val) VALUES ('keep');

-- begin-expected-error
-- message-like: cannot drop sequence t8_id_seq because other objects depend on it
-- end-expected-error
DROP SEQUENCE serial_test.t8_id_seq;

-- Table still works
-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM t8;

-- ============================================================================
-- 11. DROP SEQUENCE ... CASCADE drops the default but keeps the table
-- ============================================================================

DROP SEQUENCE serial_test.t8_id_seq CASCADE;

-- Table still exists but column lost its default
-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM t8;

-- Insert without explicit id should now fail (no default)
-- begin-expected-error
-- message-like: null value in column "id"
-- end-expected-error
INSERT INTO t8 (val) VALUES ('no_default');

DROP TABLE t8;

-- ============================================================================
-- 12. Multiple serial columns on the same table
-- ============================================================================

CREATE TABLE t9 (a serial, b serial, val text);

-- begin-expected
-- columns: pg_get_serial_sequence
-- row: serial_test.t9_a_seq
-- end-expected
SELECT pg_get_serial_sequence('t9', 'a');

-- begin-expected
-- columns: pg_get_serial_sequence
-- row: serial_test.t9_b_seq
-- end-expected
SELECT pg_get_serial_sequence('t9', 'b');

INSERT INTO t9 (val) VALUES ('multi');

-- begin-expected
-- columns: a, b, val
-- row: 1, 1, multi
-- end-expected
SELECT a, b, val FROM t9;

-- Advance only sequence b
SELECT setval('serial_test.t9_b_seq', 10);

INSERT INTO t9 (val) VALUES ('multi2');

-- begin-expected
-- columns: a, b, val
-- row: 2, 11, multi2
-- end-expected
SELECT a, b, val FROM t9 WHERE val = 'multi2';

DROP TABLE t9;

-- ============================================================================
-- 13. setval with is_called=false — next insert gets exactly that value
-- ============================================================================

CREATE TABLE t10 (id serial PRIMARY KEY, val text);

SELECT setval('serial_test.t10_id_seq', 42, false);

INSERT INTO t10 (val) VALUES ('exact');

-- begin-expected
-- columns: id
-- row: 42
-- end-expected
SELECT id FROM t10 WHERE val = 'exact';

-- Next value should be 43
INSERT INTO t10 (val) VALUES ('next');

-- begin-expected
-- columns: id
-- row: 43
-- end-expected
SELECT id FROM t10 WHERE val = 'next';

DROP TABLE t10;

-- ============================================================================
-- 14. TRUNCATE ... RESTART IDENTITY resets sequence
-- ============================================================================

CREATE TABLE t11 (id serial PRIMARY KEY, val text);
INSERT INTO t11 (val) VALUES ('a'), ('b'), ('c');

-- begin-expected
-- columns: last_value
-- row: 3
-- end-expected
SELECT last_value FROM serial_test.t11_id_seq;

TRUNCATE t11 RESTART IDENTITY;

INSERT INTO t11 (val) VALUES ('fresh');

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM t11 WHERE val = 'fresh';

DROP TABLE t11;

-- ============================================================================
-- Teardown
-- ============================================================================

DROP SCHEMA serial_test CASCADE;
