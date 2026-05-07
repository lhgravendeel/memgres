-- ============================================================================
-- Feature Comparison: setval with NULL argument
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests that setval(sequence, NULL) is a no-op: it does not error and does not
-- change the sequence position. This is the common pattern:
--   SELECT setval('foo_id_seq', (SELECT max(id) FROM foo));
-- On an empty table, max(id) is NULL, and PG treats setval(..., NULL) as a
-- no-op returning NULL.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS setval_null_test CASCADE;
CREATE SCHEMA setval_null_test;
SET search_path = setval_null_test, public;

-- ============================================================================
-- 1. setval with NULL is a no-op — sequence continues normally
-- ============================================================================

CREATE TABLE foo (id serial PRIMARY KEY);

-- First insert — id should be 1
INSERT INTO foo DEFAULT VALUES;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM foo ORDER BY id;

-- setval with NULL — should be a no-op (returns NULL, sequence unchanged)
-- begin-expected
-- columns: setval
-- row: null
-- end-expected
SELECT setval('foo_id_seq', NULL);

-- Second insert — id should be 2 (sequence was not affected by setval NULL)
INSERT INTO foo DEFAULT VALUES;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM foo ORDER BY id;

-- ============================================================================
-- 2. setval with NULL from subquery (common pattern with empty table)
-- ============================================================================

CREATE TABLE bar (id serial PRIMARY KEY);

-- Empty table — max returns NULL
-- begin-expected
-- columns: max
-- row: null
-- end-expected
SELECT max(id) FROM bar;

-- setval with NULL subquery — should be a no-op
-- begin-expected
-- columns: setval
-- row: null
-- end-expected
SELECT setval('bar_id_seq', (SELECT max(id) FROM bar));

-- Insert should still get id=1
INSERT INTO bar DEFAULT VALUES;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM bar ORDER BY id;

-- ============================================================================
-- 3. setval NULL after real inserts — sequence position preserved
-- ============================================================================

CREATE TABLE baz (id serial PRIMARY KEY);
INSERT INTO baz DEFAULT VALUES;
INSERT INTO baz DEFAULT VALUES;
INSERT INTO baz DEFAULT VALUES;

-- Sequence is now at 3
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM baz ORDER BY id;

-- setval NULL — should not reset the sequence
-- begin-expected
-- columns: setval
-- row: null
-- end-expected
SELECT setval('baz_id_seq', NULL);

-- Next insert should get id=4 (sequence unchanged)
INSERT INTO baz DEFAULT VALUES;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM baz ORDER BY id;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA setval_null_test CASCADE;
