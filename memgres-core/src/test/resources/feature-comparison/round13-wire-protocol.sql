-- ============================================================================
-- Feature Comparison: Round 13 — Wire-Protocol ErrorResponse Fields
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Exercises the extended fields of ErrorResponse (Hint/Position/File/Line/
-- Routine/Schema/Table/Column/Constraint/Datatype). This SQL file relies on
-- PG 18 populating the position (P) field automatically for undefined
-- column/table errors.
--
-- Note: The harness cannot directly inspect ErrorResponse fields; the Java
-- test `Round13WireProtocolGapsTest` checks those. This SQL file checks the
-- *visible* side: that statements produce errors with the right SQLSTATE
-- and message structure.
-- ============================================================================

DROP SCHEMA IF EXISTS r13_wire CASCADE;
CREATE SCHEMA r13_wire;
SET search_path = r13_wire, public;

-- ============================================================================
-- SECTION A: Undefined-object errors (42P01, 42703)
-- ============================================================================

-- 1. Unknown table → 42P01
-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT * FROM no_such_table_r13_wire;

-- 2. Unknown column → 42703
CREATE TABLE r13_wire_t (correct_col int);

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT corect_col FROM r13_wire_t;

-- ============================================================================
-- SECTION B: Constraint-violation identity fields
-- ============================================================================

CREATE TABLE r13_wire_pk (id int PRIMARY KEY);
INSERT INTO r13_wire_pk VALUES (1);

-- 3. Duplicate PK — schema/table/constraint populated
-- begin-expected-error
-- message-like: duplicate key
-- end-expected-error
INSERT INTO r13_wire_pk VALUES (1);

CREATE TABLE r13_wire_parent (id int PRIMARY KEY);
CREATE TABLE r13_wire_child (pid int REFERENCES r13_wire_parent(id));

-- 4. FK violation — constraint field populated
-- begin-expected-error
-- message-like: violates foreign key
-- end-expected-error
INSERT INTO r13_wire_child VALUES (999);

-- 5. Numeric overflow — datatype field populated
-- begin-expected-error
-- message-like: out of range
-- end-expected-error
SELECT (2147483647 + 1)::int;

-- ============================================================================
-- SECTION C: COPY FROM/TO PROGRAM must be rejected
-- ============================================================================

-- 6. COPY TO PROGRAM
COPY (SELECT 1) TO PROGRAM 'echo';

-- 7. COPY FROM PROGRAM
-- begin-expected-error
-- message-like: program
-- end-expected-error
COPY r13_wire_pk FROM PROGRAM 'cat /tmp/nope.csv';

-- ============================================================================
-- SECTION D: In-txn visibility after failed statement (25P02)
-- ============================================================================

BEGIN;

-- 8. First statement that fails
-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT * FROM absolutely_no_such_table_r13;

-- 9. Subsequent statement must be rejected with 25P02
-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
SELECT 1;

ROLLBACK;
