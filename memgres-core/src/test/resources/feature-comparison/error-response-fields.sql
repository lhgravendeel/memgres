-- ============================================================================
-- Feature Comparison: Error Response Fields (Detail, Hint, Position)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Wire protocol error fields are not directly SQL-testable, but PG populates
-- DETAIL, HINT, SCHEMA, TABLE, COLUMN, CONSTRAINT, DATATYPE automatically
-- for many error types. We can verify these via GET STACKED DIAGNOSTICS.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS errfield_test CASCADE;
CREATE SCHEMA errfield_test;
SET search_path = errfield_test, public;

-- Helper to extract all diagnostic fields from any SQL
CREATE FUNCTION errfield_diag(sql_text text) RETURNS TABLE(
  err_sqlstate text, message text, detail text, hint text,
  schema_name text, table_name text, column_name text,
  constraint_name text, datatype_name text
)
LANGUAGE plpgsql AS $$
DECLARE
  v_state text; v_msg text; v_detail text; v_hint text;
  v_schema text; v_table text; v_column text;
  v_constraint text; v_datatype text;
BEGIN
  EXECUTE sql_text;
  err_sqlstate := 'OK'; message := 'no error';
  RETURN NEXT;
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS
    v_state = RETURNED_SQLSTATE,
    v_msg = MESSAGE_TEXT,
    v_detail = PG_EXCEPTION_DETAIL,
    v_hint = PG_EXCEPTION_HINT,
    v_schema = SCHEMA_NAME,
    v_table = TABLE_NAME,
    v_column = COLUMN_NAME,
    v_constraint = CONSTRAINT_NAME,
    v_datatype = PG_DATATYPE_NAME;
  err_sqlstate := v_state;
  message := v_msg;
  detail := v_detail;
  hint := v_hint;
  schema_name := v_schema;
  table_name := v_table;
  column_name := v_column;
  constraint_name := v_constraint;
  datatype_name := v_datatype;
  RETURN NEXT;
END;
$$;

-- ============================================================================
-- 1. Primary key violation: populates detail, table, schema, constraint
-- ============================================================================

CREATE TABLE errfield_pk (id integer PRIMARY KEY);
INSERT INTO errfield_pk VALUES (1);

-- begin-expected
-- columns: err_sqlstate
-- row: 23505
-- end-expected
SELECT err_sqlstate FROM errfield_diag('INSERT INTO errfield_test.errfield_pk VALUES (1)');

-- note: PG populates detail with "Key (id)=(1) already exists."
-- begin-expected
-- columns: has_detail
-- row: true
-- end-expected
SELECT (detail IS NOT NULL AND detail <> '') AS has_detail
FROM errfield_diag('INSERT INTO errfield_test.errfield_pk VALUES (1)');

-- begin-expected
-- columns: has_schema, has_table, has_constraint
-- row: true, true, true
-- end-expected
SELECT
  (schema_name IS NOT NULL AND schema_name <> '') AS has_schema,
  (table_name IS NOT NULL AND table_name <> '') AS has_table,
  (constraint_name IS NOT NULL AND constraint_name <> '') AS has_constraint
FROM errfield_diag('INSERT INTO errfield_test.errfield_pk VALUES (1)');

-- ============================================================================
-- 2. Foreign key violation: populates detail, table, schema, constraint
-- ============================================================================

CREATE TABLE errfield_parent (id integer PRIMARY KEY);
CREATE TABLE errfield_child (
  id integer,
  parent_id integer REFERENCES errfield_parent(id)
);

-- begin-expected
-- columns: err_sqlstate
-- row: 23503
-- end-expected
SELECT err_sqlstate FROM errfield_diag('INSERT INTO errfield_test.errfield_child VALUES (1, 999)');

-- begin-expected
-- columns: has_detail, has_constraint
-- row: true, true
-- end-expected
SELECT
  (detail IS NOT NULL AND detail <> '') AS has_detail,
  (constraint_name IS NOT NULL AND constraint_name <> '') AS has_constraint
FROM errfield_diag('INSERT INTO errfield_test.errfield_child VALUES (1, 999)');

-- ============================================================================
-- 3. NOT NULL violation: populates column, table, schema
-- ============================================================================

CREATE TABLE errfield_nn (id integer NOT NULL, val text NOT NULL);

-- begin-expected
-- columns: err_sqlstate
-- row: 23502
-- end-expected
SELECT err_sqlstate FROM errfield_diag('INSERT INTO errfield_test.errfield_nn (id) VALUES (1)');

-- note: PG populates column_name for NOT NULL violations
-- begin-expected
-- columns: has_column, has_table
-- row: true, true
-- end-expected
SELECT
  (column_name IS NOT NULL AND column_name <> '') AS has_column,
  (table_name IS NOT NULL AND table_name <> '') AS has_table
FROM errfield_diag('INSERT INTO errfield_test.errfield_nn (id) VALUES (1)');

-- ============================================================================
-- 4. CHECK constraint violation: populates constraint, table, schema
-- ============================================================================

CREATE TABLE errfield_chk (val integer CONSTRAINT val_positive CHECK (val > 0));

-- begin-expected
-- columns: err_sqlstate
-- row: 23514
-- end-expected
SELECT err_sqlstate FROM errfield_diag('INSERT INTO errfield_test.errfield_chk VALUES (-1)');

-- begin-expected
-- columns: has_constraint, has_table
-- row: true, true
-- end-expected
SELECT
  (constraint_name IS NOT NULL AND constraint_name <> '') AS has_constraint,
  (table_name IS NOT NULL AND table_name <> '') AS has_table
FROM errfield_diag('INSERT INTO errfield_test.errfield_chk VALUES (-1)');

-- ============================================================================
-- 5. Unique constraint violation: populates detail, constraint
-- ============================================================================

CREATE TABLE errfield_uniq (email text UNIQUE);
INSERT INTO errfield_uniq VALUES ('test@example.com');

-- begin-expected
-- columns: err_sqlstate, has_detail
-- row: 23505, true
-- end-expected
SELECT err_sqlstate, (detail IS NOT NULL AND detail <> '') AS has_detail
FROM errfield_diag($$INSERT INTO errfield_test.errfield_uniq VALUES ('test@example.com')$$);

-- ============================================================================
-- 6. Type mismatch: populates datatype
-- ============================================================================

-- note: Some type errors populate PG_DATATYPE_NAME
-- begin-expected
-- columns: err_sqlstate
-- row: 22P02
-- end-expected
SELECT err_sqlstate FROM errfield_diag($$SELECT 'not_a_number'::integer$$);

-- ============================================================================
-- 7. String truncation: populates datatype
-- ============================================================================

CREATE TABLE errfield_trunc (val varchar(3));

-- begin-expected
-- columns: err_sqlstate
-- row: 22001
-- end-expected
SELECT err_sqlstate FROM errfield_diag($$INSERT INTO errfield_test.errfield_trunc VALUES ('toolong')$$);

-- ============================================================================
-- 8. Exclusion constraint violation: populates constraint, detail
-- ============================================================================

-- note: Requires btree_gist extension which may not be available
-- Skipping exclusion constraint test as it requires extension

-- ============================================================================
-- 9. Multiple constraint violations produce correct field per error
-- ============================================================================

CREATE TABLE errfield_multi (
  id integer PRIMARY KEY,
  name text NOT NULL,
  email text UNIQUE,
  score integer CHECK (score >= 0)
);
INSERT INTO errfield_multi VALUES (1, 'alice', 'alice@test.com', 100);

-- PK violation
-- begin-expected
-- columns: err_sqlstate, has_constraint
-- row: 23505, true
-- end-expected
SELECT err_sqlstate, (constraint_name IS NOT NULL AND constraint_name <> '') AS has_constraint
FROM errfield_diag($$INSERT INTO errfield_test.errfield_multi VALUES (1, 'bob', 'bob@test.com', 50)$$);

-- NOT NULL violation
-- begin-expected
-- columns: err_sqlstate, has_column
-- row: 23502, true
-- end-expected
SELECT err_sqlstate, (column_name IS NOT NULL AND column_name <> '') AS has_column
FROM errfield_diag($$INSERT INTO errfield_test.errfield_multi VALUES (2, NULL, 'bob@test.com', 50)$$);

-- CHECK violation
-- begin-expected
-- columns: err_sqlstate, has_constraint
-- row: 23514, true
-- end-expected
SELECT err_sqlstate, (constraint_name IS NOT NULL AND constraint_name <> '') AS has_constraint
FROM errfield_diag($$INSERT INTO errfield_test.errfield_multi VALUES (2, 'bob', 'bob@test.com', -1)$$);

-- UNIQUE violation
-- begin-expected
-- columns: err_sqlstate, has_detail
-- row: 23505, true
-- end-expected
SELECT err_sqlstate, (detail IS NOT NULL AND detail <> '') AS has_detail
FROM errfield_diag($$INSERT INTO errfield_test.errfield_multi VALUES (2, 'bob', 'alice@test.com', 50)$$);

-- ============================================================================
-- 10. FK delete violation: populates detail, constraint
-- ============================================================================

INSERT INTO errfield_parent VALUES (1);
INSERT INTO errfield_child VALUES (1, 1);

-- begin-expected
-- columns: err_sqlstate, has_detail
-- row: 23503, true
-- end-expected
SELECT err_sqlstate, (detail IS NOT NULL AND detail <> '') AS has_detail
FROM errfield_diag('DELETE FROM errfield_test.errfield_parent WHERE id = 1');

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA errfield_test CASCADE;
SET search_path = public;
