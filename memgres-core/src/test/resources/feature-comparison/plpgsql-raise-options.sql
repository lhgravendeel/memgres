-- ============================================================================
-- Feature Comparison: PL/pgSQL RAISE USING Options
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests RAISE statement options including MESSAGE, DETAIL, HINT, COLUMN,
-- CONSTRAINT, DATATYPE, TABLE, SCHEMA. Also tests GET STACKED DIAGNOSTICS
-- retrieval of these fields.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS raise_test CASCADE;
CREATE SCHEMA raise_test;
SET search_path = raise_test, public;

-- ============================================================================
-- 1. Basic RAISE EXCEPTION with format string
-- ============================================================================

CREATE FUNCTION raise_basic() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'something went wrong: %', 42;
END;
$$;

-- begin-expected-error
-- sqlstate: P0001
-- message-like: something went wrong: 42
-- end-expected-error
SELECT raise_basic();

-- ============================================================================
-- 2. RAISE USING MESSAGE (override default message)
-- ============================================================================

CREATE FUNCTION raise_using_message() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING MESSAGE = 'custom message text';
EXCEPTION WHEN raise_exception THEN
  RETURN SQLERRM;
END;
$$;

-- begin-expected
-- columns: result
-- row: custom message text
-- end-expected
SELECT raise_using_message() AS result;

-- ============================================================================
-- 3. RAISE USING ERRCODE + MESSAGE
-- ============================================================================

CREATE FUNCTION raise_errcode_message() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v_state text;
  v_msg text;
BEGIN
  RAISE EXCEPTION USING ERRCODE = 'XX001', MESSAGE = 'internal error occurred';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS
    v_state = RETURNED_SQLSTATE,
    v_msg = MESSAGE_TEXT;
  RETURN v_state || ': ' || v_msg;
END;
$$;

-- begin-expected
-- columns: result
-- row: XX001: internal error occurred
-- end-expected
SELECT raise_errcode_message() AS result;

-- ============================================================================
-- 4. RAISE USING DETAIL
-- ============================================================================

CREATE FUNCTION raise_with_detail() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v_detail text;
BEGIN
  RAISE EXCEPTION 'main error'
    USING DETAIL = 'here are the details of what went wrong';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_detail = PG_EXCEPTION_DETAIL;
  RETURN v_detail;
END;
$$;

-- begin-expected
-- columns: result
-- row: here are the details of what went wrong
-- end-expected
SELECT raise_with_detail() AS result;

-- ============================================================================
-- 5. RAISE USING HINT
-- ============================================================================

CREATE FUNCTION raise_with_hint() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v_hint text;
BEGIN
  RAISE EXCEPTION 'operation failed'
    USING HINT = 'Try running VACUUM first';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_hint = PG_EXCEPTION_HINT;
  RETURN v_hint;
END;
$$;

-- begin-expected
-- columns: result
-- row: Try running VACUUM first
-- end-expected
SELECT raise_with_hint() AS result;

-- ============================================================================
-- 6. RAISE USING MESSAGE + DETAIL + HINT combined
-- ============================================================================

CREATE FUNCTION raise_all_text_fields() RETURNS TABLE(msg text, detail text, hint text)
LANGUAGE plpgsql AS $$
DECLARE
  v_msg text;
  v_detail text;
  v_hint text;
BEGIN
  RAISE EXCEPTION USING
    MESSAGE = 'the main error',
    DETAIL = 'the detail text',
    HINT = 'the hint text';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS
    v_msg = MESSAGE_TEXT,
    v_detail = PG_EXCEPTION_DETAIL,
    v_hint = PG_EXCEPTION_HINT;
  msg := v_msg;
  detail := v_detail;
  hint := v_hint;
  RETURN NEXT;
END;
$$;

-- begin-expected
-- columns: msg, detail, hint
-- row: the main error, the detail text, the hint text
-- end-expected
SELECT * FROM raise_all_text_fields();

-- ============================================================================
-- 7. RAISE USING ERRCODE + MESSAGE + DETAIL + HINT
-- ============================================================================

CREATE FUNCTION raise_full_combo() RETURNS TABLE(state text, msg text, detail text, hint text)
LANGUAGE plpgsql AS $$
DECLARE
  v_state text;
  v_msg text;
  v_detail text;
  v_hint text;
BEGIN
  RAISE EXCEPTION USING
    ERRCODE = 'P0099',
    MESSAGE = 'full error',
    DETAIL = 'full detail',
    HINT = 'full hint';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS
    v_state = RETURNED_SQLSTATE,
    v_msg = MESSAGE_TEXT,
    v_detail = PG_EXCEPTION_DETAIL,
    v_hint = PG_EXCEPTION_HINT;
  state := v_state;
  msg := v_msg;
  detail := v_detail;
  hint := v_hint;
  RETURN NEXT;
END;
$$;

-- begin-expected
-- columns: state, msg, detail, hint
-- row: P0099, full error, full detail, full hint
-- end-expected
SELECT * FROM raise_full_combo();

-- ============================================================================
-- 8. RAISE USING COLUMN
-- ============================================================================

CREATE FUNCTION raise_with_column() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v_col text;
BEGIN
  RAISE EXCEPTION 'bad column'
    USING COLUMN = 'email_address';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_col = COLUMN_NAME;
  RETURN v_col;
END;
$$;

-- begin-expected
-- columns: result
-- row: email_address
-- end-expected
SELECT raise_with_column() AS result;

-- ============================================================================
-- 9. RAISE USING CONSTRAINT
-- ============================================================================

CREATE FUNCTION raise_with_constraint() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v_constraint text;
BEGIN
  RAISE EXCEPTION 'constraint violated'
    USING CONSTRAINT = 'users_email_unique';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_constraint = CONSTRAINT_NAME;
  RETURN v_constraint;
END;
$$;

-- begin-expected
-- columns: result
-- row: users_email_unique
-- end-expected
SELECT raise_with_constraint() AS result;

-- ============================================================================
-- 10. RAISE USING DATATYPE
-- ============================================================================

CREATE FUNCTION raise_with_datatype() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v_dtype text;
BEGIN
  RAISE EXCEPTION 'wrong type'
    USING DATATYPE = 'integer';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_dtype = PG_DATATYPE_NAME;
  RETURN v_dtype;
END;
$$;

-- begin-expected
-- columns: result
-- row: integer
-- end-expected
SELECT raise_with_datatype() AS result;

-- ============================================================================
-- 11. RAISE USING TABLE
-- ============================================================================

CREATE FUNCTION raise_with_table() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v_table text;
BEGIN
  RAISE EXCEPTION 'table error'
    USING TABLE = 'users';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_table = TABLE_NAME;
  RETURN v_table;
END;
$$;

-- begin-expected
-- columns: result
-- row: users
-- end-expected
SELECT raise_with_table() AS result;

-- ============================================================================
-- 12. RAISE USING SCHEMA
-- ============================================================================

CREATE FUNCTION raise_with_schema() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v_schema text;
BEGIN
  RAISE EXCEPTION 'schema error'
    USING SCHEMA = 'public';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_schema = SCHEMA_NAME;
  RETURN v_schema;
END;
$$;

-- begin-expected
-- columns: result
-- row: public
-- end-expected
SELECT raise_with_schema() AS result;

-- ============================================================================
-- 13. All USING options at once
-- ============================================================================

CREATE FUNCTION raise_all_options() RETURNS TABLE(
  state text, msg text, detail text, hint text,
  col text, constraint_name text, dtype text, tbl text, sch text
)
LANGUAGE plpgsql AS $$
DECLARE
  v_state text; v_msg text; v_detail text; v_hint text;
  v_col text; v_constraint text; v_dtype text; v_tbl text; v_sch text;
BEGIN
  RAISE EXCEPTION USING
    ERRCODE = 'XX999',
    MESSAGE = 'all options error',
    DETAIL = 'all detail',
    HINT = 'all hint',
    COLUMN = 'col1',
    CONSTRAINT = 'pk_test',
    DATATYPE = 'varchar',
    TABLE = 'my_table',
    SCHEMA = 'my_schema';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS
    v_state = RETURNED_SQLSTATE,
    v_msg = MESSAGE_TEXT,
    v_detail = PG_EXCEPTION_DETAIL,
    v_hint = PG_EXCEPTION_HINT,
    v_col = COLUMN_NAME,
    v_constraint = CONSTRAINT_NAME,
    v_dtype = PG_DATATYPE_NAME,
    v_tbl = TABLE_NAME,
    v_sch = SCHEMA_NAME;
  state := v_state;
  msg := v_msg;
  detail := v_detail;
  hint := v_hint;
  col := v_col;
  constraint_name := v_constraint;
  dtype := v_dtype;
  tbl := v_tbl;
  sch := v_sch;
  RETURN NEXT;
END;
$$;

-- begin-expected
-- columns: state, msg, detail, hint, col, constraint_name, dtype, tbl, sch
-- row: XX999, all options error, all detail, all hint, col1, pk_test, varchar, my_table, my_schema
-- end-expected
SELECT * FROM raise_all_options();

-- ============================================================================
-- 14. RAISE levels: NOTICE, WARNING, INFO, LOG, DEBUG
-- ============================================================================

-- note: Only EXCEPTION raises an error; other levels just emit messages.
-- We verify that non-EXCEPTION levels don't abort execution.

CREATE FUNCTION raise_levels() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE DEBUG 'debug message';
  RAISE LOG 'log message';
  RAISE INFO 'info message';
  RAISE NOTICE 'notice message';
  RAISE WARNING 'warning message';
  RETURN 'completed all raises';
END;
$$;

-- begin-expected
-- columns: result
-- row: completed all raises
-- end-expected
SELECT raise_levels() AS result;

-- ============================================================================
-- 15. RAISE with format placeholders
-- ============================================================================

CREATE FUNCTION raise_format(a integer, b text) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'Error: val=%, name=%', a, b;
EXCEPTION WHEN OTHERS THEN
  RETURN SQLERRM;
END;
$$;

-- begin-expected
-- columns: result
-- row: Error: val=42, name=test
-- end-expected
SELECT raise_format(42, 'test') AS result;

-- ============================================================================
-- 16. RAISE with ERRCODE as condition name
-- ============================================================================

CREATE FUNCTION raise_condition_name() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v_state text;
BEGIN
  RAISE EXCEPTION USING ERRCODE = 'unique_violation', MESSAGE = 'simulated';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE;
  RETURN v_state;
END;
$$;

-- begin-expected
-- columns: result
-- row: 23505
-- end-expected
SELECT raise_condition_name() AS result;

-- ============================================================================
-- 17. GET STACKED DIAGNOSTICS: PG_EXCEPTION_CONTEXT
-- ============================================================================

CREATE FUNCTION raise_inner() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'inner error';
END;
$$;

CREATE FUNCTION raise_outer() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v_context text;
BEGIN
  PERFORM raise_inner();
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_context = PG_EXCEPTION_CONTEXT;
  -- Context should mention the call stack
  RETURN CASE WHEN v_context IS NOT NULL AND v_context <> '' THEN 'has context' ELSE 'no context' END;
END;
$$;

-- begin-expected
-- columns: result
-- row: has context
-- end-expected
SELECT raise_outer() AS result;

-- ============================================================================
-- 18. RAISE inside EXCEPTION block (re-raise)
-- ============================================================================

CREATE FUNCTION raise_reraise() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  BEGIN
    PERFORM 1/0;
  EXCEPTION WHEN division_by_zero THEN
    RAISE;  -- re-raise the same exception
  END;
  RETURN 'should not reach';
EXCEPTION WHEN division_by_zero THEN
  RETURN 'caught re-raised division_by_zero';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught re-raised division_by_zero
-- end-expected
SELECT raise_reraise() AS result;

-- ============================================================================
-- 19. Constraint violation auto-populates DETAIL, TABLE, SCHEMA, CONSTRAINT
-- ============================================================================

-- note: When PG raises a constraint violation, it auto-fills diagnostic fields
CREATE TABLE raise_pk_test (id integer PRIMARY KEY);
INSERT INTO raise_pk_test VALUES (1);

CREATE FUNCTION raise_real_constraint_diag() RETURNS TABLE(
  state text, detail text, tbl text, sch text, constraint_name text
)
LANGUAGE plpgsql AS $$
DECLARE
  v_state text; v_detail text; v_tbl text; v_sch text; v_constraint text;
BEGIN
  INSERT INTO raise_pk_test VALUES (1);
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS
    v_state = RETURNED_SQLSTATE,
    v_detail = PG_EXCEPTION_DETAIL,
    v_tbl = TABLE_NAME,
    v_sch = SCHEMA_NAME,
    v_constraint = CONSTRAINT_NAME;
  state := v_state;
  detail := v_detail;
  tbl := v_tbl;
  sch := v_sch;
  constraint_name := v_constraint;
  RETURN NEXT;
END;
$$;

-- begin-expected
-- columns: state
-- row: 23505
-- end-expected
SELECT state FROM raise_real_constraint_diag();

-- note: PG populates table_name, schema_name, constraint_name for constraint violations
-- begin-expected
-- columns: has_table, has_constraint
-- row: true, true
-- end-expected
SELECT
  (tbl IS NOT NULL AND tbl <> '') AS has_table,
  (constraint_name IS NOT NULL AND constraint_name <> '') AS has_constraint
FROM raise_real_constraint_diag();

-- ============================================================================
-- 20. RAISE with USING in non-EXCEPTION level
-- ============================================================================

-- note: USING options are valid with all RAISE levels in PG
CREATE FUNCTION raise_notice_using() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'notice with hint' USING HINT = 'this is a hint';
  RETURN 'ok';
END;
$$;

-- begin-expected
-- columns: result
-- row: ok
-- end-expected
SELECT raise_notice_using() AS result;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA raise_test CASCADE;
SET search_path = public;
