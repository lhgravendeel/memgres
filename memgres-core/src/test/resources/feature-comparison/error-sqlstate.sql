-- ============================================================================
-- Feature Comparison: Error SQLSTATE Codes (F partial, B5)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Wire protocol error fields (severity, detail, hint, position) are not
-- SQL-testable, but SQLSTATE accuracy IS testable via PL/pgSQL EXCEPTION
-- handlers and GET STACKED DIAGNOSTICS.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS sqlstate_test CASCADE;
CREATE SCHEMA sqlstate_test;
SET search_path = sqlstate_test, public;

-- Helper function to capture SQLSTATE from any SQL string
CREATE FUNCTION get_sqlstate(sql text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  state text;
BEGIN
  EXECUTE sql;
  RETURN 'OK';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS state = RETURNED_SQLSTATE;
  RETURN state;
END;
$$;

-- ============================================================================
-- 1. 23505 unique_violation
-- ============================================================================

CREATE TABLE sqlstate_uniq (id integer PRIMARY KEY);
INSERT INTO sqlstate_uniq VALUES (1);

-- begin-expected
-- columns: state
-- row: 23505
-- end-expected
SELECT get_sqlstate('INSERT INTO sqlstate_test.sqlstate_uniq VALUES (1)') AS state;

-- ============================================================================
-- 2. 23503 foreign_key_violation
-- ============================================================================

CREATE TABLE sqlstate_parent (id integer PRIMARY KEY);
CREATE TABLE sqlstate_child (id integer, pid integer REFERENCES sqlstate_parent(id));
INSERT INTO sqlstate_parent VALUES (1);

-- begin-expected
-- columns: state
-- row: 23503
-- end-expected
SELECT get_sqlstate('INSERT INTO sqlstate_test.sqlstate_child VALUES (1, 999)') AS state;

-- ============================================================================
-- 3. 42P01 undefined_table
-- ============================================================================

-- begin-expected
-- columns: state
-- row: 42P01
-- end-expected
SELECT get_sqlstate('SELECT * FROM sqlstate_test.nonexistent_table_xyz') AS state;

-- ============================================================================
-- 4. 42703 undefined_column
-- ============================================================================

-- begin-expected
-- columns: state
-- row: 42703
-- end-expected
SELECT get_sqlstate('SELECT nonexistent_col FROM sqlstate_test.sqlstate_uniq') AS state;

-- ============================================================================
-- 5. 22012 division_by_zero
-- ============================================================================

-- begin-expected
-- columns: state
-- row: 22012
-- end-expected
SELECT get_sqlstate('SELECT 1/0') AS state;

-- ============================================================================
-- 6. 23514 check_violation
-- ============================================================================

CREATE TABLE sqlstate_chk (id integer, val integer CHECK (val > 0));

-- begin-expected
-- columns: state
-- row: 23514
-- end-expected
SELECT get_sqlstate('INSERT INTO sqlstate_test.sqlstate_chk VALUES (1, -5)') AS state;

-- ============================================================================
-- 7. 23502 not_null_violation
-- ============================================================================

CREATE TABLE sqlstate_nn (id integer NOT NULL);

-- begin-expected
-- columns: state
-- row: 23502
-- end-expected
SELECT get_sqlstate('INSERT INTO sqlstate_test.sqlstate_nn VALUES (NULL)') AS state;

-- ============================================================================
-- 8. 42601 syntax_error
-- ============================================================================

-- begin-expected
-- columns: state
-- row: 42601
-- end-expected
SELECT get_sqlstate('SELCT 1') AS state;

-- ============================================================================
-- 9. 22P02 invalid_text_representation (bad cast)
-- ============================================================================

-- begin-expected
-- columns: state
-- row: 22P02
-- end-expected
SELECT get_sqlstate($$SELECT 'notanumber'::integer$$) AS state;

-- ============================================================================
-- 10. 42P07 duplicate_table
-- ============================================================================

-- begin-expected
-- columns: state
-- row: 42P07
-- end-expected
SELECT get_sqlstate('CREATE TABLE sqlstate_test.sqlstate_uniq (id integer)') AS state;

-- ============================================================================
-- 11. 22003 numeric_value_out_of_range
-- ============================================================================

-- begin-expected
-- columns: state
-- row: 22003
-- end-expected
SELECT get_sqlstate('SELECT 2147483648::integer') AS state;

-- ============================================================================
-- 12. 22001 string_data_right_truncation (varchar length exceeded)
-- ============================================================================

CREATE TABLE sqlstate_trunc (val varchar(3));

-- begin-expected
-- columns: state
-- row: 22001
-- end-expected
SELECT get_sqlstate($$INSERT INTO sqlstate_test.sqlstate_trunc VALUES ('toolong')$$) AS state;

-- ============================================================================
-- 13. 42804 datatype_mismatch
-- ============================================================================

CREATE TABLE sqlstate_types (id integer, val text);

-- note: Trying to use integer where text is expected in UNION
-- begin-expected
-- columns: state
-- row: 42804
-- end-expected
SELECT get_sqlstate('SELECT id FROM sqlstate_test.sqlstate_types UNION ALL SELECT val FROM sqlstate_test.sqlstate_types') AS state;

-- ============================================================================
-- 14. 25001 active_sql_transaction (DDL in read-only)
-- ============================================================================

-- note: SET TRANSACTION READ ONLY + write should give 25006
-- This test verifies read-only transaction enforcement
CREATE FUNCTION sqlstate_readonly_test() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  state text;
BEGIN
  BEGIN
    SET TRANSACTION READ ONLY;
    INSERT INTO sqlstate_test.sqlstate_uniq VALUES (999);
    RETURN 'OK';
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS state = RETURNED_SQLSTATE;
    RETURN state;
  END;
END;
$$;

-- begin-expected
-- columns: state
-- row: 25006
-- end-expected
SELECT sqlstate_readonly_test() AS state;

-- ============================================================================
-- 15. PL/pgSQL EXCEPTION block catches specific SQLSTATE
-- ============================================================================

CREATE FUNCTION sqlstate_catch_specific() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO sqlstate_test.sqlstate_uniq VALUES (1);
  RETURN 'no error';
EXCEPTION
  WHEN unique_violation THEN
    RETURN 'caught unique_violation';
  WHEN OTHERS THEN
    RETURN 'caught other';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught unique_violation
-- end-expected
SELECT sqlstate_catch_specific() AS result;

-- ============================================================================
-- 16. GET STACKED DIAGNOSTICS full fields
-- ============================================================================

CREATE FUNCTION sqlstate_full_diag() RETURNS TABLE(sqlstate_val text, msg text, detail_val text)
LANGUAGE plpgsql AS $$
DECLARE
  v_state text;
  v_msg text;
  v_detail text;
BEGIN
  INSERT INTO sqlstate_test.sqlstate_uniq VALUES (1);
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS
    v_state = RETURNED_SQLSTATE,
    v_msg = MESSAGE_TEXT,
    v_detail = PG_EXCEPTION_DETAIL;
  sqlstate_val := v_state;
  msg := v_msg;
  detail_val := v_detail;
  RETURN NEXT;
END;
$$;

-- begin-expected
-- columns: sqlstate_val
-- row: 23505
-- end-expected
SELECT sqlstate_val FROM sqlstate_full_diag();

-- ============================================================================
-- 17. 42P06 duplicate_schema
-- ============================================================================

-- begin-expected
-- columns: state
-- row: 42P06
-- end-expected
SELECT get_sqlstate('CREATE SCHEMA sqlstate_test') AS state;

-- ============================================================================
-- 18. 42883 undefined_function
-- ============================================================================

-- begin-expected
-- columns: state
-- row: 42883
-- end-expected
SELECT get_sqlstate('SELECT no_such_function_xyz()') AS state;

-- ============================================================================
-- 19. P0001 raise_exception (user-raised error)
-- ============================================================================

CREATE FUNCTION sqlstate_raise() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'custom error';
END;
$$;

-- begin-expected
-- columns: state
-- row: P0001
-- end-expected
SELECT get_sqlstate('SELECT sqlstate_test.sqlstate_raise()') AS state;

-- ============================================================================
-- 20. P0004 assert_failure (PL/pgSQL ASSERT)
-- ============================================================================

CREATE FUNCTION sqlstate_assert_fail() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  ASSERT false, 'test assertion';
END;
$$;

-- begin-expected
-- columns: state
-- row: P0004
-- end-expected
SELECT get_sqlstate('SELECT sqlstate_test.sqlstate_assert_fail()') AS state;

-- ============================================================================
-- 21. RAISE with custom SQLSTATE
-- ============================================================================

CREATE FUNCTION sqlstate_custom_raise() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = 'XX123', MESSAGE = 'custom code error';
END;
$$;

-- begin-expected
-- columns: state
-- row: XX123
-- end-expected
SELECT get_sqlstate('SELECT sqlstate_test.sqlstate_custom_raise()') AS state;

-- ============================================================================
-- 22. 40001 serialization_failure (simulated via RAISE)
-- ============================================================================

-- note: True serialization failures need concurrent transactions;
-- verify the SQLSTATE constant is recognized
CREATE FUNCTION sqlstate_serial_raise() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = 'serialization_failure', MESSAGE = 'simulated';
END;
$$;

-- begin-expected
-- columns: state
-- row: 40001
-- end-expected
SELECT get_sqlstate('SELECT sqlstate_test.sqlstate_serial_raise()') AS state;

-- ============================================================================
-- 23. Multiple WHEN clauses in EXCEPTION block
-- ============================================================================

CREATE FUNCTION sqlstate_multi_when(mode text) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  IF mode = 'div' THEN
    PERFORM 1/0;
  ELSIF mode = 'null' THEN
    INSERT INTO sqlstate_test.sqlstate_nn VALUES (NULL);
  ELSIF mode = 'uniq' THEN
    INSERT INTO sqlstate_test.sqlstate_uniq VALUES (1);
  END IF;
  RETURN 'no error';
EXCEPTION
  WHEN division_by_zero THEN RETURN 'division_by_zero';
  WHEN not_null_violation THEN RETURN 'not_null_violation';
  WHEN unique_violation THEN RETURN 'unique_violation';
END;
$$;

-- begin-expected
-- columns: r1, r2, r3
-- row: division_by_zero, not_null_violation, unique_violation
-- end-expected
SELECT
  sqlstate_multi_when('div') AS r1,
  sqlstate_multi_when('null') AS r2,
  sqlstate_multi_when('uniq') AS r3;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA sqlstate_test CASCADE;
SET search_path = public;
