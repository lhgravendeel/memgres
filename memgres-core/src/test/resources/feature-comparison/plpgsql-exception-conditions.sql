-- ============================================================================
-- Feature Comparison: PL/pgSQL Exception Condition Names
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests that PL/pgSQL EXCEPTION blocks correctly recognize and catch named
-- condition names beyond the ~32 currently implemented in Memgres.
-- Each test triggers a specific error and catches it by condition name.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS exc_test CASCADE;
CREATE SCHEMA exc_test;
SET search_path = exc_test, public;

-- Helper: catch a specific condition from dynamic SQL, return 'caught' or SQLSTATE
CREATE FUNCTION exc_catch(sql_text text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  state text;
BEGIN
  EXECUTE sql_text;
  RETURN 'no error';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS state = RETURNED_SQLSTATE;
  RETURN state;
END;
$$;

-- ============================================================================
-- SECTION A: Conditions already implemented (verify still work)
-- ============================================================================

-- ============================================================================
-- 1. division_by_zero (22012)
-- ============================================================================

CREATE FUNCTION exc_div_zero() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1/0;
  RETURN 'no error';
EXCEPTION WHEN division_by_zero THEN
  RETURN 'caught division_by_zero';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught division_by_zero
-- end-expected
SELECT exc_div_zero() AS result;

-- ============================================================================
-- 2. unique_violation (23505)
-- ============================================================================

CREATE TABLE exc_uniq (id integer PRIMARY KEY);
INSERT INTO exc_uniq VALUES (1);

CREATE FUNCTION exc_unique_viol() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO exc_uniq VALUES (1);
  RETURN 'no error';
EXCEPTION WHEN unique_violation THEN
  RETURN 'caught unique_violation';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught unique_violation
-- end-expected
SELECT exc_unique_viol() AS result;

-- ============================================================================
-- 3. not_null_violation (23502)
-- ============================================================================

CREATE TABLE exc_nn (id integer NOT NULL);

CREATE FUNCTION exc_nn_viol() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO exc_nn VALUES (NULL);
  RETURN 'no error';
EXCEPTION WHEN not_null_violation THEN
  RETURN 'caught not_null_violation';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught not_null_violation
-- end-expected
SELECT exc_nn_viol() AS result;

-- ============================================================================
-- 4. foreign_key_violation (23503)
-- ============================================================================

CREATE TABLE exc_parent (id integer PRIMARY KEY);
CREATE TABLE exc_child (id integer, pid integer REFERENCES exc_parent(id));

CREATE FUNCTION exc_fk_viol() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO exc_child VALUES (1, 999);
  RETURN 'no error';
EXCEPTION WHEN foreign_key_violation THEN
  RETURN 'caught foreign_key_violation';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught foreign_key_violation
-- end-expected
SELECT exc_fk_viol() AS result;

-- ============================================================================
-- 5. check_violation (23514)
-- ============================================================================

CREATE TABLE exc_chk (val integer CHECK (val > 0));

CREATE FUNCTION exc_check_viol() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO exc_chk VALUES (-1);
  RETURN 'no error';
EXCEPTION WHEN check_violation THEN
  RETURN 'caught check_violation';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught check_violation
-- end-expected
SELECT exc_check_viol() AS result;

-- ============================================================================
-- 6. undefined_table (42P01)
-- ============================================================================

CREATE FUNCTION exc_undef_table() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM * FROM nonexistent_table_xyz123;
  RETURN 'no error';
EXCEPTION WHEN undefined_table THEN
  RETURN 'caught undefined_table';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught undefined_table
-- end-expected
SELECT exc_undef_table() AS result;

-- ============================================================================
-- 7. undefined_column (42703)
-- ============================================================================

CREATE FUNCTION exc_undef_col() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM nonexistent_column_xyz FROM exc_uniq;
  RETURN 'no error';
EXCEPTION WHEN undefined_column THEN
  RETURN 'caught undefined_column';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught undefined_column
-- end-expected
SELECT exc_undef_col() AS result;

-- ============================================================================
-- 8. invalid_text_representation (22P02)
-- ============================================================================

CREATE FUNCTION exc_bad_cast() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 'notanumber'::integer;
  RETURN 'no error';
EXCEPTION WHEN invalid_text_representation THEN
  RETURN 'caught invalid_text_representation';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught invalid_text_representation
-- end-expected
SELECT exc_bad_cast() AS result;

-- ============================================================================
-- 9. numeric_value_out_of_range (22003)
-- ============================================================================

CREATE FUNCTION exc_numeric_range() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 2147483648::integer;
  RETURN 'no error';
EXCEPTION WHEN numeric_value_out_of_range THEN
  RETURN 'caught numeric_value_out_of_range';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught numeric_value_out_of_range
-- end-expected
SELECT exc_numeric_range() AS result;

-- ============================================================================
-- 10. string_data_right_truncation (22001)
-- ============================================================================

CREATE TABLE exc_trunc (val varchar(3));

CREATE FUNCTION exc_trunc_test() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO exc_trunc VALUES ('toolong');
  RETURN 'no error';
EXCEPTION WHEN string_data_right_truncation THEN
  RETURN 'caught string_data_right_truncation';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught string_data_right_truncation
-- end-expected
SELECT exc_trunc_test() AS result;

-- ============================================================================
-- 11. no_data_found (P0002) via TOO MANY INTO targets
-- ============================================================================

CREATE FUNCTION exc_no_data() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v integer;
BEGIN
  SELECT id INTO STRICT v FROM exc_uniq WHERE id = 99999;
  RETURN 'no error';
EXCEPTION WHEN no_data_found THEN
  RETURN 'caught no_data_found';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught no_data_found
-- end-expected
SELECT exc_no_data() AS result;

-- ============================================================================
-- 12. too_many_rows (P0003)
-- ============================================================================

INSERT INTO exc_uniq VALUES (2), (3);

CREATE FUNCTION exc_too_many() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v integer;
BEGIN
  SELECT id INTO STRICT v FROM exc_uniq;
  RETURN 'no error';
EXCEPTION WHEN too_many_rows THEN
  RETURN 'caught too_many_rows';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught too_many_rows
-- end-expected
SELECT exc_too_many() AS result;

-- ============================================================================
-- 13. raise_exception (P0001)
-- ============================================================================

CREATE FUNCTION exc_raise() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'custom error message';
EXCEPTION WHEN raise_exception THEN
  RETURN 'caught raise_exception';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught raise_exception
-- end-expected
SELECT exc_raise() AS result;

-- ============================================================================
-- 14. assert_failure (P0004)
-- ============================================================================

CREATE FUNCTION exc_assert() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  ASSERT false, 'test assertion';
EXCEPTION WHEN assert_failure THEN
  RETURN 'caught assert_failure';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught assert_failure
-- end-expected
SELECT exc_assert() AS result;

-- ============================================================================
-- 15. duplicate_table (42P07)
-- ============================================================================

CREATE FUNCTION exc_dup_table() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE 'CREATE TABLE exc_uniq (id integer)';
  RETURN 'no error';
EXCEPTION WHEN duplicate_table THEN
  RETURN 'caught duplicate_table';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught duplicate_table
-- end-expected
SELECT exc_dup_table() AS result;

-- ============================================================================
-- 16. undefined_function (42883)
-- ============================================================================

CREATE FUNCTION exc_undef_func() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM no_such_function_xyz_123();
  RETURN 'no error';
EXCEPTION WHEN undefined_function THEN
  RETURN 'caught undefined_function';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught undefined_function
-- end-expected
SELECT exc_undef_func() AS result;

-- ============================================================================
-- SECTION B: Conditions NOT yet implemented in Memgres
--            (these test whether they're recognized as condition names)
-- ============================================================================

-- ============================================================================
-- 17. insufficient_privilege (42501) — via RAISE simulation
-- ============================================================================

-- note: We can't easily trigger real privilege errors in single-user mode,
-- so we RAISE with the SQLSTATE and catch by condition name
CREATE FUNCTION exc_insuff_priv() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'permission denied';
EXCEPTION WHEN insufficient_privilege THEN
  RETURN 'caught insufficient_privilege';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught insufficient_privilege
-- end-expected
SELECT exc_insuff_priv() AS result;

-- ============================================================================
-- 18. deadlock_detected (40P01) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_deadlock() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '40P01', MESSAGE = 'deadlock detected';
EXCEPTION WHEN deadlock_detected THEN
  RETURN 'caught deadlock_detected';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught deadlock_detected
-- end-expected
SELECT exc_deadlock() AS result;

-- ============================================================================
-- 19. lock_not_available (55P03) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_lock_unavail() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '55P03', MESSAGE = 'lock not available';
EXCEPTION WHEN lock_not_available THEN
  RETURN 'caught lock_not_available';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught lock_not_available
-- end-expected
SELECT exc_lock_unavail() AS result;

-- ============================================================================
-- 20. invalid_cursor_state (24000) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_cursor_state() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '24000', MESSAGE = 'invalid cursor state';
EXCEPTION WHEN invalid_cursor_state THEN
  RETURN 'caught invalid_cursor_state';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught invalid_cursor_state
-- end-expected
SELECT exc_cursor_state() AS result;

-- ============================================================================
-- 21. invalid_transaction_state (25000) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_txn_state() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '25000', MESSAGE = 'invalid transaction state';
EXCEPTION WHEN invalid_transaction_state THEN
  RETURN 'caught invalid_transaction_state';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught invalid_transaction_state
-- end-expected
SELECT exc_txn_state() AS result;

-- ============================================================================
-- 22. null_value_not_allowed (22004) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_null_not_allowed() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '22004', MESSAGE = 'null value not allowed';
EXCEPTION WHEN null_value_not_allowed THEN
  RETURN 'caught null_value_not_allowed';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught null_value_not_allowed
-- end-expected
SELECT exc_null_not_allowed() AS result;

-- ============================================================================
-- 23. invalid_regular_expression (2201B) — via real trigger
-- ============================================================================

CREATE FUNCTION exc_bad_regex() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 'test' ~ '[invalid';
  RETURN 'no error';
EXCEPTION WHEN invalid_regular_expression THEN
  RETURN 'caught invalid_regular_expression';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught invalid_regular_expression
-- end-expected
SELECT exc_bad_regex() AS result;

-- ============================================================================
-- 24. datetime_field_overflow (22008) — via real trigger
-- ============================================================================

CREATE FUNCTION exc_datetime_overflow() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM '2023-13-01'::date;
  RETURN 'no error';
EXCEPTION WHEN datetime_field_overflow THEN
  RETURN 'caught datetime_field_overflow';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught datetime_field_overflow
-- end-expected
SELECT exc_datetime_overflow() AS result;

-- ============================================================================
-- 25. object_in_use (55006) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_obj_in_use() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '55006', MESSAGE = 'object in use';
EXCEPTION WHEN object_in_use THEN
  RETURN 'caught object_in_use';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught object_in_use
-- end-expected
SELECT exc_obj_in_use() AS result;

-- ============================================================================
-- 26. program_limit_exceeded (54000) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_prog_limit() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '54000', MESSAGE = 'program limit exceeded';
EXCEPTION WHEN program_limit_exceeded THEN
  RETURN 'caught program_limit_exceeded';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught program_limit_exceeded
-- end-expected
SELECT exc_prog_limit() AS result;

-- ============================================================================
-- 27. case_not_found (20000) — via real trigger
-- ============================================================================

-- note: This tests that case_not_found is recognized as a condition name
CREATE FUNCTION exc_case_not_found() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '20000', MESSAGE = 'case not found';
EXCEPTION WHEN case_not_found THEN
  RETURN 'caught case_not_found';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught case_not_found
-- end-expected
SELECT exc_case_not_found() AS result;

-- ============================================================================
-- 28. with_check_option_violation (44000) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_check_option() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '44000', MESSAGE = 'check option violation';
EXCEPTION WHEN with_check_option_violation THEN
  RETURN 'caught with_check_option_violation';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught with_check_option_violation
-- end-expected
SELECT exc_check_option() AS result;

-- ============================================================================
-- 29. triggered_action_exception (09000) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_triggered_action() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '09000', MESSAGE = 'triggered action';
EXCEPTION WHEN triggered_action_exception THEN
  RETURN 'caught triggered_action_exception';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught triggered_action_exception
-- end-expected
SELECT exc_triggered_action() AS result;

-- ============================================================================
-- 30. plpgsql_error (P0000) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_plpgsql_error() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = 'P0000', MESSAGE = 'plpgsql error';
EXCEPTION WHEN plpgsql_error THEN
  RETURN 'caught plpgsql_error';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught plpgsql_error
-- end-expected
SELECT exc_plpgsql_error() AS result;

-- ============================================================================
-- 31. invalid_escape_sequence (22025) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_bad_escape() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '22025', MESSAGE = 'invalid escape';
EXCEPTION WHEN invalid_escape_sequence THEN
  RETURN 'caught invalid_escape_sequence';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught invalid_escape_sequence
-- end-expected
SELECT exc_bad_escape() AS result;

-- ============================================================================
-- 32. name_too_long (42622) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_name_long() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '42622', MESSAGE = 'name too long';
EXCEPTION WHEN name_too_long THEN
  RETURN 'caught name_too_long';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught name_too_long
-- end-expected
SELECT exc_name_long() AS result;

-- ============================================================================
-- 33. external_routine_exception (38000) — via RAISE simulation
-- ============================================================================

CREATE FUNCTION exc_ext_routine() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '38000', MESSAGE = 'external routine error';
EXCEPTION WHEN external_routine_exception THEN
  RETURN 'caught external_routine_exception';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught external_routine_exception
-- end-expected
SELECT exc_ext_routine() AS result;

-- ============================================================================
-- SECTION C: Class-level condition matching
-- ============================================================================

-- ============================================================================
-- 34. data_exception (class 22) — catches any 22xxx error
-- ============================================================================

CREATE FUNCTION exc_data_exception() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1/0;
  RETURN 'no error';
EXCEPTION WHEN data_exception THEN
  RETURN 'caught data_exception';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught data_exception
-- end-expected
SELECT exc_data_exception() AS result;

-- ============================================================================
-- 35. integrity_constraint_violation (class 23) — catches any 23xxx
-- ============================================================================

CREATE FUNCTION exc_integrity() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO exc_uniq VALUES (1);
  RETURN 'no error';
EXCEPTION WHEN integrity_constraint_violation THEN
  RETURN 'caught integrity_constraint_violation';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught integrity_constraint_violation
-- end-expected
SELECT exc_integrity() AS result;

-- ============================================================================
-- 36. syntax_error_or_access_rule_violation (class 42) catches 42xxx
-- ============================================================================

CREATE FUNCTION exc_syntax_class() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'test access violation';
EXCEPTION WHEN syntax_error_or_access_rule_violation THEN
  RETURN 'caught syntax_error_or_access_rule_violation';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught syntax_error_or_access_rule_violation
-- end-expected
SELECT exc_syntax_class() AS result;

-- ============================================================================
-- 37. Multiple WHEN clauses: most specific wins (order matters)
-- ============================================================================

CREATE FUNCTION exc_specificity() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO exc_uniq VALUES (1);
  RETURN 'no error';
EXCEPTION
  WHEN unique_violation THEN
    RETURN 'caught specific: unique_violation';
  WHEN integrity_constraint_violation THEN
    RETURN 'caught class: integrity_constraint_violation';
END;
$$;

-- note: PG checks WHEN clauses in order; first match wins
-- begin-expected
-- columns: result
-- row: caught specific: unique_violation
-- end-expected
SELECT exc_specificity() AS result;

-- ============================================================================
-- 38. SQLSTATE by code: catch using SQLSTATE directly
-- ============================================================================

CREATE FUNCTION exc_sqlstate_direct() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1/0;
  RETURN 'no error';
EXCEPTION WHEN SQLSTATE '22012' THEN
  RETURN 'caught by SQLSTATE 22012';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught by SQLSTATE 22012
-- end-expected
SELECT exc_sqlstate_direct() AS result;

-- ============================================================================
-- 39. SQLSTATE by custom code
-- ============================================================================

CREATE FUNCTION exc_custom_sqlstate() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = 'MG001', MESSAGE = 'custom memgres error';
EXCEPTION WHEN SQLSTATE 'MG001' THEN
  RETURN 'caught custom SQLSTATE MG001';
END;
$$;

-- begin-expected
-- columns: result
-- row: caught custom SQLSTATE MG001
-- end-expected
SELECT exc_custom_sqlstate() AS result;

-- ============================================================================
-- 40. OR condition in WHEN: catch multiple conditions
-- ============================================================================

CREATE FUNCTION exc_or_conditions(mode text) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  IF mode = 'div' THEN
    PERFORM 1/0;
  ELSIF mode = 'uniq' THEN
    INSERT INTO exc_uniq VALUES (1);
  END IF;
  RETURN 'no error';
EXCEPTION
  WHEN division_by_zero OR unique_violation THEN
    RETURN 'caught either div_zero or unique_viol';
END;
$$;

-- begin-expected
-- columns: r1, r2
-- row: caught either div_zero or unique_viol, caught either div_zero or unique_viol
-- end-expected
SELECT
  exc_or_conditions('div') AS r1,
  exc_or_conditions('uniq') AS r2;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA exc_test CASCADE;
SET search_path = public;
