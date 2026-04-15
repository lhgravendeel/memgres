-- ============================================================================
-- Feature Comparison: check_function_bodies GUC
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests the check_function_bodies GUC which controls whether PL/pgSQL
-- function bodies are validated at CREATE FUNCTION time.
-- When OFF (used by pg_dump), function bodies are not validated until call.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS cfb_test CASCADE;
CREATE SCHEMA cfb_test;
SET search_path = cfb_test, public;

-- ============================================================================
-- 1. Default: check_function_bodies = on
-- ============================================================================

-- begin-expected
-- columns: check_function_bodies
-- row: on
-- end-expected
SHOW check_function_bodies;

-- ============================================================================
-- 2. SET check_function_bodies = off
-- ============================================================================

SET check_function_bodies = off;

-- begin-expected
-- columns: check_function_bodies
-- row: off
-- end-expected
SHOW check_function_bodies;

-- ============================================================================
-- 3. With OFF: function with reference to nonexistent table should CREATE OK
-- ============================================================================

-- note: With check_function_bodies = off, PG accepts this function
-- even though the table doesn't exist yet
CREATE FUNCTION cfb_deferred_ref() RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  cnt integer;
BEGIN
  SELECT count(*) INTO cnt FROM cfb_deferred_table;
  RETURN cnt;
END;
$$;

-- note: The function was created successfully
-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_proc
  WHERE proname = 'cfb_deferred_ref'
    AND pronamespace = 'cfb_test'::regnamespace
) AS exists;

-- ============================================================================
-- 4. Calling the deferred function fails (table doesn't exist yet)
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: cfb_deferred_table
-- end-expected-error
SELECT cfb_deferred_ref();

-- ============================================================================
-- 5. Create the table, then function works
-- ============================================================================

CREATE TABLE cfb_deferred_table (id integer);
INSERT INTO cfb_deferred_table VALUES (1), (2), (3);

-- begin-expected
-- columns: result
-- row: 3
-- end-expected
SELECT cfb_deferred_ref() AS result;

DROP TABLE cfb_deferred_table;

-- ============================================================================
-- 6. With OFF: function referencing nonexistent function
-- ============================================================================

CREATE FUNCTION cfb_calls_missing() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RETURN cfb_nonexistent_function();
END;
$$;

-- note: Created OK with check_function_bodies = off
-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_proc
  WHERE proname = 'cfb_calls_missing'
    AND pronamespace = 'cfb_test'::regnamespace
) AS exists;

-- ============================================================================
-- 7. Restore check_function_bodies = on
-- ============================================================================

SET check_function_bodies = on;

-- begin-expected
-- columns: check_function_bodies
-- row: on
-- end-expected
SHOW check_function_bodies;

-- ============================================================================
-- 8. With ON: SQL function bodies validated at create time
-- ============================================================================

-- note: SQL functions are always validated regardless of GUC in PG.
-- PL/pgSQL functions are validated only when check_function_bodies = on.
-- This tests that a SQL function referencing a missing table always fails.
-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
CREATE FUNCTION cfb_sql_bad() RETURNS integer
LANGUAGE sql AS $$ SELECT count(*)::integer FROM totally_nonexistent_xyz $$;

-- ============================================================================
-- 9. pg_dump compatibility pattern: SET OFF, create functions, SET ON
-- ============================================================================

SET check_function_bodies = off;

-- Create multiple functions that might reference each other (forward refs)
CREATE FUNCTION cfb_helper() RETURNS integer
LANGUAGE plpgsql AS $$
BEGIN
  RETURN 42;
END;
$$;

CREATE FUNCTION cfb_caller() RETURNS integer
LANGUAGE plpgsql AS $$
BEGIN
  RETURN cfb_helper() + 1;
END;
$$;

SET check_function_bodies = on;

-- Both should work fine
-- begin-expected
-- columns: result
-- row: 43
-- end-expected
SELECT cfb_caller() AS result;

-- ============================================================================
-- 10. Valid function creation works with ON
-- ============================================================================

CREATE TABLE cfb_real_table (id integer, val text);
INSERT INTO cfb_real_table VALUES (1, 'hello');

CREATE FUNCTION cfb_valid_func() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v text;
BEGIN
  SELECT val INTO v FROM cfb_real_table WHERE id = 1;
  RETURN v;
END;
$$;

-- begin-expected
-- columns: result
-- row: hello
-- end-expected
SELECT cfb_valid_func() AS result;

-- ============================================================================
-- Cleanup
-- ============================================================================

SET check_function_bodies = on;
DROP SCHEMA cfb_test CASCADE;
SET search_path = public;
