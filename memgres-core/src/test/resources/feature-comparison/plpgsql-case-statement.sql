-- ============================================================================
-- Feature Comparison: PL/pgSQL CASE Statement (Control Structure)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests the PL/pgSQL CASE statement (control flow), which is distinct from
-- the SQL CASE expression. The CASE statement is used in procedural code
-- to branch execution based on conditions or values.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS plcase_test CASCADE;
CREATE SCHEMA plcase_test;
SET search_path = plcase_test, public;

-- ============================================================================
-- 1. Simple CASE (searched form) with text values
-- ============================================================================

CREATE FUNCTION plcase_simple(val integer) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  CASE val
    WHEN 1 THEN RETURN 'one';
    WHEN 2 THEN RETURN 'two';
    WHEN 3 THEN RETURN 'three';
    ELSE RETURN 'other';
  END CASE;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3, r4
-- row: one, two, three, other
-- end-expected
SELECT
  plcase_simple(1) AS r1,
  plcase_simple(2) AS r2,
  plcase_simple(3) AS r3,
  plcase_simple(99) AS r4;

-- ============================================================================
-- 2. Simple CASE without ELSE (should raise case_not_found)
-- ============================================================================

CREATE FUNCTION plcase_no_else(val integer) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  CASE val
    WHEN 1 THEN RETURN 'one';
    WHEN 2 THEN RETURN 'two';
  END CASE;
  RETURN 'unreachable';
END;
$$;

-- begin-expected
-- columns: result
-- row: one
-- end-expected
SELECT plcase_no_else(1) AS result;

-- note: PG raises case_not_found (SQLSTATE 20000) when no WHEN matches and no ELSE
-- begin-expected-error
-- sqlstate: 20000
-- message-like: case not found
-- end-expected-error
SELECT plcase_no_else(99);

-- ============================================================================
-- 3. Searched CASE (no operand; each WHEN has a boolean expression)
-- ============================================================================

CREATE FUNCTION plcase_searched(val integer) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  CASE
    WHEN val < 0 THEN RETURN 'negative';
    WHEN val = 0 THEN RETURN 'zero';
    WHEN val BETWEEN 1 AND 10 THEN RETURN 'small';
    WHEN val BETWEEN 11 AND 100 THEN RETURN 'medium';
    ELSE RETURN 'large';
  END CASE;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3, r4, r5
-- row: negative, zero, small, medium, large
-- end-expected
SELECT
  plcase_searched(-5) AS r1,
  plcase_searched(0) AS r2,
  plcase_searched(7) AS r3,
  plcase_searched(50) AS r4,
  plcase_searched(500) AS r5;

-- ============================================================================
-- 4. CASE with multiple values in a single WHEN
-- ============================================================================

CREATE FUNCTION plcase_multi_when(val integer) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  CASE val
    WHEN 1, 2, 3 THEN RETURN 'low';
    WHEN 4, 5, 6 THEN RETURN 'mid';
    WHEN 7, 8, 9 THEN RETURN 'high';
    ELSE RETURN 'out of range';
  END CASE;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3, r4
-- row: low, mid, high, out of range
-- end-expected
SELECT
  plcase_multi_when(2) AS r1,
  plcase_multi_when(5) AS r2,
  plcase_multi_when(8) AS r3,
  plcase_multi_when(0) AS r4;

-- ============================================================================
-- 5. CASE with assignment (not RETURN)
-- ============================================================================

CREATE FUNCTION plcase_assign(val integer) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  result text;
BEGIN
  CASE val
    WHEN 1 THEN result := 'first';
    WHEN 2 THEN result := 'second';
    ELSE result := 'unknown';
  END CASE;
  RETURN result;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3
-- row: first, second, unknown
-- end-expected
SELECT
  plcase_assign(1) AS r1,
  plcase_assign(2) AS r2,
  plcase_assign(3) AS r3;

-- ============================================================================
-- 6. CASE with multiple statements per WHEN branch
-- ============================================================================

CREATE FUNCTION plcase_multi_stmt(val integer) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  label text;
  doubled integer;
BEGIN
  CASE
    WHEN val > 0 THEN
      label := 'positive';
      doubled := val * 2;
    WHEN val = 0 THEN
      label := 'zero';
      doubled := 0;
    ELSE
      label := 'negative';
      doubled := val * -2;
  END CASE;
  RETURN label || ':' || doubled::text;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3
-- row: positive:10, zero:0, negative:6
-- end-expected
SELECT
  plcase_multi_stmt(5) AS r1,
  plcase_multi_stmt(0) AS r2,
  plcase_multi_stmt(-3) AS r3;

-- ============================================================================
-- 7. CASE with text operand
-- ============================================================================

CREATE FUNCTION plcase_text(status text) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  CASE status
    WHEN 'active' THEN RETURN 'User is active';
    WHEN 'inactive' THEN RETURN 'User is inactive';
    WHEN 'banned' THEN RETURN 'User is banned';
    ELSE RETURN 'Unknown status: ' || COALESCE(status, 'NULL');
  END CASE;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3, r4
-- row: User is active, User is inactive, User is banned, Unknown status: pending
-- end-expected
SELECT
  plcase_text('active') AS r1,
  plcase_text('inactive') AS r2,
  plcase_text('banned') AS r3,
  plcase_text('pending') AS r4;

-- ============================================================================
-- 8. Nested CASE statements
-- ============================================================================

CREATE FUNCTION plcase_nested(category text, val integer) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  CASE category
    WHEN 'number' THEN
      CASE
        WHEN val > 0 THEN RETURN 'positive number';
        WHEN val = 0 THEN RETURN 'zero';
        ELSE RETURN 'negative number';
      END CASE;
    WHEN 'size' THEN
      CASE
        WHEN val < 10 THEN RETURN 'small';
        WHEN val < 100 THEN RETURN 'medium';
        ELSE RETURN 'large';
      END CASE;
    ELSE
      RETURN 'unknown category';
  END CASE;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3, r4
-- row: positive number, zero, medium, unknown category
-- end-expected
SELECT
  plcase_nested('number', 5) AS r1,
  plcase_nested('number', 0) AS r2,
  plcase_nested('size', 50) AS r3,
  plcase_nested('other', 0) AS r4;

-- ============================================================================
-- 9. CASE inside a loop
-- ============================================================================

CREATE FUNCTION plcase_in_loop() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  i integer;
  result text := '';
BEGIN
  FOR i IN 1..5 LOOP
    CASE i
      WHEN 1 THEN result := result || 'one ';
      WHEN 2 THEN result := result || 'two ';
      WHEN 3 THEN result := result || 'three ';
      ELSE result := result || i::text || ' ';
    END CASE;
  END LOOP;
  RETURN trim(result);
END;
$$;

-- begin-expected
-- columns: result
-- row: one two three 4 5
-- end-expected
SELECT plcase_in_loop() AS result;

-- ============================================================================
-- 10. CASE with NULL operand
-- ============================================================================

-- note: Simple CASE uses = comparison; NULL = NULL is false, so no WHEN matches NULL
CREATE FUNCTION plcase_null_operand() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  val integer := NULL;
BEGIN
  CASE val
    WHEN 1 THEN RETURN 'one';
    WHEN NULL THEN RETURN 'null-when';
    ELSE RETURN 'fell through to else';
  END CASE;
END;
$$;

-- begin-expected
-- columns: result
-- row: fell through to else
-- end-expected
SELECT plcase_null_operand() AS result;

-- ============================================================================
-- 11. Searched CASE with IS NULL
-- ============================================================================

CREATE FUNCTION plcase_is_null(val integer) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  CASE
    WHEN val IS NULL THEN RETURN 'null value';
    WHEN val > 0 THEN RETURN 'positive';
    ELSE RETURN 'non-positive';
  END CASE;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3
-- row: null value, positive, non-positive
-- end-expected
SELECT
  plcase_is_null(NULL) AS r1,
  plcase_is_null(5) AS r2,
  plcase_is_null(-1) AS r3;

-- ============================================================================
-- 12. CASE with RAISE inside branch
-- ============================================================================

CREATE FUNCTION plcase_raise(val integer) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  CASE
    WHEN val < 0 THEN
      RAISE EXCEPTION 'negative value not allowed: %', val;
    WHEN val = 0 THEN
      RAISE NOTICE 'zero value received';
      RETURN 'zero';
    ELSE
      RETURN 'ok: ' || val::text;
  END CASE;
END;
$$;

-- begin-expected
-- columns: result
-- row: ok: 5
-- end-expected
SELECT plcase_raise(5) AS result;

-- begin-expected-error
-- sqlstate: P0001
-- message-like: negative value not allowed
-- end-expected-error
SELECT plcase_raise(-1);

-- ============================================================================
-- 13. CASE with DML inside branch
-- ============================================================================

CREATE TABLE plcase_log (id serial, msg text);

CREATE FUNCTION plcase_dml(action text) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  CASE action
    WHEN 'insert' THEN
      INSERT INTO plcase_log (msg) VALUES ('inserted');
      RETURN 'inserted';
    WHEN 'delete' THEN
      DELETE FROM plcase_log;
      RETURN 'deleted';
    ELSE
      RETURN 'noop';
  END CASE;
END;
$$;

-- begin-expected
-- columns: result
-- row: inserted
-- end-expected
SELECT plcase_dml('insert') AS result;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::integer AS cnt FROM plcase_log;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: replica identity
-- end-expected-error
SELECT plcase_dml('delete') AS result;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::integer AS cnt FROM plcase_log;

-- ============================================================================
-- 14. CASE with boolean operand
-- ============================================================================

CREATE FUNCTION plcase_bool(flag boolean) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  CASE flag
    WHEN true THEN RETURN 'yes';
    WHEN false THEN RETURN 'no';
    ELSE RETURN 'null';
  END CASE;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3
-- row: yes, no, null
-- end-expected
SELECT
  plcase_bool(true) AS r1,
  plcase_bool(false) AS r2,
  plcase_bool(NULL) AS r3;

-- ============================================================================
-- 15. CASE with variable as operand
-- ============================================================================

CREATE FUNCTION plcase_var_operand(input integer) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  category integer;
BEGIN
  category := input / 10;
  CASE category
    WHEN 0 THEN RETURN 'single digit';
    WHEN 1 THEN RETURN 'teens';
    WHEN 2 THEN RETURN 'twenties';
    ELSE RETURN 'thirty+';
  END CASE;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3, r4
-- row: single digit, teens, twenties, thirty+
-- end-expected
SELECT
  plcase_var_operand(5) AS r1,
  plcase_var_operand(15) AS r2,
  plcase_var_operand(25) AS r3,
  plcase_var_operand(42) AS r4;

-- ============================================================================
-- 16. SQL CASE expression still works (regression check)
-- ============================================================================

-- note: Ensure PL/pgSQL CASE statement doesn't break SQL CASE expression
CREATE FUNCTION plcase_sql_expr(val integer) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  result text;
BEGIN
  SELECT CASE val WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END INTO result;
  RETURN result;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3
-- row: one, two, other
-- end-expected
SELECT
  plcase_sql_expr(1) AS r1,
  plcase_sql_expr(2) AS r2,
  plcase_sql_expr(99) AS r3;

-- ============================================================================
-- 17. Both SQL CASE expression and PL/pgSQL CASE statement in same function
-- ============================================================================

CREATE FUNCTION plcase_both(val integer) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  prefix text;
  suffix text;
BEGIN
  -- PL/pgSQL CASE statement
  CASE
    WHEN val > 0 THEN prefix := 'pos';
    ELSE prefix := 'non-pos';
  END CASE;

  -- SQL CASE expression
  SELECT CASE WHEN val % 2 = 0 THEN 'even' ELSE 'odd' END INTO suffix;

  RETURN prefix || '-' || suffix;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3
-- row: pos-odd, pos-even, non-pos-even
-- end-expected
SELECT
  plcase_both(3) AS r1,
  plcase_both(4) AS r2,
  plcase_both(0) AS r3;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA plcase_test CASCADE;
SET search_path = public;
