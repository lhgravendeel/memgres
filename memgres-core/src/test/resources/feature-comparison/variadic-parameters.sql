-- ============================================================================
-- Feature Comparison: VARIADIC Parameters
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests VARIADIC parameter handling in user-defined functions: argument
-- collection into arrays, VARIADIC keyword at call site, mixed params, etc.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS var_test CASCADE;
CREATE SCHEMA var_test;
SET search_path = var_test, public;

-- ============================================================================
-- 1. Basic VARIADIC text[] parameter
-- ============================================================================

CREATE FUNCTION var_concat_all(VARIADIC parts text[]) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  result text := '';
  p text;
BEGIN
  FOREACH p IN ARRAY parts LOOP
    result := result || p;
  END LOOP;
  RETURN result;
END;
$$;

-- begin-expected
-- columns: result
-- row: abc
-- end-expected
SELECT var_concat_all('a', 'b', 'c') AS result;

-- ============================================================================
-- 2. VARIADIC with single argument
-- ============================================================================

-- begin-expected
-- columns: result
-- row: hello
-- end-expected
SELECT var_concat_all('hello') AS result;

-- ============================================================================
-- 3. VARIADIC with no variadic arguments (empty array)
-- ============================================================================

-- note: Calling with VARIADIC ARRAY[]::text[] explicitly
-- begin-expected
-- columns: result
-- row:
-- end-expected
SELECT var_concat_all(VARIADIC ARRAY[]::text[]) AS result;

-- ============================================================================
-- 4. Mixed fixed + VARIADIC parameters
-- ============================================================================

CREATE FUNCTION var_with_prefix(prefix text, VARIADIC parts text[]) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  result text := prefix || ':';
  p text;
BEGIN
  FOREACH p IN ARRAY parts LOOP
    result := result || p || ',';
  END LOOP;
  RETURN rtrim(result, ',');
END;
$$;

-- begin-expected
-- columns: result
-- row: hello:a,b,c
-- end-expected
SELECT var_with_prefix('hello', 'a', 'b', 'c') AS result;

-- ============================================================================
-- 5. Mixed fixed + VARIADIC with just the prefix
-- ============================================================================

-- begin-expected-error
-- message-like: var_with_prefix
-- end-expected-error
SELECT var_with_prefix('hello') AS result;

-- ============================================================================
-- 6. VARIADIC integer[] parameter
-- ============================================================================

CREATE FUNCTION var_sum_all(VARIADIC nums integer[]) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  total integer := 0;
  n integer;
BEGIN
  FOREACH n IN ARRAY nums LOOP
    total := total + n;
  END LOOP;
  RETURN total;
END;
$$;

-- begin-expected
-- columns: result
-- row: 15
-- end-expected
SELECT var_sum_all(1, 2, 3, 4, 5) AS result;

-- begin-expected
-- columns: result
-- row: 42
-- end-expected
SELECT var_sum_all(42) AS result;

-- ============================================================================
-- 7. Calling VARIADIC function with explicit VARIADIC keyword
-- ============================================================================

-- note: VARIADIC keyword at call site passes array directly
-- begin-expected
-- columns: result
-- row: 6
-- end-expected
SELECT var_sum_all(VARIADIC ARRAY[1, 2, 3]) AS result;

-- ============================================================================
-- 8. VARIADIC with array_length check
-- ============================================================================

CREATE FUNCTION var_count_args(VARIADIC args text[]) RETURNS integer
LANGUAGE plpgsql AS $$
BEGIN
  RETURN array_length(args, 1);
END;
$$;

-- begin-expected
-- columns: result
-- row: 4
-- end-expected
SELECT var_count_args('a', 'b', 'c', 'd') AS result;

-- ============================================================================
-- 9. VARIADIC in SQL function
-- ============================================================================

CREATE FUNCTION var_sql_concat(VARIADIC parts text[]) RETURNS text
LANGUAGE sql AS $$ SELECT array_to_string(parts, '-') $$;

-- begin-expected
-- columns: result
-- row: x-y-z
-- end-expected
SELECT var_sql_concat('x', 'y', 'z') AS result;

-- ============================================================================
-- 10. VARIADIC with NULL arguments
-- ============================================================================

CREATE FUNCTION var_null_safe(VARIADIC parts text[]) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  result text := '';
  p text;
BEGIN
  FOREACH p IN ARRAY parts LOOP
    result := result || COALESCE(p, 'NULL') || ',';
  END LOOP;
  RETURN rtrim(result, ',');
END;
$$;

-- begin-expected
-- columns: result
-- row: a,NULL,b
-- end-expected
SELECT var_null_safe('a', NULL, 'b') AS result;

-- ============================================================================
-- 11. Built-in VARIADIC functions: concat()
-- ============================================================================

-- note: PG's concat() is VARIADIC "any"
-- begin-expected
-- columns: result
-- row: hello world 42
-- end-expected
SELECT concat('hello', ' ', 'world', ' ', 42) AS result;

-- ============================================================================
-- 12. Built-in VARIADIC functions: concat_ws()
-- ============================================================================

-- begin-expected
-- columns: result
-- row: a-b-c
-- end-expected
SELECT concat_ws('-', 'a', 'b', 'c') AS result;

-- begin-expected
-- columns: result
-- row: a-c
-- end-expected
SELECT concat_ws('-', 'a', NULL, 'c') AS result;

-- ============================================================================
-- 13. Built-in VARIADIC: format()
-- ============================================================================

-- begin-expected
-- columns: result
-- row: Hello Alice, you have 5 items
-- end-expected
SELECT format('Hello %s, you have %s items', 'Alice', 5) AS result;

-- ============================================================================
-- 14. Built-in VARIADIC: COALESCE (though syntax, not function)
-- ============================================================================

-- begin-expected
-- columns: r1, r2
-- row: 1, hello
-- end-expected
SELECT
  COALESCE(NULL, NULL, 1) AS r1,
  COALESCE(NULL, 'hello', 'world') AS r2;

-- ============================================================================
-- 15. VARIADIC function overloading
-- ============================================================================

CREATE FUNCTION var_overload(a integer) RETURNS text
LANGUAGE sql AS $$ SELECT 'single: ' || a::text $$;

CREATE FUNCTION var_overload(VARIADIC a integer[]) RETURNS text
LANGUAGE sql AS $$ SELECT 'variadic: ' || array_to_string(a, ',') $$;

-- begin-expected
-- columns: result
-- row: single: 1
-- end-expected
SELECT var_overload(1) AS result;

-- begin-expected
-- columns: result
-- row: variadic: 1,2,3
-- end-expected
SELECT var_overload(1, 2, 3) AS result;

-- ============================================================================
-- 16. VARIADIC with two fixed params
-- ============================================================================

CREATE FUNCTION var_two_fixed(sep text, prefix text, VARIADIC parts text[]) RETURNS text
LANGUAGE sql AS $$ SELECT prefix || array_to_string(parts, sep) $$;

-- begin-expected
-- columns: result
-- row: >>a|b|c
-- end-expected
SELECT var_two_fixed('|', '>>', 'a', 'b', 'c') AS result;

-- ============================================================================
-- 17. Passing VARIADIC array from variable
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 10
-- end-expected
SELECT var_sum_all(VARIADIC ARRAY[1, 2, 3, 4]) AS result;

-- ============================================================================
-- 18. VARIADIC function used in WHERE clause
-- ============================================================================

CREATE TABLE var_items (id integer, name text);
INSERT INTO var_items VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

-- begin-expected
-- columns: id, name
-- row: 1, a
-- row: 3, c
-- end-expected
SELECT id, name FROM var_items
WHERE name = ANY(ARRAY['a', 'c'])
ORDER BY id;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA var_test CASCADE;
SET search_path = public;
