-- ============================================================================
-- Feature Comparison: PL/pgSQL Functions & Forward Reference Support
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- command: TAG   -> expected command tag
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS fn_test CASCADE;
CREATE SCHEMA fn_test;
SET search_path = fn_test, public;

DROP TABLE IF EXISTS fn_data CASCADE;
CREATE TABLE fn_data (id integer PRIMARY KEY, name text, val numeric(10,2));
INSERT INTO fn_data VALUES (1, 'alpha', 10.5), (2, 'beta', 20.0), (3, 'gamma', 30.75);

-- ============================================================================
-- 1. Basic SQL-language function
-- ============================================================================

CREATE FUNCTION fn_add_sql(a integer, b integer) RETURNS integer
LANGUAGE sql AS $$ SELECT a + b $$;

-- begin-expected
-- columns: result
-- row: 7
-- end-expected
SELECT fn_add_sql(3, 4) AS result;

-- ============================================================================
-- 2. Basic PL/pgSQL function
-- ============================================================================

CREATE FUNCTION fn_add_plpgsql(a integer, b integer) RETURNS integer
LANGUAGE plpgsql AS $$
BEGIN
  RETURN a + b;
END;
$$;

-- begin-expected
-- columns: result
-- row: 7
-- end-expected
SELECT fn_add_plpgsql(3, 4) AS result;

-- ============================================================================
-- 3. CREATE OR REPLACE FUNCTION
-- ============================================================================

CREATE FUNCTION fn_replaceable() RETURNS text LANGUAGE sql AS $$ SELECT 'v1'::text $$;

-- begin-expected
-- columns: result
-- row: v1
-- end-expected
SELECT fn_replaceable() AS result;

CREATE OR REPLACE FUNCTION fn_replaceable() RETURNS text LANGUAGE sql AS $$ SELECT 'v2'::text $$;

-- begin-expected
-- columns: result
-- row: v2
-- end-expected
SELECT fn_replaceable() AS result;

-- ============================================================================
-- 4. Forward reference: function body references nonexistent table
-- ============================================================================

-- note: PL/pgSQL bodies are NOT validated at CREATE time (only at call time)
CREATE FUNCTION fn_forward_ref() RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v text;
BEGIN
  SELECT name INTO v FROM fn_future_table LIMIT 1;
  RETURN v;
END;
$$;

-- Calling before table exists should fail
-- begin-expected-error
-- message-like: relation "fn_future_table" does not exist
-- end-expected-error
SELECT fn_forward_ref();

-- Now create the table and call succeeds
CREATE TABLE fn_future_table (name text);
INSERT INTO fn_future_table VALUES ('hello');

-- begin-expected
-- columns: fn_forward_ref
-- row: hello
-- end-expected
SELECT fn_forward_ref();

DROP TABLE fn_future_table CASCADE;

-- ============================================================================
-- 5. Forward reference: function calls another function not yet created
-- ============================================================================

CREATE FUNCTION fn_caller() RETURNS integer LANGUAGE plpgsql AS $$
BEGIN
  RETURN fn_callee_future();
END;
$$;

-- begin-expected-error
-- message-like: function fn_callee_future() does not exist
-- end-expected-error
SELECT fn_caller();

CREATE FUNCTION fn_callee_future() RETURNS integer LANGUAGE sql AS $$ SELECT 42 $$;

-- begin-expected
-- columns: fn_caller
-- row: 42
-- end-expected
SELECT fn_caller();

-- ============================================================================
-- 6. Forward reference: nonexistent column
-- ============================================================================

CREATE FUNCTION fn_bad_col() RETURNS text LANGUAGE plpgsql AS $$
DECLARE v text;
BEGIN
  SELECT nonexistent_col INTO v FROM fn_data LIMIT 1;
  RETURN v;
END;
$$;

-- begin-expected-error
-- message-like: column "nonexistent_col" does not exist
-- end-expected-error
SELECT fn_bad_col();

-- ============================================================================
-- 7. check_function_bodies GUC
-- ============================================================================

-- note: SQL-language functions ARE validated at CREATE time (unless check_function_bodies=off)

-- begin-expected-error
-- message-like: relation "fn_no_such_table_sql" does not exist
-- end-expected-error
CREATE FUNCTION fn_bad_sql_body() RETURNS integer LANGUAGE sql AS $$
  SELECT x FROM fn_no_such_table_sql
$$;

SET check_function_bodies = off;

-- command: CREATE FUNCTION
-- note: With check_function_bodies off, even SQL functions skip body validation
CREATE FUNCTION fn_bad_sql_body() RETURNS integer LANGUAGE sql AS $$
  SELECT x FROM fn_no_such_table_sql
$$;

SET check_function_bodies = on;

-- Calling the invalid SQL function still fails at runtime
-- begin-expected-error
-- message-like: relation "fn_no_such_table_sql" does not exist
-- end-expected-error
SELECT fn_bad_sql_body();

-- ============================================================================
-- 8. Function overloading
-- ============================================================================

CREATE FUNCTION fn_overload(a integer) RETURNS text LANGUAGE sql AS $$ SELECT 'int:'||a::text $$;
CREATE FUNCTION fn_overload(a text) RETURNS text LANGUAGE sql AS $$ SELECT 'text:'||a $$;
CREATE FUNCTION fn_overload(a integer, b integer) RETURNS text LANGUAGE sql AS $$ SELECT 'int2:'||(a+b)::text $$;

-- begin-expected
-- columns: r1
-- row: int:5
-- end-expected
SELECT fn_overload(5) AS r1;

-- begin-expected
-- columns: r2
-- row: text:hello
-- end-expected
SELECT fn_overload('hello') AS r2;

-- begin-expected
-- columns: r3
-- row: int2:9
-- end-expected
SELECT fn_overload(4, 5) AS r3;

-- ============================================================================
-- 9. VARIADIC parameters
-- ============================================================================

CREATE FUNCTION fn_variadic_sum(VARIADIC nums integer[]) RETURNS integer LANGUAGE plpgsql AS $$
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
SELECT fn_variadic_sum(1, 2, 3, 4, 5) AS result;

-- begin-expected
-- columns: result
-- row: 10
-- end-expected
SELECT fn_variadic_sum(10) AS result;

-- ============================================================================
-- 10. DEFAULT parameter values
-- ============================================================================

CREATE FUNCTION fn_with_defaults(a integer, b integer DEFAULT 10, c text DEFAULT 'x')
RETURNS text LANGUAGE sql AS $$ SELECT a::text || '-' || b::text || '-' || c $$;

-- begin-expected
-- columns: result
-- row: 1-10-x
-- end-expected
SELECT fn_with_defaults(1) AS result;

-- begin-expected
-- columns: result
-- row: 1-20-x
-- end-expected
SELECT fn_with_defaults(1, 20) AS result;

-- begin-expected
-- columns: result
-- row: 1-20-y
-- end-expected
SELECT fn_with_defaults(1, 20, 'y') AS result;

-- ============================================================================
-- 11. OUT parameters
-- ============================================================================

CREATE FUNCTION fn_out_params(IN x integer, OUT doubled integer, OUT tripled integer)
LANGUAGE plpgsql AS $$
BEGIN
  doubled := x * 2;
  tripled := x * 3;
END;
$$;

-- begin-expected
-- columns: doubled, tripled
-- row: 10, 15
-- end-expected
SELECT * FROM fn_out_params(5);

-- ============================================================================
-- 12. INOUT parameters
-- ============================================================================

CREATE FUNCTION fn_inout(INOUT val integer) LANGUAGE plpgsql AS $$
BEGIN
  val := val * val;
END;
$$;

-- begin-expected
-- columns: fn_inout
-- row: 25
-- end-expected
SELECT fn_inout(5);

-- ============================================================================
-- 13. RETURNS TABLE
-- ============================================================================

CREATE FUNCTION fn_returns_table(min_val numeric)
RETURNS TABLE(id integer, name text) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY SELECT d.id, d.name FROM fn_data d WHERE d.val >= min_val ORDER BY d.id;
END;
$$;

-- begin-expected
-- columns: id, name
-- row: 2, beta
-- row: 3, gamma
-- end-expected
SELECT * FROM fn_returns_table(20);

-- ============================================================================
-- 14. RETURNS SETOF with RETURN NEXT
-- ============================================================================

CREATE FUNCTION fn_setof_next() RETURNS SETOF integer LANGUAGE plpgsql AS $$
BEGIN
  RETURN NEXT 10;
  RETURN NEXT 20;
  RETURN NEXT 30;
  RETURN;
END;
$$;

-- begin-expected
-- columns: fn_setof_next
-- row: 10
-- row: 20
-- row: 30
-- end-expected
SELECT * FROM fn_setof_next();

-- ============================================================================
-- 15. RETURNS SETOF with RETURN QUERY
-- ============================================================================

CREATE FUNCTION fn_setof_query() RETURNS SETOF fn_data LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY SELECT * FROM fn_data WHERE val > 15 ORDER BY id;
  RETURN;
END;
$$;

-- begin-expected
-- columns: id, name, val
-- row: 2, beta, 20.00
-- row: 3, gamma, 30.75
-- end-expected
SELECT * FROM fn_setof_query();

-- ============================================================================
-- 16. Volatility: IMMUTABLE / STABLE / VOLATILE
-- ============================================================================

CREATE FUNCTION fn_immutable(a integer) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$ SELECT a * 2 $$;

CREATE FUNCTION fn_stable_now() RETURNS timestamptz
LANGUAGE sql STABLE AS $$ SELECT now() $$;

CREATE FUNCTION fn_volatile_random() RETURNS double precision
LANGUAGE sql VOLATILE AS $$ SELECT random() $$;

-- Verify they work
-- begin-expected
-- columns: result
-- row: 10
-- end-expected
SELECT fn_immutable(5) AS result;

-- ============================================================================
-- 17. Volatility: IMMUTABLE required for index expressions
-- ============================================================================

CREATE TABLE fn_idx_test (id serial PRIMARY KEY, val integer);
INSERT INTO fn_idx_test (val) VALUES (1), (2), (3);

-- IMMUTABLE function in expression index — should succeed
CREATE INDEX idx_immut ON fn_idx_test (fn_immutable(val));

-- VOLATILE function in expression index — PG allows this
CREATE FUNCTION fn_vol_for_idx(x integer) RETURNS integer
LANGUAGE sql VOLATILE AS $$ SELECT x + 1 $$;

CREATE INDEX idx_vol ON fn_idx_test (fn_vol_for_idx(val));

-- STABLE function in expression index — PG allows this too
CREATE FUNCTION fn_stable_for_idx(x integer) RETURNS integer
LANGUAGE sql STABLE AS $$ SELECT x + 1 $$;

CREATE INDEX idx_stable ON fn_idx_test (fn_stable_for_idx(val));

DROP TABLE fn_idx_test CASCADE;

-- ============================================================================
-- 18. Function SET clauses
-- ============================================================================

CREATE FUNCTION fn_set_clause() RETURNS text LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
  sp text;
BEGIN
  SHOW search_path INTO sp;
  RETURN sp;
END;
$$;

-- begin-expected
-- columns: fn_set_clause
-- row: pg_catalog
-- end-expected
SELECT fn_set_clause();

-- Verify search_path is restored after call
-- begin-expected
-- columns: search_path
-- row: fn_test, public
-- end-expected
SHOW search_path;

-- ============================================================================
-- 19. PL/pgSQL control flow: IF / ELSIF / ELSE
-- ============================================================================

CREATE FUNCTION fn_classify(n integer) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  IF n < 0 THEN
    RETURN 'negative';
  ELSIF n = 0 THEN
    RETURN 'zero';
  ELSIF n < 10 THEN
    RETURN 'small';
  ELSE
    RETURN 'large';
  END IF;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3, r4
-- row: negative, zero, small, large
-- end-expected
SELECT fn_classify(-5) AS r1, fn_classify(0) AS r2, fn_classify(7) AS r3, fn_classify(100) AS r4;

-- ============================================================================
-- 20. PL/pgSQL control flow: LOOP / EXIT
-- ============================================================================

CREATE FUNCTION fn_loop_sum(n integer) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
  i integer := 1;
  total integer := 0;
BEGIN
  LOOP
    EXIT WHEN i > n;
    total := total + i;
    i := i + 1;
  END LOOP;
  RETURN total;
END;
$$;

-- begin-expected
-- columns: result
-- row: 55
-- end-expected
SELECT fn_loop_sum(10) AS result;

-- ============================================================================
-- 21. PL/pgSQL control flow: WHILE
-- ============================================================================

CREATE FUNCTION fn_while_countdown(n integer) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  result text := '';
BEGIN
  WHILE n > 0 LOOP
    result := result || n::text;
    n := n - 1;
    IF n > 0 THEN result := result || ','; END IF;
  END LOOP;
  RETURN result;
END;
$$;

-- begin-expected
-- columns: result
-- row: 3,2,1
-- end-expected
SELECT fn_while_countdown(3) AS result;

-- ============================================================================
-- 22. PL/pgSQL control flow: FOR (integer range)
-- ============================================================================

CREATE FUNCTION fn_for_range(low integer, high integer) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  result text := '';
  i integer;
BEGIN
  FOR i IN low..high LOOP
    IF result <> '' THEN result := result || ','; END IF;
    result := result || i::text;
  END LOOP;
  RETURN result;
END;
$$;

-- begin-expected
-- columns: result
-- row: 3,4,5,6,7
-- end-expected
SELECT fn_for_range(3, 7) AS result;

-- ============================================================================
-- 23. PL/pgSQL control flow: FOR (query)
-- ============================================================================

CREATE FUNCTION fn_for_query() RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  result text := '';
  r RECORD;
BEGIN
  FOR r IN SELECT name FROM fn_data ORDER BY id LOOP
    IF result <> '' THEN result := result || ','; END IF;
    result := result || r.name;
  END LOOP;
  RETURN result;
END;
$$;

-- begin-expected
-- columns: result
-- row: alpha,beta,gamma
-- end-expected
SELECT fn_for_query() AS result;

-- ============================================================================
-- 24. PL/pgSQL control flow: FOREACH over array
-- ============================================================================

CREATE FUNCTION fn_foreach(arr integer[]) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
  total integer := 0;
  elem integer;
BEGIN
  FOREACH elem IN ARRAY arr LOOP
    total := total + elem;
  END LOOP;
  RETURN total;
END;
$$;

-- begin-expected
-- columns: result
-- row: 60
-- end-expected
SELECT fn_foreach(ARRAY[10, 20, 30]) AS result;

-- ============================================================================
-- 25. PL/pgSQL control flow: CONTINUE
-- ============================================================================

CREATE FUNCTION fn_skip_evens(n integer) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  result text := '';
  i integer;
BEGIN
  FOR i IN 1..n LOOP
    CONTINUE WHEN i % 2 = 0;
    IF result <> '' THEN result := result || ','; END IF;
    result := result || i::text;
  END LOOP;
  RETURN result;
END;
$$;

-- begin-expected
-- columns: result
-- row: 1,3,5
-- end-expected
SELECT fn_skip_evens(6) AS result;

-- ============================================================================
-- 26. PL/pgSQL variables: CONSTANT, NOT NULL
-- ============================================================================

CREATE FUNCTION fn_constant_test() RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
  c CONSTANT integer := 42;
BEGIN
  RETURN c;
END;
$$;

-- begin-expected
-- columns: result
-- row: 42
-- end-expected
SELECT fn_constant_test() AS result;

-- ============================================================================
-- 27. PL/pgSQL variables: %TYPE
-- ============================================================================

CREATE FUNCTION fn_type_ref() RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v fn_data.name%TYPE;
BEGIN
  SELECT name INTO v FROM fn_data WHERE id = 1;
  RETURN v;
END;
$$;

-- begin-expected
-- columns: result
-- row: alpha
-- end-expected
SELECT fn_type_ref() AS result;

-- ============================================================================
-- 28. PL/pgSQL variables: %ROWTYPE
-- ============================================================================

CREATE FUNCTION fn_rowtype_ref() RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  r fn_data%ROWTYPE;
BEGIN
  SELECT * INTO r FROM fn_data WHERE id = 2;
  RETURN r.name || ':' || r.val::text;
END;
$$;

-- begin-expected
-- columns: result
-- row: beta:20.00
-- end-expected
SELECT fn_rowtype_ref() AS result;

-- ============================================================================
-- 29. PL/pgSQL variables: RECORD
-- ============================================================================

CREATE FUNCTION fn_record_test() RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  r RECORD;
BEGIN
  SELECT id, name INTO r FROM fn_data WHERE id = 3;
  RETURN r.id::text || ':' || r.name;
END;
$$;

-- begin-expected
-- columns: result
-- row: 3:gamma
-- end-expected
SELECT fn_record_test() AS result;

-- ============================================================================
-- 30. PL/pgSQL RAISE NOTICE / WARNING / EXCEPTION
-- ============================================================================

-- note: RAISE NOTICE and WARNING produce server messages, not query results.
--       Only RAISE EXCEPTION actually stops execution and produces an error.

CREATE FUNCTION fn_raise_exception() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'something went wrong: %', 42;
END;
$$;

-- begin-expected-error
-- message-like: something went wrong: 42
-- end-expected-error
SELECT fn_raise_exception();

-- ============================================================================
-- 31. PL/pgSQL RAISE with ERRCODE
-- ============================================================================

CREATE FUNCTION fn_raise_errcode() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'custom error' USING ERRCODE = '12345';
END;
$$;

-- begin-expected-error
-- message-like: custom error
-- end-expected-error
SELECT fn_raise_errcode();

-- ============================================================================
-- 32. PL/pgSQL EXCEPTION block
-- ============================================================================

CREATE FUNCTION fn_exception_handler() RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1/0;
  RETURN 'no error';
EXCEPTION
  WHEN division_by_zero THEN
    RETURN 'caught division_by_zero';
END;
$$;

-- begin-expected
-- columns: fn_exception_handler
-- row: caught division_by_zero
-- end-expected
SELECT fn_exception_handler();

-- ============================================================================
-- 33. PL/pgSQL EXCEPTION block: unhandled exception propagates
-- ============================================================================

CREATE FUNCTION fn_unhandled_exception() RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1/0;
  RETURN 'no error';
EXCEPTION
  WHEN unique_violation THEN
    RETURN 'caught unique_violation';
END;
$$;

-- begin-expected-error
-- message-like: division by zero
-- end-expected-error
SELECT fn_unhandled_exception();

-- ============================================================================
-- 34. PL/pgSQL GET STACKED DIAGNOSTICS
-- ============================================================================

CREATE FUNCTION fn_get_diagnostics() RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v_msg text;
  v_state text;
BEGIN
  PERFORM 1/0;
  RETURN 'no error';
EXCEPTION
  WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
      v_msg = MESSAGE_TEXT,
      v_state = RETURNED_SQLSTATE;
    RETURN v_state || ':' || v_msg;
END;
$$;

-- begin-expected
-- columns: fn_get_diagnostics
-- row: 22012:division by zero
-- end-expected
SELECT fn_get_diagnostics();

-- ============================================================================
-- 35. PL/pgSQL EXECUTE (dynamic SQL)
-- ============================================================================

CREATE FUNCTION fn_dynamic_query(tbl text) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
  cnt integer;
BEGIN
  EXECUTE 'SELECT count(*)::integer FROM ' || quote_ident(tbl) INTO cnt;
  RETURN cnt;
END;
$$;

-- begin-expected
-- columns: fn_dynamic_query
-- row: 3
-- end-expected
SELECT fn_dynamic_query('fn_data');

-- ============================================================================
-- 36. PL/pgSQL EXECUTE with USING
-- ============================================================================

CREATE FUNCTION fn_dynamic_using(min_id integer) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
  cnt integer;
BEGIN
  EXECUTE 'SELECT count(*)::integer FROM fn_data WHERE id >= $1' INTO cnt USING min_id;
  RETURN cnt;
END;
$$;

-- begin-expected
-- columns: fn_dynamic_using
-- row: 2
-- end-expected
SELECT fn_dynamic_using(2);

-- ============================================================================
-- 37. Nested function calls
-- ============================================================================

CREATE FUNCTION fn_inner(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x * 10 $$;
CREATE FUNCTION fn_outer(x integer) RETURNS integer LANGUAGE plpgsql AS $$
BEGIN
  RETURN fn_inner(x) + 1;
END;
$$;

-- begin-expected
-- columns: result
-- row: 51
-- end-expected
SELECT fn_outer(5) AS result;

-- ============================================================================
-- 38. Recursive function
-- ============================================================================

CREATE FUNCTION fn_factorial(n integer) RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN
  IF n <= 1 THEN
    RETURN 1;
  ELSE
    RETURN n * fn_factorial(n - 1);
  END IF;
END;
$$;

-- begin-expected
-- columns: result
-- row: 120
-- end-expected
SELECT fn_factorial(5) AS result;

-- ============================================================================
-- 39. DROP FUNCTION with argument types
-- ============================================================================

CREATE FUNCTION fn_to_drop(integer) RETURNS integer LANGUAGE sql AS $$ SELECT $1 $$;
CREATE FUNCTION fn_to_drop(text) RETURNS text LANGUAGE sql AS $$ SELECT $1 $$;

-- Drop only the integer variant
DROP FUNCTION fn_to_drop(integer);

-- Text variant still works
-- begin-expected
-- columns: fn_to_drop
-- row: hello
-- end-expected
SELECT fn_to_drop('hello');

-- begin-expected-error
-- message-like: function fn_to_drop(integer) does not exist
-- end-expected-error
SELECT fn_to_drop(1);

DROP FUNCTION fn_to_drop(text);

-- ============================================================================
-- 40. DROP FUNCTION IF EXISTS
-- ============================================================================

-- command: DROP FUNCTION
DROP FUNCTION IF EXISTS fn_nonexistent_function();

-- ============================================================================
-- 41. Dollar-quoting variants
-- ============================================================================

CREATE FUNCTION fn_dollar_body() RETURNS text LANGUAGE plpgsql AS $body$
BEGIN
  RETURN 'used $body$ delimiters';
END;
$body$;

-- begin-expected
-- columns: fn_dollar_body
-- row: used $body$ delimiters
-- end-expected
SELECT fn_dollar_body();

-- Nested dollar-quotes
CREATE FUNCTION fn_nested_dollar() RETURNS text LANGUAGE plpgsql AS $outer$
DECLARE
  v text;
BEGIN
  v := $inner$it's a test with 'quotes'$inner$;
  RETURN v;
END;
$outer$;

-- begin-expected
-- columns: fn_nested_dollar
-- row: it's a test with 'quotes'
-- end-expected
SELECT fn_nested_dollar();

-- ============================================================================
-- 42. Function in different contexts: WHERE, ORDER BY, JOIN
-- ============================================================================

CREATE FUNCTION fn_double(x numeric) RETURNS numeric LANGUAGE sql AS $$ SELECT x * 2 $$;

-- In WHERE
-- begin-expected
-- columns: name
-- row: gamma
-- end-expected
SELECT name FROM fn_data WHERE fn_double(val) > 40 ORDER BY id;

-- In ORDER BY
-- begin-expected
-- columns: name
-- row: gamma
-- row: beta
-- row: alpha
-- end-expected
SELECT name FROM fn_data ORDER BY fn_double(val) DESC;

-- In SELECT list with subquery
-- begin-expected
-- columns: name, doubled
-- row: alpha, 21.00
-- row: beta, 40.00
-- row: gamma, 61.50
-- end-expected
SELECT name, fn_double(val) AS doubled FROM fn_data ORDER BY id;

-- ============================================================================
-- 43. Function as default column value
-- ============================================================================

CREATE FUNCTION fn_default_val() RETURNS text LANGUAGE sql AS $$ SELECT 'generated'::text $$;

CREATE TABLE fn_default_test (
  id serial PRIMARY KEY,
  label text DEFAULT fn_default_val()
);

INSERT INTO fn_default_test (id) VALUES (1);

-- begin-expected
-- columns: id, label
-- row: 1, generated
-- end-expected
SELECT * FROM fn_default_test;

DROP TABLE fn_default_test CASCADE;

-- ============================================================================
-- 44. Duplicate function error
-- ============================================================================

CREATE FUNCTION fn_dup_test(integer) RETURNS integer LANGUAGE sql AS $$ SELECT $1 $$;

-- begin-expected-error
-- message-like: function "fn_dup_test" already exists with same argument types
-- end-expected-error
CREATE FUNCTION fn_dup_test(integer) RETURNS integer LANGUAGE sql AS $$ SELECT $1 + 1 $$;

-- ============================================================================
-- 45. ALTER FUNCTION: rename
-- ============================================================================

CREATE FUNCTION fn_old_name() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
ALTER FUNCTION fn_old_name() RENAME TO fn_new_name;

-- begin-expected
-- columns: fn_new_name
-- row: 1
-- end-expected
SELECT fn_new_name();

-- begin-expected-error
-- message-like: function fn_old_name() does not exist
-- end-expected-error
SELECT fn_old_name();

-- ============================================================================
-- 46. ALTER FUNCTION: change volatility
-- ============================================================================

CREATE FUNCTION fn_change_vol(x integer) RETURNS integer LANGUAGE sql VOLATILE AS $$ SELECT x $$;
ALTER FUNCTION fn_change_vol(integer) IMMUTABLE;

-- Should now be usable in expression index
CREATE TABLE fn_alter_vol_test (id integer);
CREATE INDEX idx_alter_vol ON fn_alter_vol_test (fn_change_vol(id));
DROP TABLE fn_alter_vol_test CASCADE;

-- ============================================================================
-- 47. Function returning NULL explicitly
-- ============================================================================

CREATE FUNCTION fn_returns_null() RETURNS integer LANGUAGE plpgsql AS $$
BEGIN
  RETURN NULL;
END;
$$;

-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT fn_returns_null() IS NULL AS is_null;

-- ============================================================================
-- 48. Function with no arguments
-- ============================================================================

CREATE FUNCTION fn_no_args() RETURNS text LANGUAGE sql AS $$ SELECT 'no args'::text $$;

-- begin-expected
-- columns: fn_no_args
-- row: no args
-- end-expected
SELECT fn_no_args();

-- ============================================================================
-- 49. PL/pgSQL assignment from query that returns no rows
-- ============================================================================

CREATE FUNCTION fn_no_row() RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v text;
BEGIN
  SELECT name INTO v FROM fn_data WHERE id = 999;
  IF v IS NULL THEN
    RETURN 'no row found';
  ELSE
    RETURN v;
  END IF;
END;
$$;

-- begin-expected
-- columns: fn_no_row
-- row: no row found
-- end-expected
SELECT fn_no_row();

-- ============================================================================
-- 50. PL/pgSQL PERFORM (discard result)
-- ============================================================================

CREATE FUNCTION fn_perform_test() RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1 + 1;
  RETURN 'done';
END;
$$;

-- begin-expected
-- columns: fn_perform_test
-- row: done
-- end-expected
SELECT fn_perform_test();

-- ============================================================================
-- 51. PL/pgSQL CASE expression
-- ============================================================================

CREATE FUNCTION fn_case_test(x integer) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  CASE x
    WHEN 1 THEN RETURN 'one';
    WHEN 2 THEN RETURN 'two';
    ELSE RETURN 'other';
  END CASE;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3
-- row: one, two, other
-- end-expected
SELECT fn_case_test(1) AS r1, fn_case_test(2) AS r2, fn_case_test(99) AS r3;

-- ============================================================================
-- 52. PL/pgSQL searched CASE
-- ============================================================================

CREATE FUNCTION fn_searched_case(x integer) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  CASE
    WHEN x < 0 THEN RETURN 'negative';
    WHEN x = 0 THEN RETURN 'zero';
    WHEN x > 0 THEN RETURN 'positive';
  END CASE;
END;
$$;

-- begin-expected
-- columns: r1, r2, r3
-- row: negative, zero, positive
-- end-expected
SELECT fn_searched_case(-1) AS r1, fn_searched_case(0) AS r2, fn_searched_case(1) AS r3;

-- ============================================================================
-- 53. Function with table DML (INSERT/UPDATE/DELETE)
-- ============================================================================

CREATE TABLE fn_dml_test (id integer PRIMARY KEY, val text);

CREATE FUNCTION fn_do_dml() RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO fn_dml_test VALUES (1, 'a'), (2, 'b');
  UPDATE fn_dml_test SET val = 'B' WHERE id = 2;
  DELETE FROM fn_dml_test WHERE id = 1;
  RETURN (SELECT val FROM fn_dml_test WHERE id = 2);
END;
$$;

-- begin-expected
-- columns: fn_do_dml
-- row: B
-- end-expected
SELECT fn_do_dml();

-- Verify the DML persisted
-- begin-expected
-- columns: id, val
-- row: 2, B
-- end-expected
SELECT * FROM fn_dml_test ORDER BY id;

DROP TABLE fn_dml_test CASCADE;

-- ============================================================================
-- 54. Calling function in CTE
-- ============================================================================

-- begin-expected
-- columns: doubled
-- row: 21.00
-- row: 40.00
-- row: 61.50
-- end-expected
WITH doubled AS (
  SELECT fn_double(val) AS doubled FROM fn_data ORDER BY id
)
SELECT * FROM doubled;

-- ============================================================================
-- 55. Function in HAVING clause
-- ============================================================================

-- begin-expected
-- columns: name, dval
-- row: gamma, 61.50
-- end-expected
SELECT name, fn_double(val) AS dval
FROM fn_data
GROUP BY id, name, val
HAVING fn_double(val) > 50
ORDER BY id;

-- ============================================================================
-- 56. PL/pgSQL: multiple RETURN QUERY calls accumulate
-- ============================================================================

CREATE FUNCTION fn_multi_return_query() RETURNS SETOF integer LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY SELECT 1;
  RETURN QUERY SELECT 2;
  RETURN QUERY SELECT 3;
  RETURN;
END;
$$;

-- begin-expected
-- columns: fn_multi_return_query
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT * FROM fn_multi_return_query();

-- ============================================================================
-- 57. PL/pgSQL: FOUND variable
-- ============================================================================

CREATE FUNCTION fn_found_test(search_id integer) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v text;
BEGIN
  SELECT name INTO v FROM fn_data WHERE id = search_id;
  IF FOUND THEN
    RETURN 'found:' || v;
  ELSE
    RETURN 'not found';
  END IF;
END;
$$;

-- begin-expected
-- columns: r1, r2
-- row: found:alpha, not found
-- end-expected
SELECT fn_found_test(1) AS r1, fn_found_test(999) AS r2;

-- ============================================================================
-- 58. PL/pgSQL: nested blocks
-- ============================================================================

CREATE FUNCTION fn_nested_blocks() RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  outer_var text := 'outer';
BEGIN
  DECLARE
    inner_var text := 'inner';
  BEGIN
    RETURN outer_var || '+' || inner_var;
  END;
END;
$$;

-- begin-expected
-- columns: fn_nested_blocks
-- row: outer+inner
-- end-expected
SELECT fn_nested_blocks();

-- ============================================================================
-- 59. Function with boolean parameters
-- ============================================================================

CREATE FUNCTION fn_bool_test(a boolean, b boolean) RETURNS text LANGUAGE sql AS $$
  SELECT CASE WHEN a AND b THEN 'both' WHEN a THEN 'a only' WHEN b THEN 'b only' ELSE 'neither' END
$$;

-- begin-expected
-- columns: r1, r2, r3
-- row: both, a only, neither
-- end-expected
SELECT fn_bool_test(true, true) AS r1, fn_bool_test(true, false) AS r2, fn_bool_test(false, false) AS r3;

-- ============================================================================
-- 60. pg_proc catalog: check function exists
-- ============================================================================

-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_proc WHERE proname = 'fn_add_plpgsql'
) AS exists;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA fn_test CASCADE;
SET search_path = public;
