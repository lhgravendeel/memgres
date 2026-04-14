-- ============================================================================
-- Feature Comparison: SQL-Standard Function Body (A9)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG 14 introduced SQL-standard function bodies:
--   CREATE FUNCTION f(x int) RETURNS int RETURN x + 1;
--   CREATE FUNCTION f(x int) RETURNS int BEGIN ATOMIC SELECT x + 1; END;
-- These differ from dollar-quoted bodies by using RETURN expr or BEGIN ATOMIC.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS ssf_test CASCADE;
CREATE SCHEMA ssf_test;
SET search_path = ssf_test, public;

CREATE TABLE ssf_data (id integer PRIMARY KEY, val integer, label text);
INSERT INTO ssf_data VALUES (1, 10, 'alpha'), (2, 20, 'beta'), (3, 30, 'gamma');

-- ============================================================================
-- 1. Basic RETURN expr
-- ============================================================================

CREATE FUNCTION ssf_add(a integer, b integer) RETURNS integer
LANGUAGE sql
RETURN a + b;

-- begin-expected
-- columns: result
-- row: 7
-- end-expected
SELECT ssf_add(3, 4) AS result;

-- ============================================================================
-- 2. RETURN with single expression (no SELECT)
-- ============================================================================

CREATE FUNCTION ssf_double(x integer) RETURNS integer
LANGUAGE sql
RETURN x * 2;

-- begin-expected
-- columns: result
-- row: 20
-- end-expected
SELECT ssf_double(10) AS result;

-- ============================================================================
-- 3. BEGIN ATOMIC ... END
-- ============================================================================

CREATE FUNCTION ssf_atomic_add(a integer, b integer) RETURNS integer
LANGUAGE sql
BEGIN ATOMIC
  SELECT a + b;
END;

-- begin-expected
-- columns: ssf_atomic_add
-- row: 12
-- end-expected
SELECT ssf_atomic_add(5, 7);

-- ============================================================================
-- 4. BEGIN ATOMIC with multiple statements (last result returned)
-- ============================================================================

CREATE FUNCTION ssf_atomic_multi(x integer) RETURNS integer
LANGUAGE sql
BEGIN ATOMIC
  SELECT x + 1;
  SELECT x * 10;
END;

-- begin-expected
-- columns: ssf_atomic_multi
-- row: 50
-- end-expected
SELECT ssf_atomic_multi(5);

-- ============================================================================
-- 5. RETURN with type cast
-- ============================================================================

CREATE FUNCTION ssf_to_text(x integer) RETURNS text
LANGUAGE sql
RETURN x::text;

-- begin-expected
-- columns: result
-- row: 42
-- end-expected
SELECT ssf_to_text(42) AS result;

-- ============================================================================
-- 6. RETURN with COALESCE
-- ============================================================================

CREATE FUNCTION ssf_coalesce(x integer) RETURNS integer
LANGUAGE sql
RETURN COALESCE(x, 0);

-- begin-expected
-- columns: r1, r2
-- row: 5, 0
-- end-expected
SELECT ssf_coalesce(5) AS r1, ssf_coalesce(NULL) AS r2;

-- ============================================================================
-- 7. RETURN with CASE
-- ============================================================================

CREATE FUNCTION ssf_classify(x integer) RETURNS text
LANGUAGE sql
RETURN CASE WHEN x > 0 THEN 'positive' WHEN x = 0 THEN 'zero' ELSE 'negative' END;

-- begin-expected
-- columns: r1, r2, r3
-- row: positive, zero, negative
-- end-expected
SELECT ssf_classify(5) AS r1, ssf_classify(0) AS r2, ssf_classify(-3) AS r3;

-- ============================================================================
-- 8. RETURN with subquery
-- ============================================================================

CREATE FUNCTION ssf_max_val() RETURNS integer
LANGUAGE sql
RETURN (SELECT max(val) FROM ssf_data);

-- begin-expected
-- columns: ssf_max_val
-- row: 30
-- end-expected
SELECT ssf_max_val();

-- ============================================================================
-- 9. SQL-standard body with IMMUTABLE
-- ============================================================================

CREATE FUNCTION ssf_immut(x integer) RETURNS integer
LANGUAGE sql IMMUTABLE
RETURN x * x;

-- Usable in expression index
CREATE TABLE ssf_idx_test (id integer, val integer);
CREATE INDEX idx_ssf ON ssf_idx_test (ssf_immut(val));
DROP TABLE ssf_idx_test CASCADE;

-- begin-expected
-- columns: result
-- row: 25
-- end-expected
SELECT ssf_immut(5) AS result;

-- ============================================================================
-- 10. SQL-standard body with STRICT
-- ============================================================================

CREATE FUNCTION ssf_strict(x integer) RETURNS integer
LANGUAGE sql STRICT
RETURN x + 100;

-- begin-expected
-- columns: r1, r2
-- row: 105, true
-- end-expected
SELECT ssf_strict(5) AS r1, ssf_strict(NULL) IS NULL AS r2;

-- ============================================================================
-- 11. SQL-standard body with SECURITY DEFINER
-- ============================================================================

CREATE FUNCTION ssf_secdef() RETURNS text
LANGUAGE sql SECURITY DEFINER
RETURN current_user::text;

-- begin-expected
-- columns: has_value
-- row: true
-- end-expected
SELECT ssf_secdef() IS NOT NULL AS has_value;

-- ============================================================================
-- 12. SQL-standard body with default parameters
-- ============================================================================

CREATE FUNCTION ssf_defaults(a integer, b integer DEFAULT 10) RETURNS integer
LANGUAGE sql
RETURN a + b;

-- begin-expected
-- columns: r1, r2
-- row: 11, 7
-- end-expected
SELECT ssf_defaults(1) AS r1, ssf_defaults(3, 4) AS r2;

-- ============================================================================
-- 13. CREATE OR REPLACE with SQL-standard body
-- ============================================================================

CREATE FUNCTION ssf_replace() RETURNS text LANGUAGE sql RETURN 'v1'::text;

-- begin-expected
-- columns: result
-- row: v1
-- end-expected
SELECT ssf_replace() AS result;

CREATE OR REPLACE FUNCTION ssf_replace() RETURNS text LANGUAGE sql RETURN 'v2'::text;

-- begin-expected
-- columns: result
-- row: v2
-- end-expected
SELECT ssf_replace() AS result;

-- ============================================================================
-- 14. DROP FUNCTION with SQL-standard body
-- ============================================================================

CREATE FUNCTION ssf_drop_me(x integer) RETURNS integer LANGUAGE sql RETURN x;

DROP FUNCTION ssf_drop_me(integer);

-- begin-expected-error
-- message-like: function ssf_drop_me(integer) does not exist
-- end-expected-error
SELECT ssf_drop_me(1);

-- ============================================================================
-- 15. SQL-standard function in various contexts
-- ============================================================================

-- In WHERE
-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM ssf_data WHERE ssf_double(val) > 25 ORDER BY id;

-- In ORDER BY
-- begin-expected
-- columns: id
-- row: 3
-- row: 2
-- row: 1
-- end-expected
SELECT id FROM ssf_data ORDER BY ssf_double(val) DESC;

-- In CTE
-- begin-expected
-- columns: doubled
-- row: 20
-- row: 40
-- row: 60
-- end-expected
WITH d AS (SELECT ssf_double(val) AS doubled FROM ssf_data ORDER BY id)
SELECT * FROM d;

-- ============================================================================
-- 16. RETURN with string concatenation
-- ============================================================================

CREATE FUNCTION ssf_greet(name text) RETURNS text
LANGUAGE sql
RETURN 'Hello, ' || name || '!';

-- begin-expected
-- columns: result
-- row: Hello, World!
-- end-expected
SELECT ssf_greet('World') AS result;

-- ============================================================================
-- 17. RETURN with boolean expression
-- ============================================================================

CREATE FUNCTION ssf_is_even(x integer) RETURNS boolean
LANGUAGE sql
RETURN x % 2 = 0;

-- begin-expected
-- columns: r1, r2
-- row: true, false
-- end-expected
SELECT ssf_is_even(4) AS r1, ssf_is_even(5) AS r2;

-- ============================================================================
-- 18. RETURN with arithmetic chain
-- ============================================================================

CREATE FUNCTION ssf_poly(x integer) RETURNS integer
LANGUAGE sql IMMUTABLE
RETURN x * x + 2 * x + 1;

-- begin-expected
-- columns: result
-- row: 16
-- end-expected
SELECT ssf_poly(3) AS result;

-- ============================================================================
-- 19. pg_proc catalog: SQL-standard function stored correctly
-- ============================================================================

-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_proc WHERE proname = 'ssf_add' AND prolang = (SELECT oid FROM pg_language WHERE lanname = 'sql')
) AS exists;

-- ============================================================================
-- 20. BEGIN ATOMIC with INSERT (DML in function body)
-- ============================================================================

CREATE TABLE ssf_log (id serial PRIMARY KEY, msg text);

CREATE FUNCTION ssf_insert_log(m text) RETURNS integer
LANGUAGE sql
BEGIN ATOMIC
  INSERT INTO ssf_log (msg) VALUES (m) RETURNING id;
END;

-- begin-expected
-- columns: ssf_insert_log
-- row: 1
-- end-expected
SELECT ssf_insert_log('test entry');

-- begin-expected
-- columns: msg
-- row: test entry
-- end-expected
SELECT msg FROM ssf_log WHERE id = 1;

DROP TABLE ssf_log CASCADE;

-- ============================================================================
-- 21. BEGIN ATOMIC with UPDATE
-- ============================================================================

CREATE TABLE ssf_counter (id integer PRIMARY KEY, val integer);
INSERT INTO ssf_counter VALUES (1, 0);

CREATE FUNCTION ssf_increment(target_id integer) RETURNS integer
LANGUAGE sql
BEGIN ATOMIC
  UPDATE ssf_counter SET val = val + 1 WHERE id = target_id RETURNING val;
END;

-- begin-expected
-- columns: ssf_increment
-- row: 1
-- end-expected
SELECT ssf_increment(1);

-- begin-expected
-- columns: ssf_increment
-- row: 2
-- end-expected
SELECT ssf_increment(1);

DROP TABLE ssf_counter CASCADE;

-- ============================================================================
-- 22. RETURNS SETOF with BEGIN ATOMIC
-- ============================================================================

CREATE FUNCTION ssf_series(n integer) RETURNS SETOF integer
LANGUAGE sql
BEGIN ATOMIC
  SELECT generate_series(1, n);
END;

-- begin-expected
-- columns: ssf_series
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT * FROM ssf_series(3);

-- ============================================================================
-- 23. RETURNS TABLE with BEGIN ATOMIC
-- ============================================================================

CREATE TABLE ssf_people (id integer, name text, age integer);
INSERT INTO ssf_people VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 35);

CREATE FUNCTION ssf_adults() RETURNS TABLE(name text, age integer)
LANGUAGE sql
BEGIN ATOMIC
  SELECT name, age FROM ssf_people WHERE age >= 30 ORDER BY name;
END;

-- begin-expected
-- columns: name, age
-- row: Alice, 30
-- row: Carol, 35
-- end-expected
SELECT * FROM ssf_adults();

DROP TABLE ssf_people CASCADE;

-- ============================================================================
-- 24. RETURN NULL literal
-- ============================================================================

CREATE FUNCTION ssf_null() RETURNS text
LANGUAGE sql
RETURN NULL;

-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT ssf_null() IS NULL AS is_null;

-- ============================================================================
-- 25. SQL function calling another SQL function
-- ============================================================================

CREATE FUNCTION ssf_double(x integer) RETURNS integer
LANGUAGE sql IMMUTABLE
RETURN x * 2;

CREATE FUNCTION ssf_quadruple(x integer) RETURNS integer
LANGUAGE sql IMMUTABLE
RETURN ssf_double(ssf_double(x));

-- begin-expected
-- columns: result
-- row: 20
-- end-expected
SELECT ssf_quadruple(5) AS result;

-- ============================================================================
-- 26. RETURN with IN/OUT parameters
-- ============================================================================

CREATE FUNCTION ssf_swap(INOUT a integer, INOUT b integer)
LANGUAGE sql
BEGIN ATOMIC
  SELECT b, a;
END;

-- begin-expected
-- columns: a, b
-- row: 20, 10
-- end-expected
SELECT * FROM ssf_swap(10, 20);

-- ============================================================================
-- 27. BEGIN ATOMIC with multiple statements (last wins)
-- ============================================================================

CREATE TABLE ssf_side (id serial, val text);

CREATE FUNCTION ssf_multi_stmt(x text) RETURNS text
LANGUAGE sql
BEGIN ATOMIC
  INSERT INTO ssf_side (val) VALUES (x);
  SELECT 'done: ' || x;
END;

-- begin-expected
-- columns: ssf_multi_stmt
-- row: done: hello
-- end-expected
SELECT ssf_multi_stmt('hello');

-- Side effect happened
-- begin-expected
-- columns: val
-- row: hello
-- end-expected
SELECT val FROM ssf_side WHERE val = 'hello';

DROP TABLE ssf_side CASCADE;

-- ============================================================================
-- 28. Error: mixing PL/pgSQL body with RETURN keyword
-- ============================================================================

-- note: RETURN expr is SQL-standard syntax; mixing with $$ body is an error
-- begin-expected-error
-- message-like: error
-- end-expected-error
CREATE FUNCTION ssf_mix_fail(x integer) RETURNS integer
LANGUAGE plpgsql
RETURN x + 1;

-- ============================================================================
-- 29. CREATE OR REPLACE preserves SQL-standard body
-- ============================================================================

CREATE FUNCTION ssf_replace_test(x integer) RETURNS integer
LANGUAGE sql
RETURN x + 1;

-- begin-expected
-- columns: result
-- row: 6
-- end-expected
SELECT ssf_replace_test(5) AS result;

CREATE OR REPLACE FUNCTION ssf_replace_test(x integer) RETURNS integer
LANGUAGE sql
RETURN x * 10;

-- begin-expected
-- columns: result
-- row: 50
-- end-expected
SELECT ssf_replace_test(5) AS result;

-- ============================================================================
-- 30. RETURN with window function inside subquery
-- ============================================================================

CREATE FUNCTION ssf_first_rank() RETURNS integer
LANGUAGE sql
RETURN (SELECT val FROM (
  SELECT val, ROW_NUMBER() OVER (ORDER BY val DESC) AS rn
  FROM (VALUES (10),(20),(30)) t(val)
) sub WHERE rn = 1);

-- begin-expected
-- columns: result
-- row: 30
-- end-expected
SELECT ssf_first_rank() AS result;

-- ============================================================================
-- 31. BEGIN ATOMIC with DELETE ... RETURNING
-- ============================================================================

CREATE TABLE ssf_del_test (id integer PRIMARY KEY, val text);
INSERT INTO ssf_del_test VALUES (1, 'remove-me'), (2, 'keep');

CREATE FUNCTION ssf_delete_and_return(target_id integer) RETURNS text
LANGUAGE sql
BEGIN ATOMIC
  DELETE FROM ssf_del_test WHERE id = target_id RETURNING val;
END;

-- begin-expected
-- columns: ssf_delete_and_return
-- row: remove-me
-- end-expected
SELECT ssf_delete_and_return(1);

-- Verify row was deleted
-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::integer AS cnt FROM ssf_del_test;

DROP TABLE ssf_del_test CASCADE;

-- ============================================================================
-- 32. RETURN with aggregate function
-- ============================================================================

CREATE TABLE ssf_agg_data (val integer);
INSERT INTO ssf_agg_data VALUES (10), (20), (30);

CREATE FUNCTION ssf_total() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT sum(val) FROM ssf_agg_data);

-- begin-expected
-- columns: result
-- row: 60
-- end-expected
SELECT ssf_total() AS result;

DROP TABLE ssf_agg_data CASCADE;

-- ============================================================================
-- 33. Dependency tracking: DROP table used by SQL function
-- ============================================================================

CREATE TABLE ssf_dep_table (id integer PRIMARY KEY, val text);

CREATE FUNCTION ssf_dep_func() RETURNS SETOF text
LANGUAGE sql STABLE
BEGIN ATOMIC
  SELECT val FROM ssf_dep_table;
END;

-- CASCADE should drop the dependent function
DROP TABLE ssf_dep_table CASCADE;

-- Function should be gone
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM pg_proc WHERE proname = 'ssf_dep_func';

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA ssf_test CASCADE;
SET search_path = public;
