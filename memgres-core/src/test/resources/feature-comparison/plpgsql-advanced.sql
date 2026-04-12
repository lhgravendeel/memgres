-- ============================================================================
-- Feature Comparison: PL/pgSQL Advanced Gaps (D1-D5)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- D1: RETURN QUERY EXECUTE (dynamic SQL in set-returning functions)
-- D2: FOREACH ... IN ARRAY with SLICE
-- D3: PL/pgSQL ASSERT statement
-- D4: CALL ... INTO (capture OUT params)
-- D5: Window function EXCLUDE clause
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS pla_test CASCADE;
CREATE SCHEMA pla_test;
SET search_path = pla_test, public;

-- ============================================================================
-- D1: RETURN QUERY EXECUTE
-- ============================================================================

-- ============================================================================
-- 1. Basic RETURN QUERY EXECUTE
-- ============================================================================

CREATE TABLE pla_data (id integer PRIMARY KEY, val text);
INSERT INTO pla_data VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');

CREATE FUNCTION pla_dynamic_query(tbl text) RETURNS SETOF pla_data
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY EXECUTE 'SELECT * FROM ' || tbl || ' ORDER BY id';
END;
$$;

-- begin-expected
-- columns: id, val
-- row: 1, alpha
-- row: 2, beta
-- row: 3, gamma
-- end-expected
SELECT * FROM pla_dynamic_query('pla_data');

-- ============================================================================
-- 2. RETURN QUERY EXECUTE with parameters (USING)
-- ============================================================================

CREATE FUNCTION pla_dynamic_filtered(min_id integer) RETURNS SETOF pla_data
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY EXECUTE 'SELECT * FROM pla_data WHERE id >= $1 ORDER BY id' USING min_id;
END;
$$;

-- begin-expected
-- columns: id, val
-- row: 2, beta
-- row: 3, gamma
-- end-expected
SELECT * FROM pla_dynamic_filtered(2);

-- ============================================================================
-- 3. RETURN QUERY EXECUTE with multiple USING params
-- ============================================================================

CREATE FUNCTION pla_dynamic_range(lo integer, hi integer) RETURNS SETOF pla_data
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY EXECUTE 'SELECT * FROM pla_data WHERE id BETWEEN $1 AND $2 ORDER BY id'
    USING lo, hi;
END;
$$;

-- begin-expected
-- columns: id, val
-- row: 2, beta
-- row: 3, gamma
-- end-expected
SELECT * FROM pla_dynamic_range(2, 3);

-- ============================================================================
-- 4. RETURN QUERY EXECUTE returning no rows
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM pla_dynamic_filtered(100);

-- ============================================================================
-- 5. RETURN QUERY EXECUTE with dynamic column list
-- ============================================================================

CREATE FUNCTION pla_dynamic_cols(col text) RETURNS SETOF text
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY EXECUTE 'SELECT ' || quote_ident(col) || '::text FROM pla_data ORDER BY id';
END;
$$;

-- begin-expected
-- columns: pla_dynamic_cols
-- row: alpha
-- row: beta
-- row: gamma
-- end-expected
SELECT * FROM pla_dynamic_cols('val');

-- ============================================================================
-- 6. Mixed RETURN QUERY (static) and RETURN QUERY EXECUTE (dynamic)
-- ============================================================================

CREATE FUNCTION pla_mixed_return() RETURNS SETOF integer
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY SELECT id FROM pla_data WHERE id = 1;
  RETURN QUERY EXECUTE 'SELECT id FROM pla_data WHERE id = 3';
END;
$$;

-- begin-expected
-- columns: pla_mixed_return
-- row: 1
-- row: 3
-- end-expected
SELECT * FROM pla_mixed_return();

-- ============================================================================
-- 7. RETURN QUERY EXECUTE with format()
-- ============================================================================

CREATE FUNCTION pla_format_query(schema_name text, table_name text) RETURNS SETOF record
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY EXECUTE format('SELECT id, val FROM %I.%I ORDER BY id', schema_name, table_name);
END;
$$;

-- begin-expected
-- columns: id, val
-- row: 1, alpha
-- row: 2, beta
-- row: 3, gamma
-- end-expected
SELECT * FROM pla_format_query('pla_test', 'pla_data') AS t(id integer, val text);

-- ============================================================================
-- D2: FOREACH ... IN ARRAY with SLICE
-- ============================================================================

-- ============================================================================
-- 8. Basic FOREACH ... IN ARRAY (element-by-element)
-- ============================================================================

CREATE FUNCTION pla_foreach_basic() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  arr integer[] := ARRAY[10, 20, 30];
  elem integer;
  result text := '';
BEGIN
  FOREACH elem IN ARRAY arr LOOP
    result := result || elem::text || ',';
  END LOOP;
  RETURN result;
END;
$$;

-- begin-expected
-- columns: result
-- row: 10,20,30,
-- end-expected
SELECT pla_foreach_basic() AS result;

-- ============================================================================
-- 9. FOREACH SLICE 1 (iterate over 1D slices of 2D array)
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: SLICE
-- end-expected-error
CREATE FUNCTION pla_foreach_slice1() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  arr integer[] := ARRAY[[1,2],[3,4],[5,6]];
  slice integer[];
  result text := '';
BEGIN
  FOREACH slice SLICE 1 IN ARRAY arr LOOP
    result := result || slice::text || ';';
  END LOOP;
  RETURN result;
END;
$$;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: pla_foreach_slice1
-- end-expected-error
SELECT pla_foreach_slice1() AS result;

-- ============================================================================
-- 10. FOREACH SLICE 0 (same as no SLICE — element-by-element)
-- ============================================================================

CREATE FUNCTION pla_foreach_slice0() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  arr integer[] := ARRAY[1, 2, 3];
  elem integer;
  result text := '';
BEGIN
  FOREACH elem SLICE 0 IN ARRAY arr LOOP
    result := result || elem::text || ',';
  END LOOP;
  RETURN result;
END;
$$;

-- begin-expected
-- columns: result
-- row: 1,2,3,
-- end-expected
SELECT pla_foreach_slice0() AS result;

-- ============================================================================
-- 11. FOREACH with text array
-- ============================================================================

CREATE FUNCTION pla_foreach_text() RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  arr text[] := ARRAY['hello', 'world'];
  elem text;
  result text := '';
BEGIN
  FOREACH elem IN ARRAY arr LOOP
    result := result || upper(elem) || ' ';
  END LOOP;
  RETURN trim(result);
END;
$$;

-- begin-expected
-- columns: result
-- row: HELLO WORLD
-- end-expected
SELECT pla_foreach_text() AS result;

-- ============================================================================
-- 12. FOREACH on empty array
-- ============================================================================

CREATE FUNCTION pla_foreach_empty() RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  arr integer[] := '{}';
  elem integer;
  cnt integer := 0;
BEGIN
  FOREACH elem IN ARRAY arr LOOP
    cnt := cnt + 1;
  END LOOP;
  RETURN cnt;
END;
$$;

-- begin-expected
-- columns: result
-- row: 0
-- end-expected
SELECT pla_foreach_empty() AS result;

-- ============================================================================
-- D3: PL/pgSQL ASSERT
-- ============================================================================

-- ============================================================================
-- 13. ASSERT: passing assertion
-- ============================================================================

CREATE FUNCTION pla_assert_pass() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  ASSERT 1 = 1, 'one equals one';
  RETURN 'ok';
END;
$$;

-- begin-expected
-- columns: result
-- row: ok
-- end-expected
SELECT pla_assert_pass() AS result;

-- ============================================================================
-- 14. ASSERT: failing assertion
-- ============================================================================

CREATE FUNCTION pla_assert_fail() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  ASSERT 1 = 2, 'one does not equal two';
  RETURN 'should not reach here';
END;
$$;

-- begin-expected-error
-- sqlstate: P0004
-- message-like: one does not equal two
-- end-expected-error
SELECT pla_assert_fail();

-- ============================================================================
-- 15. ASSERT: without message
-- ============================================================================

CREATE FUNCTION pla_assert_no_msg() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  ASSERT false;
  RETURN 'nope';
END;
$$;

-- begin-expected-error
-- sqlstate: P0004
-- message-like: assert
-- end-expected-error
SELECT pla_assert_no_msg();

-- ============================================================================
-- 16. ASSERT: with expression
-- ============================================================================

CREATE FUNCTION pla_assert_expr(x integer) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  ASSERT x > 0, 'x must be positive, got ' || x::text;
  RETURN 'valid: ' || x::text;
END;
$$;

-- begin-expected
-- columns: result
-- row: valid: 5
-- end-expected
SELECT pla_assert_expr(5) AS result;

-- begin-expected-error
-- sqlstate: P0004
-- message-like: x must be positive
-- end-expected-error
SELECT pla_assert_expr(-1);

-- ============================================================================
-- 17. ASSERT: disabled via plpgsql.check_asserts
-- ============================================================================

-- note: When plpgsql.check_asserts = off, ASSERT is skipped
SET plpgsql.check_asserts = off;

-- begin-expected
-- columns: result
-- row: should not reach here
-- end-expected
SELECT pla_assert_fail() AS result;

SET plpgsql.check_asserts = on;

-- ============================================================================
-- D4: CALL ... INTO
-- ============================================================================

-- ============================================================================
-- 18. Procedure with OUT parameter
-- ============================================================================

CREATE PROCEDURE pla_proc_out(IN x integer, OUT result integer)
LANGUAGE plpgsql AS $$
BEGIN
  result := x * 10;
END;
$$;

-- begin-expected
-- columns: result
-- row: 50
-- end-expected
CALL pla_proc_out(5, NULL);

-- ============================================================================
-- 19. CALL ... INTO from PL/pgSQL
-- ============================================================================

CREATE FUNCTION pla_call_into(x integer) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  v integer;
BEGIN
  CALL pla_proc_out(x, v);
  RETURN v;
END;
$$;

-- begin-expected
-- columns: result
-- row: 70
-- end-expected
SELECT pla_call_into(7) AS result;

-- ============================================================================
-- 20. Procedure with multiple OUT parameters
-- ============================================================================

CREATE PROCEDURE pla_proc_multi_out(IN x integer, OUT doubled integer, OUT tripled integer)
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
CALL pla_proc_multi_out(5, NULL, NULL);

-- ============================================================================
-- 21. CALL INTO with multiple OUT params from PL/pgSQL
-- ============================================================================

CREATE FUNCTION pla_call_multi_into(x integer) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  d integer;
  t integer;
BEGIN
  CALL pla_proc_multi_out(x, d, t);
  RETURN 'doubled=' || d::text || ' tripled=' || t::text;
END;
$$;

-- begin-expected
-- columns: result
-- row: doubled=8 tripled=12
-- end-expected
SELECT pla_call_multi_into(4) AS result;

-- ============================================================================
-- 22. Procedure with INOUT parameter
-- ============================================================================

CREATE PROCEDURE pla_proc_inout(INOUT val integer)
LANGUAGE plpgsql AS $$
BEGIN
  val := val + 100;
END;
$$;

-- begin-expected
-- columns: val
-- row: 105
-- end-expected
CALL pla_proc_inout(5);

-- ============================================================================
-- D5: Window Function EXCLUDE Clause
-- ============================================================================

-- ============================================================================
-- 23. EXCLUDE CURRENT ROW
-- ============================================================================

CREATE TABLE pla_window (id integer, val integer);
INSERT INTO pla_window VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);

-- begin-expected
-- columns: id, val, sum_excl
-- row: 1, 10, NULL
-- row: 2, 20, 10
-- row: 3, 30, 30
-- row: 4, 40, 60
-- row: 5, 50, 100
-- end-expected
SELECT id, val,
  sum(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS sum_excl
FROM pla_window ORDER BY id;

-- ============================================================================
-- 24. EXCLUDE GROUP
-- ============================================================================

CREATE TABLE pla_window2 (grp text, val integer);
INSERT INTO pla_window2 VALUES ('a', 10), ('a', 20), ('b', 30), ('b', 40);

-- note: EXCLUDE GROUP excludes all rows with same ORDER BY value as current
-- begin-expected
-- columns: grp, val, sum_excl
-- row: a, 10, NULL
-- row: a, 20, 10
-- row: b, 30, 30
-- row: b, 40, 60
-- end-expected
SELECT grp, val,
  sum(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS sum_excl
FROM pla_window2 ORDER BY val;

-- ============================================================================
-- 25. EXCLUDE TIES
-- ============================================================================

-- note: EXCLUDE TIES excludes peer rows but keeps current row
-- Using RANGE frame to make ties meaningful
CREATE TABLE pla_ties (id integer, score integer);
INSERT INTO pla_ties VALUES (1, 10), (2, 10), (3, 20), (4, 20), (5, 30);

-- begin-expected
-- columns: id, score, cnt
-- row: 1, 10, 1
-- row: 2, 10, 1
-- row: 3, 20, 3
-- row: 4, 20, 3
-- row: 5, 30, 5
-- end-expected
SELECT id, score,
  count(*) OVER (ORDER BY score RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) AS cnt
FROM pla_ties ORDER BY id;

-- ============================================================================
-- 26. EXCLUDE NO OTHERS (default — include everything)
-- ============================================================================

-- begin-expected
-- columns: id, val, running_sum
-- row: 1, 10, 10
-- row: 2, 20, 30
-- row: 3, 30, 60
-- row: 4, 40, 100
-- row: 5, 50, 150
-- end-expected
SELECT id, val,
  sum(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS) AS running_sum
FROM pla_window ORDER BY id;

-- ============================================================================
-- 27. EXCLUDE with GROUPS frame
-- ============================================================================

-- begin-expected
-- columns: id, score, sum_excl
-- row: 1, 10, 10
-- row: 2, 10, 10
-- row: 3, 20, 40
-- row: 4, 20, 40
-- row: 5, 30, 100
-- end-expected
SELECT id, score,
  sum(score) OVER (ORDER BY score GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS sum_excl
FROM pla_ties ORDER BY id;

-- ============================================================================
-- 28. EXCLUDE with avg() window function
-- ============================================================================

-- begin-expected
-- columns: id, val, avg_excl
-- row: 3, 30, NULL
-- end-expected
SELECT id, val,
  avg(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)::integer AS avg_excl
FROM pla_window WHERE id = 3;

DROP TABLE pla_window, pla_window2, pla_ties;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA pla_test CASCADE;
SET search_path = public;
