-- ============================================================================
-- Feature Comparison: STRICT / RETURNS NULL ON NULL INPUT & SFUNC STRICT
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS strict_test CASCADE;
CREATE SCHEMA strict_test;
SET search_path = strict_test, public;

CREATE TABLE strict_data (id integer PRIMARY KEY, val integer, label text);
INSERT INTO strict_data VALUES
  (1, 10, 'a'), (2, NULL, 'b'), (3, 30, 'c'), (4, NULL, NULL), (5, 50, 'e');

-- ============================================================================
-- 1. Basic STRICT function: NULL input -> NULL output
-- ============================================================================

CREATE FUNCTION strict_double(x integer) RETURNS integer
LANGUAGE sql STRICT AS $$ SELECT x * 2 $$;

-- begin-expected
-- columns: result
-- row: 20
-- end-expected
SELECT strict_double(10) AS result;

-- NULL input returns NULL without calling body
-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT strict_double(NULL) IS NULL AS is_null;

-- ============================================================================
-- 2. RETURNS NULL ON NULL INPUT (synonym for STRICT)
-- ============================================================================

CREATE FUNCTION rnonni_double(x integer) RETURNS integer
LANGUAGE sql RETURNS NULL ON NULL INPUT AS $$ SELECT x * 2 $$;

-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT rnonni_double(NULL) IS NULL AS is_null;

-- begin-expected
-- columns: result
-- row: 14
-- end-expected
SELECT rnonni_double(7) AS result;

-- ============================================================================
-- 3. CALLED ON NULL INPUT (default): NULL passed to body
-- ============================================================================

CREATE FUNCTION non_strict_fn(x integer) RETURNS text
LANGUAGE sql CALLED ON NULL INPUT AS $$
  SELECT CASE WHEN x IS NULL THEN 'was null' ELSE 'was ' || x::text END
$$;

-- begin-expected
-- columns: result
-- row: was null
-- end-expected
SELECT non_strict_fn(NULL) AS result;

-- begin-expected
-- columns: result
-- row: was 5
-- end-expected
SELECT non_strict_fn(5) AS result;

-- ============================================================================
-- 4. STRICT with multiple parameters: any NULL -> NULL
-- ============================================================================

CREATE FUNCTION strict_add(a integer, b integer) RETURNS integer
LANGUAGE sql STRICT AS $$ SELECT a + b $$;

-- begin-expected
-- columns: r1, r2, r3, r4
-- row: 3, true, true, true
-- end-expected
SELECT
  strict_add(1, 2) AS r1,
  strict_add(NULL, 2) IS NULL AS r2,
  strict_add(1, NULL) IS NULL AS r3,
  strict_add(NULL, NULL) IS NULL AS r4;

-- ============================================================================
-- 5. STRICT with three parameters
-- ============================================================================

CREATE FUNCTION strict_concat3(a text, b text, c text) RETURNS text
LANGUAGE sql STRICT AS $$ SELECT a || b || c $$;

-- begin-expected
-- columns: r1, r2
-- row: abc, true
-- end-expected
SELECT strict_concat3('a', 'b', 'c') AS r1, strict_concat3('a', NULL, 'c') IS NULL AS r2;

-- ============================================================================
-- 6. STRICT PL/pgSQL function
-- ============================================================================

CREATE FUNCTION strict_plpgsql(x integer) RETURNS text
LANGUAGE plpgsql STRICT AS $$
BEGIN
  RETURN 'value:' || x::text;
END;
$$;

-- begin-expected
-- columns: r1, r2
-- row: value:42, true
-- end-expected
SELECT strict_plpgsql(42) AS r1, strict_plpgsql(NULL) IS NULL AS r2;

-- ============================================================================
-- 7. STRICT function in SELECT list with nullable column
-- ============================================================================

-- begin-expected
-- columns: id, doubled
-- row: 1, 20
-- row: 2,
-- row: 3, 60
-- row: 4,
-- row: 5, 100
-- end-expected
SELECT id, strict_double(val) AS doubled FROM strict_data ORDER BY id;

-- ============================================================================
-- 8. STRICT function in WHERE clause
-- ============================================================================

-- note: strict_double(NULL) IS NULL, so NULL > 25 is false -> rows with NULL val excluded
-- begin-expected
-- columns: id
-- row: 3
-- row: 5
-- end-expected
SELECT id FROM strict_data WHERE strict_double(val) > 25 ORDER BY id;

-- ============================================================================
-- 9. STRICT function in JOIN ON condition
-- ============================================================================

CREATE TABLE strict_lookup (doubled_val integer, meaning text);
INSERT INTO strict_lookup VALUES (20, 'twenty'), (60, 'sixty');

-- begin-expected
-- columns: id, meaning
-- row: 1, twenty
-- row: 3, sixty
-- end-expected
SELECT d.id, l.meaning
FROM strict_data d
JOIN strict_lookup l ON strict_double(d.val) = l.doubled_val
ORDER BY d.id;

DROP TABLE strict_lookup;

-- ============================================================================
-- 10. STRICT function with COALESCE
-- ============================================================================

-- begin-expected
-- columns: id, result
-- row: 1, 20
-- row: 2, -1
-- row: 3, 60
-- row: 4, -1
-- row: 5, 100
-- end-expected
SELECT id, COALESCE(strict_double(val), -1) AS result FROM strict_data ORDER BY id;

-- ============================================================================
-- 11. STRICT function in CASE
-- ============================================================================

-- begin-expected
-- columns: id, result
-- row: 1, 20
-- row: 2, no val
-- row: 3, 60
-- end-expected
SELECT id,
  CASE WHEN val IS NOT NULL THEN strict_double(val)::text ELSE 'no val' END AS result
FROM strict_data
WHERE id <= 3
ORDER BY id;

-- ============================================================================
-- 12. STRICT aggregate SFUNC: NULLs skipped
-- ============================================================================

CREATE FUNCTION strict_sfunc_add(state integer, val integer) RETURNS integer
LANGUAGE sql STRICT IMMUTABLE AS $$ SELECT state + val $$;

CREATE AGGREGATE strict_sum(integer) (
  SFUNC = strict_sfunc_add,
  STYPE = integer,
  INITCOND = '0'
);

-- NULLs in val column are skipped by STRICT SFUNC
-- begin-expected
-- columns: total
-- row: 90
-- end-expected
SELECT strict_sum(val) AS total FROM strict_data;

-- ============================================================================
-- 13. STRICT SFUNC + no INITCOND: first non-NULL becomes state
-- ============================================================================

CREATE AGGREGATE strict_sum_no_init(integer) (
  SFUNC = strict_sfunc_add,
  STYPE = integer
);

-- begin-expected
-- columns: total
-- row: 90
-- end-expected
SELECT strict_sum_no_init(val) AS total FROM strict_data;

-- ============================================================================
-- 14. STRICT SFUNC + INITCOND + all NULLs -> returns INITCOND
-- ============================================================================

-- begin-expected
-- columns: total
-- row: 0
-- end-expected
SELECT strict_sum(val) AS total
FROM (VALUES (NULL::integer), (NULL::integer), (NULL::integer)) AS t(val);

-- ============================================================================
-- 15. STRICT SFUNC + no INITCOND + all NULLs -> returns NULL
-- ============================================================================

-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT strict_sum_no_init(val) IS NULL AS is_null
FROM (VALUES (NULL::integer), (NULL::integer)) AS t(val);

-- ============================================================================
-- 16. STRICT SFUNC with mixed NULLs and values
-- ============================================================================

-- begin-expected
-- columns: total
-- row: 6
-- end-expected
SELECT strict_sum(val) AS total
FROM (VALUES (1), (NULL::integer), (2), (NULL::integer), (3)) AS t(val);

-- ============================================================================
-- 17. Non-STRICT SFUNC receives NULLs
-- ============================================================================

CREATE FUNCTION non_strict_sfunc(state text, val integer) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT state || CASE WHEN val IS NULL THEN 'N' ELSE val::text END
$$;

CREATE AGGREGATE agg_track_nulls(integer) (
  SFUNC = non_strict_sfunc,
  STYPE = text,
  INITCOND = ''
);

-- begin-expected
-- columns: result
-- row: 10N30N50
-- end-expected
SELECT agg_track_nulls(val) AS result FROM strict_data ORDER BY id;

-- ============================================================================
-- 18. STRICT aggregate per group with NULLs
-- ============================================================================

CREATE TABLE strict_grouped (grp text, val integer);
INSERT INTO strict_grouped VALUES
  ('a', 1), ('a', NULL), ('a', 3),
  ('b', NULL), ('b', NULL),
  ('c', 10);

-- begin-expected
-- columns: grp, total
-- row: a, 4
-- row: b,
-- row: c, 10
-- end-expected
SELECT grp, strict_sum_no_init(val) AS total
FROM strict_grouped
GROUP BY grp
ORDER BY grp;

DROP TABLE strict_grouped;

-- ============================================================================
-- 19. STRICT function: RETURNS SETOF with NULL inputs
-- ============================================================================

CREATE FUNCTION strict_generate(n integer) RETURNS SETOF integer
LANGUAGE plpgsql STRICT AS $$
BEGIN
  FOR i IN 1..n LOOP
    RETURN NEXT i;
  END LOOP;
  RETURN;
END;
$$;

-- begin-expected
-- columns: strict_generate
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT * FROM strict_generate(3);

-- NULL input -> returns no rows (empty set)
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*) AS cnt FROM strict_generate(NULL);

-- ============================================================================
-- 20. STRICT function returning composite type
-- ============================================================================

CREATE TYPE strict_pair AS (a integer, b integer);

CREATE FUNCTION strict_make_pair(x integer, y integer) RETURNS strict_pair
LANGUAGE plpgsql STRICT AS $$
DECLARE
  result strict_pair;
BEGIN
  result.a := x;
  result.b := y;
  RETURN result;
END;
$$;

-- begin-expected
-- columns: a, b
-- row: 1, 2
-- end-expected
SELECT (strict_make_pair(1, 2)).*;

-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT strict_make_pair(NULL, 2) IS NULL AS is_null;

-- ============================================================================
-- 21. ALTER FUNCTION: CALLED ON NULL INPUT <-> STRICT
-- ============================================================================

CREATE FUNCTION strict_toggle(x integer) RETURNS integer
LANGUAGE sql STRICT AS $$ SELECT x $$;

-- Verify it's STRICT
-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT strict_toggle(NULL) IS NULL AS is_null;

-- Change to CALLED ON NULL INPUT
ALTER FUNCTION strict_toggle(integer) CALLED ON NULL INPUT;

-- Now NULL is passed through
-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT strict_toggle(NULL) IS NULL AS is_null;

-- note: Still NULL because body returns NULL when input is NULL (SELECT NULL),
--       but the function IS now called (body executes).

-- Change back to STRICT
ALTER FUNCTION strict_toggle(integer) STRICT;

-- ============================================================================
-- 22. pg_proc: proisstrict column
-- ============================================================================

-- begin-expected
-- columns: strict_fn, non_strict_fn
-- row: true, false
-- end-expected
SELECT
  (SELECT proisstrict FROM pg_proc WHERE proname = 'strict_double' LIMIT 1) AS strict_fn,
  (SELECT proisstrict FROM pg_proc WHERE proname = 'non_strict_fn' LIMIT 1) AS non_strict_fn;

-- ============================================================================
-- 23. STRICT with boolean result
-- ============================================================================

CREATE FUNCTION strict_is_positive(x integer) RETURNS boolean
LANGUAGE sql STRICT AS $$ SELECT x > 0 $$;

-- begin-expected
-- columns: r1, r2, r3
-- row: true, false, true
-- end-expected
SELECT
  strict_is_positive(5) AS r1,
  strict_is_positive(-1) AS r2,
  strict_is_positive(NULL) IS NULL AS r3;

-- ============================================================================
-- 24. STRICT with text concatenation
-- ============================================================================

CREATE FUNCTION strict_greet(name text) RETURNS text
LANGUAGE sql STRICT AS $$ SELECT 'Hello, ' || name $$;

-- begin-expected
-- columns: r1, r2
-- row: Hello, World, true
-- end-expected
SELECT strict_greet('World') AS r1, strict_greet(NULL) IS NULL AS r2;

-- ============================================================================
-- 25. STRICT SFUNC aggregate + FILTER
-- ============================================================================

-- begin-expected
-- columns: filtered_sum
-- row: 80
-- end-expected
SELECT strict_sum(val) FILTER (WHERE val > 15) AS filtered_sum FROM strict_data;

-- ============================================================================
-- 26. STRICT SFUNC aggregate in window function
-- ============================================================================

-- begin-expected
-- columns: id, val, running
-- row: 1, 10, 10
-- row: 3, 30, 40
-- row: 5, 50, 90
-- end-expected
SELECT id, val, sum(val) OVER (ORDER BY id) AS running
FROM strict_data
WHERE val IS NOT NULL
ORDER BY id;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA strict_test CASCADE;
SET search_path = public;
