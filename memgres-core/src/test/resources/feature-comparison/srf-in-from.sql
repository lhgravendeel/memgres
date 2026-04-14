-- ============================================================================
-- Feature Comparison: Set-Returning Functions in FROM Clause
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests using SRFs (set-returning functions) in the FROM clause, which PG
-- allows for any SRF. Memgres has limitations on SRF-in-FROM usage.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS srf_test CASCADE;
CREATE SCHEMA srf_test;
SET search_path = srf_test, public;

-- ============================================================================
-- 1. generate_series() in FROM clause
-- ============================================================================

-- begin-expected
-- columns: x
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- row: 5
-- end-expected
SELECT x FROM generate_series(1, 5) AS x;

-- ============================================================================
-- 2. generate_series() in FROM with alias and WHERE
-- ============================================================================

-- begin-expected
-- columns: n
-- row: 2
-- row: 4
-- end-expected
SELECT n FROM generate_series(1, 5) AS n WHERE n % 2 = 0;

-- ============================================================================
-- 3. generate_series() in FROM with JOIN
-- ============================================================================

CREATE TABLE srf_items (id integer PRIMARY KEY, name text);
INSERT INTO srf_items VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');

-- begin-expected
-- columns: id, name
-- row: 1, alpha
-- row: 2, beta
-- end-expected
SELECT i.id, i.name
FROM srf_items i
JOIN generate_series(1, 2) AS g(n) ON i.id = g.n
ORDER BY i.id;

-- ============================================================================
-- 4. unnest() in FROM clause
-- ============================================================================

-- begin-expected
-- columns: val
-- row: apple
-- row: banana
-- row: cherry
-- end-expected
SELECT val FROM unnest(ARRAY['apple', 'banana', 'cherry']) AS val;

-- ============================================================================
-- 5. unnest() in FROM with ordinal position
-- ============================================================================

-- begin-expected
-- columns: val, ord
-- row: a, 1
-- row: b, 2
-- row: c, 3
-- end-expected
SELECT val, ord FROM unnest(ARRAY['a', 'b', 'c']) WITH ORDINALITY AS t(val, ord);

-- ============================================================================
-- 6. Multiple SRFs in FROM via CROSS JOIN
-- ============================================================================

-- begin-expected
-- columns: x, y
-- row: 1, a
-- row: 1, b
-- row: 2, a
-- row: 2, b
-- end-expected
SELECT x, y
FROM generate_series(1, 2) AS x
CROSS JOIN unnest(ARRAY['a', 'b']) AS y
ORDER BY x, y;

-- ============================================================================
-- 7. generate_series() with timestamp in FROM
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 4
-- end-expected
SELECT count(*)::integer AS cnt
FROM generate_series(
  '2024-01-01'::timestamp,
  '2024-01-04'::timestamp,
  '1 day'::interval
) AS t;

-- ============================================================================
-- 8. User-defined SRF in FROM clause
-- ============================================================================

CREATE FUNCTION srf_generate_pairs(n integer) RETURNS TABLE(a integer, b integer)
LANGUAGE plpgsql AS $$
BEGIN
  FOR i IN 1..n LOOP
    a := i;
    b := i * 10;
    RETURN NEXT;
  END LOOP;
END;
$$;

-- begin-expected
-- columns: a, b
-- row: 1, 10
-- row: 2, 20
-- row: 3, 30
-- end-expected
SELECT * FROM srf_generate_pairs(3);

-- ============================================================================
-- 9. User-defined SRF in FROM with WHERE
-- ============================================================================

-- begin-expected
-- columns: a, b
-- row: 2, 20
-- row: 3, 30
-- end-expected
SELECT * FROM srf_generate_pairs(3) WHERE a >= 2;

-- ============================================================================
-- 10. User-defined SRF in FROM with JOIN
-- ============================================================================

-- begin-expected
-- columns: name, b
-- row: alpha, 10
-- row: beta, 20
-- row: gamma, 30
-- end-expected
SELECT i.name, p.b
FROM srf_items i
JOIN srf_generate_pairs(3) p ON i.id = p.a
ORDER BY i.id;

-- ============================================================================
-- 11. SRF in subquery FROM clause
-- ============================================================================

-- begin-expected
-- columns: total
-- row: 15
-- end-expected
SELECT sum(x)::integer AS total FROM (
  SELECT x FROM generate_series(1, 5) AS x
) sub;

-- ============================================================================
-- 12. LATERAL join with SRF
-- ============================================================================

CREATE TABLE srf_ranges (id integer, cnt integer);
INSERT INTO srf_ranges VALUES (1, 3), (2, 2);

-- begin-expected
-- columns: id, n
-- row: 1, 1
-- row: 1, 2
-- row: 1, 3
-- row: 2, 1
-- row: 2, 2
-- end-expected
SELECT r.id, g.n
FROM srf_ranges r,
     LATERAL generate_series(1, r.cnt) AS g(n)
ORDER BY r.id, g.n;

-- ============================================================================
-- 13. SRF in FROM used in CTE
-- ============================================================================

-- begin-expected
-- columns: sq
-- row: 1
-- row: 4
-- row: 9
-- row: 16
-- end-expected
WITH nums AS (
  SELECT x FROM generate_series(1, 4) AS x
)
SELECT x * x AS sq FROM nums;

-- ============================================================================
-- 14. generate_subscripts() in FROM
-- ============================================================================

-- begin-expected
-- columns: idx
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT idx FROM generate_subscripts(ARRAY[10, 20, 30], 1) AS idx;

-- ============================================================================
-- 15. regexp_split_to_table() in FROM
-- ============================================================================

-- begin-expected
-- columns: word
-- row: hello
-- row: world
-- row: foo
-- end-expected
SELECT word FROM regexp_split_to_table('hello world foo', '\s+') AS word;

-- ============================================================================
-- 16. string_to_table() in FROM (PG 14+)
-- ============================================================================

-- begin-expected
-- columns: part
-- row: a
-- row: b
-- row: c
-- end-expected
SELECT part FROM string_to_table('a,b,c', ',') AS part;

-- ============================================================================
-- 17. SRF in FROM with aggregation
-- ============================================================================

-- begin-expected
-- columns: total, cnt
-- row: 55, 10
-- end-expected
SELECT sum(x)::integer AS total, count(*)::integer AS cnt
FROM generate_series(1, 10) AS x;

-- ============================================================================
-- 18. SRF in FROM with GROUP BY
-- ============================================================================

-- begin-expected
-- columns: bucket, cnt
-- row: 0, 3
-- row: 1, 4
-- row: 2, 3
-- end-expected
SELECT (x / 4) AS bucket, count(*)::integer AS cnt
FROM generate_series(1, 10) AS x
GROUP BY (x / 4)
ORDER BY bucket;

-- ============================================================================
-- 19. Nested SRF calls in FROM
-- ============================================================================

-- begin-expected
-- columns: elem
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT elem FROM unnest(ARRAY(SELECT x FROM generate_series(1, 3) AS x)) AS elem;

-- ============================================================================
-- 20. SRF with column definition list
-- ============================================================================

CREATE FUNCTION srf_record_func() RETURNS SETOF record
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY SELECT 1, 'one'::text;
  RETURN QUERY SELECT 2, 'two'::text;
END;
$$;

-- begin-expected
-- columns: id, name
-- row: 1, one
-- row: 2, two
-- end-expected
SELECT * FROM srf_record_func() AS t(id integer, name text);

-- ============================================================================
-- 21. json_each() in FROM (if available)
-- ============================================================================

-- begin-expected
-- columns: k, v
-- row: a, 1
-- row: b, 2
-- end-expected
SELECT key AS k, value AS v
FROM json_each('{"a": 1, "b": 2}'::json)
ORDER BY key;

-- ============================================================================
-- 22. jsonb_array_elements() in FROM
-- ============================================================================

-- begin-expected
-- columns: elem
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT elem::integer
FROM jsonb_array_elements('[1, 2, 3]'::jsonb) AS elem
ORDER BY elem::integer;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA srf_test CASCADE;
SET search_path = public;
