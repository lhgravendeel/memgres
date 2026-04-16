-- ============================================================================
-- Feature Comparison: Round 18 — JSON / JSONB family siblings
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION W1: json_populate_record
-- ============================================================================

DROP TYPE IF EXISTS r18_jpr CASCADE;
CREATE TYPE r18_jpr AS (a int, b text);

-- 1. json_populate_record yields typed row
-- begin-expected
-- columns: a,b
-- row: 7,x
-- end-expected
SELECT a, b
  FROM json_populate_record(null::r18_jpr, '{"a":7,"b":"x"}'::json);

-- ============================================================================
-- SECTION W2: jsonb_to_record returns typed columns
-- ============================================================================

-- 2. jsonb_to_record with (int,int) returns integers
-- begin-expected
-- columns: a,b
-- row: 1,2
-- end-expected
SELECT a, b
  FROM jsonb_to_record('{"a":1,"b":2}'::jsonb) AS x(a int, b int);

-- ============================================================================
-- SECTION W3: row_to_json on composite
-- ============================================================================

-- 3. row_to_json round-trips
-- begin-expected
-- columns: j
-- row: {"a":1,"b":"y"}
-- end-expected
SELECT row_to_json(x) AS j FROM (SELECT 1 AS a, 'y' AS b) x;

-- ============================================================================
-- SECTION W4: row_to_json(rec, pretty)
-- ============================================================================

-- 4. 2-arg form parses and pretty-prints
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (row_to_json(x, true) LIKE E'%\n%') AS ok FROM (SELECT 1 AS a) x;

-- ============================================================================
-- SECTION W5: jsonb_set with create_missing=false
-- ============================================================================

-- 5. jsonb_set create_missing=false on missing path returns original
-- begin-expected
-- columns: j
-- row: {"a": 1}
-- end-expected
SELECT jsonb_set('{"a":1}'::jsonb, '{b}', '99', false)::text AS j;

-- ============================================================================
-- SECTION W6: jsonb_strip_nulls / json_strip_nulls
-- ============================================================================

-- 6. jsonb_strip_nulls drops null-valued keys
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (jsonb_strip_nulls('{"a":1,"b":null,"c":2}'::jsonb)::text NOT LIKE '%null%') AS ok;

-- 7. json_strip_nulls registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='json_strip_nulls';

-- ============================================================================
-- SECTION W7: jsonb - text[] operator
-- ============================================================================

-- 8. jsonb - text[] removes listed keys
-- begin-expected
-- columns: j
-- row: {"b": 2}
-- end-expected
SELECT ('{"a":1,"b":2,"c":3}'::jsonb - ARRAY['a','c'])::text AS j;

-- ============================================================================
-- SECTION W8: jsonb_path_match registered
-- ============================================================================

-- 9. jsonb_path_match registered
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='jsonb_path_match';

-- ============================================================================
-- SECTION W9: jsonb_path_query_first
-- ============================================================================

-- 10. jsonb_path_query_first returns first match
-- begin-expected
-- columns: v
-- row: 2
-- end-expected
SELECT jsonb_path_query_first('{"a":[1,2,3]}'::jsonb, '$.a[*] ? (@ > 1)')::text AS v;
