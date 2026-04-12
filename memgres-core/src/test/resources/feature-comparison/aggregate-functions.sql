-- ============================================================================
-- Feature Comparison: CREATE AGGREGATE & Aggregate Functions
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

DROP SCHEMA IF EXISTS agg_test CASCADE;
CREATE SCHEMA agg_test;
SET search_path = agg_test, public;

CREATE TABLE agg_data (id serial PRIMARY KEY, grp text, val integer, label text);
INSERT INTO agg_data (grp, val, label) VALUES
  ('a', 10, 'x'), ('a', 20, 'y'), ('a', 30, 'z'),
  ('b', 5, 'p'), ('b', 15, 'q'),
  ('c', NULL, 'r'), ('c', 40, 's');

CREATE TABLE agg_empty (id integer, val integer);

-- ============================================================================
-- 1. Basic CREATE AGGREGATE: integer sum
-- ============================================================================

CREATE FUNCTION agg_int_add(state integer, val integer) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$ SELECT state + val $$;

CREATE AGGREGATE agg_mysum(integer) (
  SFUNC = agg_int_add,
  STYPE = integer,
  INITCOND = '0'
);

-- begin-expected
-- columns: total
-- row: 120
-- end-expected
SELECT agg_mysum(val) AS total FROM agg_data WHERE val IS NOT NULL;

-- ============================================================================
-- 2. Custom aggregate with GROUP BY
-- ============================================================================

-- begin-expected
-- columns: grp, total
-- row: a, 60
-- row: b, 20
-- row: c, 40
-- end-expected
SELECT grp, agg_mysum(val) AS total
FROM agg_data
WHERE val IS NOT NULL
GROUP BY grp
ORDER BY grp;

-- ============================================================================
-- 3. Custom aggregate on empty table
-- ============================================================================

-- note: With INITCOND, empty input returns INITCOND value
-- begin-expected
-- columns: total
-- row: 0
-- end-expected
SELECT agg_mysum(val) AS total FROM agg_empty;

-- ============================================================================
-- 4. Aggregate with FINALFUNC
-- ============================================================================

CREATE FUNCTION agg_text_accum(state text, val text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT CASE WHEN state = '' THEN val ELSE state || ',' || val END $$;

CREATE FUNCTION agg_text_finish(state text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT '[' || state || ']' $$;

CREATE AGGREGATE agg_concat_bracket(text) (
  SFUNC = agg_text_accum,
  STYPE = text,
  INITCOND = '',
  FINALFUNC = agg_text_finish
);

-- begin-expected
-- columns: result
-- row: [x,y,z,p,q,r,s]
-- end-expected
SELECT agg_concat_bracket(label) AS result FROM agg_data ORDER BY id;

-- ============================================================================
-- 5. Aggregate without INITCOND
-- ============================================================================

-- note: Without INITCOND, first non-NULL value becomes initial state

CREATE AGGREGATE agg_mysum_no_init(integer) (
  SFUNC = agg_int_add,
  STYPE = integer
);

-- begin-expected
-- columns: total
-- row: 120
-- end-expected
SELECT agg_mysum_no_init(val) AS total FROM agg_data WHERE val IS NOT NULL;

-- On empty table, returns NULL (no initial state)
-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT agg_mysum_no_init(val) IS NULL AS is_null FROM agg_empty;

-- ============================================================================
-- 6. Aggregate with all NULLs (STRICT SFUNC + INITCOND)
-- ============================================================================

-- note: See strict-functions.sql for comprehensive STRICT SFUNC testing.
-- Here we just verify the basic behavior with our aggregate.

-- begin-expected
-- columns: total
-- row: 0
-- end-expected
SELECT agg_mysum(val) AS total
FROM (VALUES (NULL::integer), (NULL::integer)) AS t(val);

-- ============================================================================
-- 7. Aggregate with FILTER clause
-- ============================================================================

-- begin-expected
-- columns: total_all, total_big
-- row: 120, 90
-- end-expected
SELECT
  sum(val) AS total_all,
  sum(val) FILTER (WHERE val >= 20) AS total_big
FROM agg_data;

-- FILTER with custom aggregate
-- begin-expected
-- columns: mysum_big
-- row: 90
-- end-expected
SELECT agg_mysum(val) FILTER (WHERE val >= 20) AS mysum_big FROM agg_data;

-- ============================================================================
-- 8. Aggregate with ORDER BY inside (ordered-set style)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: p,q
-- end-expected
SELECT string_agg(label, ',' ORDER BY label) AS result
FROM agg_data
WHERE grp = 'b';

-- ============================================================================
-- 9. Aggregate with DISTINCT
-- ============================================================================

CREATE TABLE agg_dups (val integer);
INSERT INTO agg_dups VALUES (1), (1), (2), (2), (3);

-- begin-expected
-- columns: cnt_all, cnt_distinct
-- row: 5, 3
-- end-expected
SELECT count(val) AS cnt_all, count(DISTINCT val) AS cnt_distinct FROM agg_dups;

-- begin-expected
-- columns: sum_all, sum_distinct
-- row: 9, 6
-- end-expected
SELECT sum(val) AS sum_all, sum(DISTINCT val) AS sum_distinct FROM agg_dups;

DROP TABLE agg_dups;

-- ============================================================================
-- 10. Aggregate in HAVING clause
-- ============================================================================

-- begin-expected
-- columns: grp, total
-- row: a, 60
-- row: c, 40
-- end-expected
SELECT grp, sum(val) AS total
FROM agg_data
WHERE val IS NOT NULL
GROUP BY grp
HAVING sum(val) >= 40
ORDER BY grp;

-- ============================================================================
-- 11. Aggregate in window function
-- ============================================================================

-- begin-expected
-- columns: id, val, running_sum
-- row: 1, 10, 10
-- row: 2, 20, 30
-- row: 3, 30, 60
-- end-expected
SELECT id, val, sum(val) OVER (ORDER BY id) AS running_sum
FROM agg_data
WHERE grp = 'a'
ORDER BY id;

-- ============================================================================
-- 12. Aggregate with CASE expression
-- ============================================================================

-- begin-expected
-- columns: a_sum, b_sum
-- row: 60, 20
-- end-expected
SELECT
  sum(CASE WHEN grp = 'a' THEN val ELSE 0 END) AS a_sum,
  sum(CASE WHEN grp = 'b' THEN val ELSE 0 END) AS b_sum
FROM agg_data;

-- ============================================================================
-- 13. Custom aggregate with text accumulator (no FINALFUNC)
-- ============================================================================

CREATE FUNCTION agg_csv_accum(state text, val text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN state = '' THEN val ELSE state || ',' || val END
$$;

CREATE AGGREGATE agg_csv(text) (
  SFUNC = agg_csv_accum,
  STYPE = text,
  INITCOND = ''
);

-- begin-expected
-- columns: result
-- row: x,y,z
-- end-expected
SELECT agg_csv(label) AS result FROM agg_data WHERE grp = 'a' ORDER BY id;

-- ============================================================================
-- 14. Multiple aggregates in one query
-- ============================================================================

-- begin-expected
-- columns: my_total, pg_total, my_csv
-- row: 60, 60, x,y,z
-- end-expected
SELECT
  agg_mysum(val) AS my_total,
  sum(val) AS pg_total,
  agg_csv(label) AS my_csv
FROM agg_data
WHERE grp = 'a';

-- ============================================================================
-- 15. Aggregate on joined tables
-- ============================================================================

CREATE TABLE agg_groups (grp text PRIMARY KEY, description text);
INSERT INTO agg_groups VALUES ('a', 'Group A'), ('b', 'Group B'), ('c', 'Group C');

-- begin-expected
-- columns: description, total
-- row: Group A, 60
-- row: Group B, 20
-- row: Group C, 40
-- end-expected
SELECT g.description, sum(d.val) AS total
FROM agg_data d
JOIN agg_groups g ON d.grp = g.grp
WHERE d.val IS NOT NULL
GROUP BY g.description
ORDER BY g.description;

DROP TABLE agg_groups;

-- ============================================================================
-- 16. Aggregate with CTE
-- ============================================================================

-- begin-expected
-- columns: grp, avg_val
-- row: a, 20
-- row: b, 10
-- end-expected
WITH filtered AS (
  SELECT grp, val FROM agg_data WHERE val IS NOT NULL AND grp IN ('a', 'b')
)
SELECT grp, avg(val)::integer AS avg_val
FROM filtered
GROUP BY grp
ORDER BY grp;

-- ============================================================================
-- 17. Nested aggregates via subquery
-- ============================================================================

-- begin-expected
-- columns: max_group_sum
-- row: 60
-- end-expected
SELECT max(group_sum) AS max_group_sum
FROM (
  SELECT sum(val) AS group_sum
  FROM agg_data
  WHERE val IS NOT NULL
  GROUP BY grp
) sub;

-- ============================================================================
-- 18. DROP AGGREGATE
-- ============================================================================

CREATE FUNCTION agg_temp_fn(integer, integer) RETURNS integer
LANGUAGE sql AS $$ SELECT $1 + $2 $$;

CREATE AGGREGATE agg_temp(integer) (
  SFUNC = agg_temp_fn,
  STYPE = integer,
  INITCOND = '0'
);

-- Verify it works
-- begin-expected
-- columns: result
-- row: 6
-- end-expected
SELECT agg_temp(x) AS result FROM (VALUES (1), (2), (3)) AS t(x);

DROP AGGREGATE agg_temp(integer);

-- begin-expected-error
-- message-like: aggregate agg_temp(integer) does not exist
-- end-expected-error
SELECT agg_temp(1);

-- ============================================================================
-- 19. DROP AGGREGATE IF EXISTS
-- ============================================================================

-- command: DROP AGGREGATE
DROP AGGREGATE IF EXISTS agg_nonexistent(integer);

-- ============================================================================
-- 20. Aggregate with NULL handling (non-STRICT SFUNC)
-- ============================================================================

-- note: By default in PG, aggregates skip NULL values for the value arguments.
-- But a non-STRICT SFUNC explicitly handles NULLs if they reach it.
-- The standard aggregate (sum, etc.) skip NULLs automatically.

-- begin-expected
-- columns: total
-- row: 120
-- end-expected
SELECT sum(val) AS total FROM agg_data;

-- ============================================================================
-- 21. Built-in aggregates: sum, avg, count, min, max
-- ============================================================================

-- begin-expected
-- columns: s, a, c, mn, mx
-- row: 120, 20, 6, 5, 40
-- end-expected
SELECT
  sum(val) AS s,
  avg(val)::integer AS a,
  count(val) AS c,
  min(val) AS mn,
  max(val) AS mx
FROM agg_data;

-- count(*) includes NULLs, count(val) does not
-- begin-expected
-- columns: count_star, count_val
-- row: 7, 6
-- end-expected
SELECT count(*) AS count_star, count(val) AS count_val FROM agg_data;

-- ============================================================================
-- 22. Built-in aggregates: array_agg, string_agg
-- ============================================================================

-- begin-expected
-- columns: labels
-- row: x,y,z
-- end-expected
SELECT string_agg(label, ',' ORDER BY id) AS labels
FROM agg_data
WHERE grp = 'a';

-- begin-expected
-- columns: vals
-- row: {10,20,30}
-- end-expected
SELECT array_agg(val ORDER BY id) AS vals
FROM agg_data
WHERE grp = 'a';

-- ============================================================================
-- 23. Built-in aggregates: bool_and, bool_or, every
-- ============================================================================

CREATE TABLE agg_bool (id integer, flag boolean);
INSERT INTO agg_bool VALUES (1, true), (2, true), (3, false);

-- begin-expected
-- columns: band, bor, ev
-- row: false, true, false
-- end-expected
SELECT bool_and(flag) AS band, bool_or(flag) AS bor, every(flag) AS ev FROM agg_bool;

-- With only true values
-- begin-expected
-- columns: band, bor
-- row: true, true
-- end-expected
SELECT bool_and(flag) AS band, bool_or(flag) AS bor
FROM agg_bool WHERE flag = true;

DROP TABLE agg_bool;

-- ============================================================================
-- 24. Built-in aggregates: bit_and, bit_or, bit_xor
-- ============================================================================

CREATE TABLE agg_bits (val integer);
INSERT INTO agg_bits VALUES (12), (10), (14);  -- 1100, 1010, 1110

-- begin-expected
-- columns: band, bor, bxor
-- row: 8, 14, 12
-- end-expected
SELECT bit_and(val) AS band, bit_or(val) AS bor, bit_xor(val) AS bxor FROM agg_bits;

DROP TABLE agg_bits;

-- ============================================================================
-- 25. Aggregate with complex state type (array accumulator)
-- ============================================================================

CREATE FUNCTION agg_array_append_fn(state integer[], val integer) RETURNS integer[]
LANGUAGE sql IMMUTABLE AS $$ SELECT state || val $$;

CREATE FUNCTION agg_array_sort_finish(state integer[]) RETURNS integer[]
LANGUAGE sql IMMUTABLE AS $$ SELECT array_agg(x ORDER BY x) FROM unnest(state) AS x $$;

CREATE AGGREGATE agg_sorted_array(integer) (
  SFUNC = agg_array_append_fn,
  STYPE = integer[],
  INITCOND = '{}',
  FINALFUNC = agg_array_sort_finish
);

-- begin-expected
-- columns: result
-- row: {5,10,15,20,30,40}
-- end-expected
SELECT agg_sorted_array(val) AS result FROM agg_data WHERE val IS NOT NULL;

-- ============================================================================
-- 26. Custom aggregate per group
-- ============================================================================

-- begin-expected
-- columns: grp, sorted_vals
-- row: a, {10,20,30}
-- row: b, {5,15}
-- row: c, {40}
-- end-expected
SELECT grp, agg_sorted_array(val) AS sorted_vals
FROM agg_data
WHERE val IS NOT NULL
GROUP BY grp
ORDER BY grp;

-- ============================================================================
-- 27. Aggregate with COALESCE
-- ============================================================================

-- begin-expected
-- columns: total
-- row: 120
-- end-expected
SELECT sum(COALESCE(val, 0)) AS total FROM agg_data;

-- ============================================================================
-- 28. Multiple custom aggregates with different types
-- ============================================================================

CREATE FUNCTION agg_text_len_add(state integer, val text) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$ SELECT state + length(val) $$;

CREATE AGGREGATE agg_total_length(text) (
  SFUNC = agg_text_len_add,
  STYPE = integer,
  INITCOND = '0'
);

-- begin-expected
-- columns: total_chars
-- row: 7
-- end-expected
SELECT agg_total_length(label) AS total_chars FROM agg_data;

-- ============================================================================
-- 29. Aggregate in subquery
-- ============================================================================

-- begin-expected
-- columns: grp
-- row: a
-- end-expected
SELECT grp
FROM agg_data
WHERE val IS NOT NULL
GROUP BY grp
HAVING sum(val) = (
  SELECT max(grp_total) FROM (
    SELECT sum(val) AS grp_total FROM agg_data WHERE val IS NOT NULL GROUP BY grp
  ) sub
);

-- ============================================================================
-- 30. Aggregate: DISTINCT + ORDER BY inside string_agg
-- ============================================================================

CREATE TABLE agg_dup_labels (label text);
INSERT INTO agg_dup_labels VALUES ('c'), ('a'), ('b'), ('a'), ('c');

-- begin-expected
-- columns: result
-- row: a,b,c
-- end-expected
SELECT string_agg(DISTINCT label, ',' ORDER BY label) AS result FROM agg_dup_labels;

DROP TABLE agg_dup_labels;

-- ============================================================================
-- 31. Window aggregate: ROW_NUMBER, RANK, DENSE_RANK
-- ============================================================================

-- begin-expected
-- columns: id, val, rn, rnk
-- row: 4, 5, 1, 1
-- row: 5, 15, 2, 2
-- row: 1, 10, 3, 3
-- row: 2, 20, 4, 4
-- row: 3, 30, 5, 5
-- row: 7, 40, 6, 6
-- end-expected
SELECT id, val,
  row_number() OVER (ORDER BY val) AS rn,
  rank() OVER (ORDER BY val) AS rnk
FROM agg_data
WHERE val IS NOT NULL
ORDER BY val, id;

-- ============================================================================
-- 32. Window aggregate: running sum with ROWS frame
-- ============================================================================

-- begin-expected
-- columns: id, val, rolling3
-- row: 1, 10, 10
-- row: 2, 20, 30
-- row: 3, 30, 60
-- end-expected
SELECT id, val,
  sum(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rolling3
FROM agg_data
WHERE grp = 'a'
ORDER BY id;

-- ============================================================================
-- 33. Aggregate: error on nested aggregate
-- ============================================================================

-- begin-expected-error
-- message-like: aggregate function calls cannot be nested
-- end-expected-error
SELECT sum(count(*)) FROM agg_data;

-- ============================================================================
-- 34. Aggregate with UNION
-- ============================================================================

-- begin-expected
-- columns: total
-- row: 180
-- end-expected
SELECT sum(val) AS total FROM (
  SELECT val FROM agg_data WHERE grp = 'a'
  UNION ALL
  SELECT val FROM agg_data WHERE val IS NOT NULL
) combined;

-- ============================================================================
-- 35. CREATE AGGREGATE with wrong SFUNC signature
-- ============================================================================

-- begin-expected-error
-- message-like: function agg_nonexistent_fn(integer, integer) does not exist
-- end-expected-error
CREATE AGGREGATE agg_bad(integer) (
  SFUNC = agg_nonexistent_fn,
  STYPE = integer
);

-- ============================================================================
-- 36. Aggregate: count with various expressions
-- ============================================================================

-- begin-expected
-- columns: c1, c2, c3
-- row: 7, 6, 3
-- end-expected
SELECT
  count(*) AS c1,
  count(val) AS c2,
  count(val) FILTER (WHERE val > 15) AS c3
FROM agg_data;

-- ============================================================================
-- 37. Custom aggregate used in ORDER BY
-- ============================================================================

-- begin-expected
-- columns: grp
-- row: b
-- row: c
-- row: a
-- end-expected
SELECT grp
FROM agg_data
WHERE val IS NOT NULL
GROUP BY grp
ORDER BY agg_mysum(val);

-- ============================================================================
-- 38. Aggregate on single-row table
-- ============================================================================

CREATE TABLE agg_single (val integer);
INSERT INTO agg_single VALUES (42);

-- begin-expected
-- columns: s, a, c, mn, mx
-- row: 42, 42, 1, 42, 42
-- end-expected
SELECT sum(val) AS s, avg(val)::integer AS a, count(val) AS c, min(val) AS mn, max(val) AS mx
FROM agg_single;

DROP TABLE agg_single;

-- ============================================================================
-- 39. Aggregate on all-NULL column
-- ============================================================================

CREATE TABLE agg_nulls (val integer);
INSERT INTO agg_nulls VALUES (NULL), (NULL), (NULL);

-- begin-expected
-- columns: s, a, c
-- row: ,, 0
-- end-expected
SELECT sum(val) AS s, avg(val) AS a, count(val) AS c FROM agg_nulls;

DROP TABLE agg_nulls;

-- ============================================================================
-- 40. Statistical aggregates: variance, stddev
-- ============================================================================

-- begin-expected
-- columns: var_val, stddev_val
-- row: 150.00, 12.25
-- end-expected
SELECT
  round(var_samp(val)::numeric, 2) AS var_val,
  round(stddev_samp(val)::numeric, 2) AS stddev_val
FROM agg_data
WHERE grp = 'a';

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA agg_test CASCADE;
SET search_path = public;
