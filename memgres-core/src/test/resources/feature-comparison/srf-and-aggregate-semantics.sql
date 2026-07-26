-- ============================================================================
-- Feature Comparison: set-returning functions and aggregate result semantics
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A set-returning function belongs in FROM or at the top of a select-list
-- expression; anywhere else PostgreSQL refuses rather than guessing what the
-- extra rows should mean. Aggregates are equally strict about what a group is
-- allowed to reference once DISTINCT or GROUP BY has collapsed it.
-- ============================================================================

DROP TABLE IF EXISTS sas_t CASCADE;
CREATE TABLE sas_t (id int PRIMARY KEY, x float8, n numeric, k int, b text);
INSERT INTO sas_t VALUES (1, 1.5, 1.5, 3, 'p'), (2, 2.5, 2.5, 1, 'q'), (3, 3.5, 3.5, 2, 'r');

-- ============================================================================
-- 1. generate_series over numeric and fractional bounds
-- ============================================================================

-- begin-expected
-- columns: g
-- row: {1.0;1.25;1.50;1.75;2.00}
-- end-expected
SELECT replace(array_agg(g::text)::text, ',', ';') AS g FROM generate_series(1.0, 2.0, 0.25) g;

-- begin-expected
-- columns: g
-- row: {1.5;2.5;3.5}
-- end-expected
SELECT replace(array_agg(g::text)::text, ',', ';') AS g FROM generate_series(1.5, 3.5) g;

-- A zero step can never terminate
-- begin-expected-error
-- sqlstate: 22023
-- message-like: step size cannot equal zero
-- end-expected-error
SELECT count(*) FROM generate_series(1, 3, 0);

-- A negative step with an ascending range yields nothing, not an endless series
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM generate_series(
  timestamp '2024-01-01', timestamp '2024-01-05', interval '-1 day');

-- begin-expected
-- columns: n
-- row: 5
-- end-expected
SELECT count(*) AS n FROM generate_series(
  timestamp '2024-01-05', timestamp '2024-01-01', interval '-1 day');

-- ============================================================================
-- 2. Aggregate and operator result types
-- ============================================================================

-- begin-expected
-- columns: a | b | c | d
-- row: double precision, double precision, double precision, double precision
-- end-expected
SELECT pg_typeof(avg(x))::text AS a, pg_typeof(sum(x))::text AS b,
       pg_typeof(stddev(x))::text AS c, pg_typeof(variance(x))::text AS d
  FROM sas_t;

-- begin-expected
-- columns: a | b
-- row: double precision, double precision
-- end-expected
SELECT pg_typeof(corr(x, n::float8))::text AS a,
       pg_typeof(covar_pop(x, n::float8))::text AS b FROM sas_t;

-- begin-expected
-- columns: t | v
-- row: double precision, 8
-- end-expected
SELECT pg_typeof(2^3)::text AS t, (2^3)::text AS v;

-- begin-expected
-- columns: v | t
-- row: 1000.0000000000000, numeric
-- end-expected
SELECT (10.0^3)::text AS v, pg_typeof(10.0^3)::text AS t;

-- ============================================================================
-- 3. A set-returning function may not hide in a conditional or a filter
-- ============================================================================

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in CASE
-- end-expected-error
SELECT CASE WHEN true THEN generate_series(1,3) END;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in COALESCE
-- end-expected-error
SELECT COALESCE(generate_series(1,3), 0);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in WHERE
-- end-expected-error
SELECT 1 FROM sas_t WHERE generate_series(1,3) > 1;

-- ============================================================================
-- 4. WITH ORDINALITY alias handling
-- ============================================================================

-- begin-expected
-- columns: a | b
-- row: x, 1
-- row: y, 2
-- end-expected
SELECT a, b FROM unnest(ARRAY['x','y']) WITH ORDINALITY AS t(a, b);

-- A short alias list still leaves the ordinality column in place
-- begin-expected
-- columns: a | ordinality
-- row: x, 1
-- row: y, 2
-- end-expected
SELECT * FROM unnest(ARRAY['x','y']) WITH ORDINALITY AS t(a);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: has 2 columns available but 3 columns specified
-- end-expected-error
SELECT * FROM unnest(ARRAY['x','y']) WITH ORDINALITY AS t(a, b, c);

-- ============================================================================
-- 5. Aggregate and GROUP BY validation
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause or be used in an aggregate function
-- end-expected-error
SELECT b FROM sas_t GROUP BY b HAVING k > 1;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ORDER BY expressions must appear in argument list
-- end-expected-error
SELECT string_agg(DISTINCT b, ',' ORDER BY k) FROM sas_t;

-- ============================================================================
-- 6. DESC ordering places NULLs first
-- ============================================================================

-- begin-expected
-- columns: a
-- row: {NULL;2;1}
-- end-expected
SELECT replace(array_agg(DISTINCT v ORDER BY v DESC)::text, ',', ';') AS a
  FROM (VALUES (1),(2),(NULL::int)) s(v);

-- begin-expected
-- columns: a
-- row: {1;2;NULL}
-- end-expected
SELECT replace(array_agg(DISTINCT v ORDER BY v)::text, ',', ';') AS a
  FROM (VALUES (1),(2),(NULL::int)) s(v);

-- ============================================================================
-- 7. Ordered-set and JSON functions carry their own column label
-- ============================================================================

-- begin-expected
-- columns: percentile_cont
-- row: 2.5
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM sas_t;

-- begin-expected
-- columns: mode
-- row: 1
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY k) FROM sas_t;

-- begin-expected
-- columns: percentile_disc
-- row: 2
-- end-expected
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY k) FROM sas_t;

-- begin-expected
-- columns: json_value
-- row: 1
-- end-expected
SELECT json_value('{"a": 1}'::jsonb, '$.a' RETURNING int);

DROP TABLE sas_t;
