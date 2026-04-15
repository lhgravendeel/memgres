-- ============================================================================
-- Feature Comparison: Ordered-Set Aggregate Functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests the WITHIN GROUP (ORDER BY ...) aggregate functions:
-- percentile_cont, percentile_disc, mode, rank, dense_rank,
-- percent_rank, cume_dist (hypothetical-set aggregates).
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS osa_test CASCADE;
CREATE SCHEMA osa_test;
SET search_path = osa_test, public;

CREATE TABLE osa_data (id serial, grp text, val numeric);
INSERT INTO osa_data (grp, val) VALUES
  ('a', 10), ('a', 20), ('a', 30), ('a', 40), ('a', 50),
  ('b', 5), ('b', 15), ('b', 25),
  ('c', 100), ('c', 200);

CREATE TABLE osa_scores (student text, score integer);
INSERT INTO osa_scores VALUES
  ('alice', 85), ('bob', 92), ('carol', 78),
  ('dave', 92), ('eve', 88), ('frank', 95),
  ('grace', 78), ('heidi', 88);

-- ============================================================================
-- SECTION A: percentile_cont
-- ============================================================================

-- ============================================================================
-- 1. percentile_cont: median (50th percentile)
-- ============================================================================

-- begin-expected
-- columns: median
-- row: 30
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY val)::integer AS median
FROM osa_data WHERE grp = 'a';

-- ============================================================================
-- 2. percentile_cont: 25th and 75th percentiles
-- ============================================================================

-- begin-expected
-- columns: p25, p75
-- row: 20, 40
-- end-expected
SELECT
  percentile_cont(0.25) WITHIN GROUP (ORDER BY val)::integer AS p25,
  percentile_cont(0.75) WITHIN GROUP (ORDER BY val)::integer AS p75
FROM osa_data WHERE grp = 'a';

-- ============================================================================
-- 3. percentile_cont: 0th and 100th percentiles (min/max)
-- ============================================================================

-- begin-expected
-- columns: p0, p100
-- row: 10, 50
-- end-expected
SELECT
  percentile_cont(0) WITHIN GROUP (ORDER BY val)::integer AS p0,
  percentile_cont(1) WITHIN GROUP (ORDER BY val)::integer AS p100
FROM osa_data WHERE grp = 'a';

-- ============================================================================
-- 4. percentile_cont with GROUP BY
-- ============================================================================

-- begin-expected
-- columns: grp, median
-- row: a, 30
-- row: b, 15
-- row: c, 150
-- end-expected
SELECT grp,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY val)::integer AS median
FROM osa_data
GROUP BY grp
ORDER BY grp;

-- ============================================================================
-- 5. percentile_cont: array form (multiple percentiles at once)
-- ============================================================================

-- begin-expected
-- columns: percentiles
-- row: {10,30,50}
-- end-expected
SELECT percentile_cont(ARRAY[0.0, 0.5, 1.0]) WITHIN GROUP (ORDER BY val)::integer[] AS percentiles
FROM osa_data WHERE grp = 'a';

-- ============================================================================
-- 6. percentile_cont: single-row input
-- ============================================================================

-- begin-expected
-- columns: p50
-- row: 100
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x)::integer AS p50
FROM (VALUES (100)) AS t(x);

-- ============================================================================
-- 7. percentile_cont: interpolation between values
-- ============================================================================

-- note: With values 10, 20, the 50th percentile is 15 (interpolated)
-- begin-expected
-- columns: p50
-- row: 15
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x)::integer AS p50
FROM (VALUES (10), (20)) AS t(x);

-- ============================================================================
-- SECTION B: percentile_disc
-- ============================================================================

-- ============================================================================
-- 8. percentile_disc: median
-- ============================================================================

-- note: percentile_disc returns an actual value (no interpolation)
-- begin-expected
-- columns: median
-- row: 30
-- end-expected
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY val)::integer AS median
FROM osa_data WHERE grp = 'a';

-- ============================================================================
-- 9. percentile_disc: exact percentile boundaries
-- ============================================================================

-- begin-expected
-- columns: p0, p100
-- row: 10, 50
-- end-expected
SELECT
  percentile_disc(0) WITHIN GROUP (ORDER BY val)::integer AS p0,
  percentile_disc(1) WITHIN GROUP (ORDER BY val)::integer AS p100
FROM osa_data WHERE grp = 'a';

-- ============================================================================
-- 10. percentile_disc with GROUP BY
-- ============================================================================

-- begin-expected
-- columns: grp, median
-- row: a, 30
-- row: b, 15
-- row: c, 100
-- end-expected
SELECT grp,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY val)::integer AS median
FROM osa_data
GROUP BY grp
ORDER BY grp;

-- ============================================================================
-- 11. percentile_disc: array form
-- ============================================================================

-- begin-expected
-- columns: percentiles
-- row: {10,30,50}
-- end-expected
SELECT percentile_disc(ARRAY[0.0, 0.5, 1.0]) WITHIN GROUP (ORDER BY val)::integer[] AS percentiles
FROM osa_data WHERE grp = 'a';

-- ============================================================================
-- SECTION C: mode()
-- ============================================================================

-- ============================================================================
-- 12. mode(): most frequent value
-- ============================================================================

-- begin-expected
-- columns: modal_score
-- row: 78
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY score) AS modal_score
FROM osa_scores;

-- ============================================================================
-- 13. mode() with GROUP BY
-- ============================================================================

CREATE TABLE osa_mode_data (grp text, val integer);
INSERT INTO osa_mode_data VALUES
  ('x', 1), ('x', 1), ('x', 2),
  ('y', 3), ('y', 4), ('y', 4), ('y', 4);

-- begin-expected
-- columns: grp, mode_val
-- row: x, 1
-- row: y, 4
-- end-expected
SELECT grp, mode() WITHIN GROUP (ORDER BY val) AS mode_val
FROM osa_mode_data
GROUP BY grp
ORDER BY grp;

DROP TABLE osa_mode_data;

-- ============================================================================
-- 14. mode(): all values unique (returns first in sort order)
-- ============================================================================

-- begin-expected
-- columns: mode_val
-- row: 1
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY x) AS mode_val
FROM (VALUES (3), (1), (2)) AS t(x);

-- ============================================================================
-- 15. mode(): single value
-- ============================================================================

-- begin-expected
-- columns: mode_val
-- row: 42
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY x) AS mode_val
FROM (VALUES (42)) AS t(x);

-- ============================================================================
-- SECTION D: Hypothetical-set aggregates
-- ============================================================================

-- ============================================================================
-- 16. rank(): hypothetical rank of a value
-- ============================================================================

-- note: "What rank would score 90 have in this set?"
-- begin-expected
-- columns: hyp_rank
-- row: 6
-- end-expected
SELECT rank(90) WITHIN GROUP (ORDER BY score) AS hyp_rank
FROM osa_scores;

-- ============================================================================
-- 17. dense_rank(): hypothetical dense rank
-- ============================================================================

-- begin-expected
-- columns: hyp_dense_rank
-- row: 4
-- end-expected
SELECT dense_rank(90) WITHIN GROUP (ORDER BY score) AS hyp_dense_rank
FROM osa_scores;

-- ============================================================================
-- 18. rank() vs dense_rank() comparison
-- ============================================================================

-- begin-expected
-- columns: r_rank, r_dense
-- row: 1, 1
-- end-expected
SELECT
  rank(70) WITHIN GROUP (ORDER BY score) AS r_rank,
  dense_rank(70) WITHIN GROUP (ORDER BY score) AS r_dense
FROM osa_scores;

-- begin-expected
-- columns: r_rank, r_dense
-- row: 9, 6
-- end-expected
SELECT
  rank(100) WITHIN GROUP (ORDER BY score) AS r_rank,
  dense_rank(100) WITHIN GROUP (ORDER BY score) AS r_dense
FROM osa_scores;

-- ============================================================================
-- 19. percent_rank(): hypothetical percent rank
-- ============================================================================

-- begin-expected
-- columns: pct_rank
-- row: 0.00
-- end-expected
SELECT round(percent_rank(70) WITHIN GROUP (ORDER BY score)::numeric, 2) AS pct_rank
FROM osa_scores;

-- begin-expected
-- columns: pct_rank
-- row: 1.00
-- end-expected
SELECT round(percent_rank(100) WITHIN GROUP (ORDER BY score)::numeric, 2) AS pct_rank
FROM osa_scores;

-- ============================================================================
-- 20. cume_dist(): hypothetical cumulative distribution
-- ============================================================================

-- begin-expected
-- columns: cum_dist
-- row: 1.00
-- end-expected
SELECT round(cume_dist(100) WITHIN GROUP (ORDER BY score)::numeric, 2) AS cum_dist
FROM osa_scores;

-- ============================================================================
-- 21. Hypothetical-set with GROUP BY
-- ============================================================================

CREATE TABLE osa_hyp_groups (dept text, salary integer);
INSERT INTO osa_hyp_groups VALUES
  ('eng', 80), ('eng', 90), ('eng', 100),
  ('sales', 60), ('sales', 70), ('sales', 80);

-- begin-expected
-- columns: dept, hyp_rank
-- row: eng, 2
-- row: sales, 4
-- end-expected
SELECT dept,
  rank(85) WITHIN GROUP (ORDER BY salary) AS hyp_rank
FROM osa_hyp_groups
GROUP BY dept
ORDER BY dept;

DROP TABLE osa_hyp_groups;

-- ============================================================================
-- SECTION E: Edge cases
-- ============================================================================

-- ============================================================================
-- 22. Ordered-set aggregate on empty input
-- ============================================================================

CREATE TABLE osa_empty (val integer);

-- begin-expected
-- columns: p50
-- row: NULL
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY val) AS p50
FROM osa_empty;

-- begin-expected
-- columns: disc_p50
-- row: NULL
-- end-expected
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY val) AS disc_p50
FROM osa_empty;

-- begin-expected
-- columns: mode_val
-- row: NULL
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY val) AS mode_val
FROM osa_empty;

DROP TABLE osa_empty;

-- ============================================================================
-- 23. percentile_cont with NULLs (NULLs are ignored)
-- ============================================================================

-- begin-expected
-- columns: p50
-- row: 20
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x)::integer AS p50
FROM (VALUES (10), (NULL::integer), (20), (NULL::integer), (30)) AS t(x);

-- ============================================================================
-- 24. Invalid percentile value (out of 0..1 range)
-- ============================================================================

-- begin-expected-error
-- message-like: percentile
-- end-expected-error
SELECT percentile_cont(1.5) WITHIN GROUP (ORDER BY val) FROM osa_data;

-- begin-expected-error
-- message-like: percentile
-- end-expected-error
SELECT percentile_cont(-0.1) WITHIN GROUP (ORDER BY val) FROM osa_data;

-- ============================================================================
-- 25. Ordered-set with FILTER clause
-- ============================================================================

-- begin-expected-error
-- message-like: FILTER
-- end-expected-error
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY val)::integer
  FILTER (WHERE grp = 'a') AS p50_a
FROM osa_data;

-- ============================================================================
-- 26. Combining ordered-set and regular aggregates
-- ============================================================================

-- begin-expected
-- columns: avg_val, median_val, mode_val
-- row: 30, 30, 10
-- end-expected
SELECT
  avg(val)::integer AS avg_val,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY val)::integer AS median_val,
  mode() WITHIN GROUP (ORDER BY val)::integer AS mode_val
FROM osa_data WHERE grp = 'a';

-- ============================================================================
-- 27. percentile_cont with DESC order
-- ============================================================================

-- begin-expected
-- columns: p25_desc
-- row: 40
-- end-expected
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY val DESC)::integer AS p25_desc
FROM osa_data WHERE grp = 'a';

-- ============================================================================
-- 28. Hypothetical rank of existing value
-- ============================================================================

-- note: rank of 92 in scores: there are already two 92s
-- begin-expected
-- columns: hyp_rank
-- row: 6
-- end-expected
SELECT rank(92) WITHIN GROUP (ORDER BY score) AS hyp_rank
FROM osa_scores;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA osa_test CASCADE;
SET search_path = public;
