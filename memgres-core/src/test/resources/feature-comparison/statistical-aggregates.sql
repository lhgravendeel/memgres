-- ============================================================================
-- Feature Comparison: Statistical Aggregate Functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests correlation and regression aggregate functions: corr, covar_pop,
-- covar_samp, regr_avgx, regr_avgy, regr_count, regr_intercept,
-- regr_r2, regr_slope, regr_sxx, regr_syy, regr_sxy.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS stat_test CASCADE;
CREATE SCHEMA stat_test;
SET search_path = stat_test, public;

-- Perfect linear relationship: y = 2x
CREATE TABLE stat_linear (x double precision, y double precision);
INSERT INTO stat_linear VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10);

-- Data with noise
CREATE TABLE stat_noisy (x double precision, y double precision);
INSERT INTO stat_noisy VALUES
  (1, 2.1), (2, 3.8), (3, 6.2), (4, 7.9), (5, 10.1);

-- Data with NULLs
CREATE TABLE stat_nulls (x double precision, y double precision);
INSERT INTO stat_nulls VALUES
  (1, 2), (2, NULL), (3, 6), (NULL, 8), (5, 10);

-- ============================================================================
-- SECTION A: Covariance
-- ============================================================================

-- ============================================================================
-- 1. covar_pop: population covariance (perfect linear)
-- ============================================================================

-- begin-expected
-- columns: cov
-- row: 4.00
-- end-expected
SELECT round(covar_pop(y, x)::numeric, 2) AS cov FROM stat_linear;

-- ============================================================================
-- 2. covar_samp: sample covariance (perfect linear)
-- ============================================================================

-- begin-expected
-- columns: cov
-- row: 5.00
-- end-expected
SELECT round(covar_samp(y, x)::numeric, 2) AS cov FROM stat_linear;

-- ============================================================================
-- 3. Covariance with NULLs (NULLs are excluded)
-- ============================================================================

-- note: Only rows where both x and y are non-null: (1,2), (3,6), (5,10)
-- begin-expected
-- columns: cov_pop, cov_samp
-- row: 5.33, 8.00
-- end-expected
SELECT
  round(covar_pop(y, x)::numeric, 2) AS cov_pop,
  round(covar_samp(y, x)::numeric, 2) AS cov_samp
FROM stat_nulls;

-- ============================================================================
-- SECTION B: Correlation
-- ============================================================================

-- ============================================================================
-- 4. corr: perfect positive correlation
-- ============================================================================

-- begin-expected
-- columns: r
-- row: 1.00
-- end-expected
SELECT round(corr(y, x)::numeric, 2) AS r FROM stat_linear;

-- ============================================================================
-- 5. corr: near-perfect correlation (noisy data)
-- ============================================================================

-- begin-expected
-- columns: r_above_099
-- row: true
-- end-expected
SELECT round(corr(y, x)::numeric, 4) > 0.99 AS r_above_099 FROM stat_noisy;

-- ============================================================================
-- 6. corr: negative correlation
-- ============================================================================

-- begin-expected
-- columns: r
-- row: -1.00
-- end-expected
SELECT round(corr(y, x)::numeric, 2) AS r
FROM (VALUES (1, 10::double precision), (2, 8::double precision),
             (3, 6::double precision), (4, 4::double precision),
             (5, 2::double precision)) AS t(x, y);

-- ============================================================================
-- SECTION C: Regression functions
-- ============================================================================

-- ============================================================================
-- 7. regr_count: number of valid pairs
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 5
-- end-expected
SELECT regr_count(y, x) AS cnt FROM stat_linear;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT regr_count(y, x) AS cnt FROM stat_nulls;

-- ============================================================================
-- 8. regr_avgx, regr_avgy: average of x and y
-- ============================================================================

-- begin-expected
-- columns: avg_x, avg_y
-- row: 3.00, 6.00
-- end-expected
SELECT
  round(regr_avgx(y, x)::numeric, 2) AS avg_x,
  round(regr_avgy(y, x)::numeric, 2) AS avg_y
FROM stat_linear;

-- ============================================================================
-- 9. regr_slope: slope of least-squares regression
-- ============================================================================

-- begin-expected
-- columns: slope
-- row: 2.00
-- end-expected
SELECT round(regr_slope(y, x)::numeric, 2) AS slope FROM stat_linear;

-- ============================================================================
-- 10. regr_intercept: y-intercept of regression line
-- ============================================================================

-- begin-expected
-- columns: intercept
-- row: 0.00
-- end-expected
SELECT round(regr_intercept(y, x)::numeric, 2) AS intercept FROM stat_linear;

-- ============================================================================
-- 11. regr_r2: R-squared (coefficient of determination)
-- ============================================================================

-- begin-expected
-- columns: r2
-- row: 1.00
-- end-expected
SELECT round(regr_r2(y, x)::numeric, 2) AS r2 FROM stat_linear;

-- ============================================================================
-- 12. regr_sxx, regr_syy, regr_sxy: regression sum-of-squares
-- ============================================================================

-- begin-expected
-- columns: sxx, syy, sxy
-- row: 10.00, 40.00, 20.00
-- end-expected
SELECT
  round(regr_sxx(y, x)::numeric, 2) AS sxx,
  round(regr_syy(y, x)::numeric, 2) AS syy,
  round(regr_sxy(y, x)::numeric, 2) AS sxy
FROM stat_linear;

-- ============================================================================
-- SECTION D: Edge Cases
-- ============================================================================

-- ============================================================================
-- 13. All aggregates on empty table
-- ============================================================================

CREATE TABLE stat_empty (x double precision, y double precision);

-- begin-expected
-- columns: corr, cov_pop, cnt, slope
-- row: NULL, NULL, 0, NULL
-- end-expected
SELECT
  corr(y, x) AS corr,
  covar_pop(y, x) AS cov_pop,
  regr_count(y, x) AS cnt,
  regr_slope(y, x) AS slope
FROM stat_empty;

DROP TABLE stat_empty;

-- ============================================================================
-- 14. Single-row data
-- ============================================================================

-- begin-expected
-- columns: cov_pop, cov_samp, corr_val
-- row: 0.00, NULL, NULL
-- end-expected
SELECT
  round(covar_pop(y, x)::numeric, 2) AS cov_pop,
  covar_samp(y, x) AS cov_samp,
  corr(y, x) AS corr_val
FROM (VALUES (1::double precision, 2::double precision)) AS t(x, y);

-- ============================================================================
-- 15. All NULL pairs
-- ============================================================================

-- begin-expected
-- columns: cnt, cov_pop
-- row: 0, NULL
-- end-expected
SELECT
  regr_count(y, x) AS cnt,
  covar_pop(y, x) AS cov_pop
FROM (VALUES (NULL::double precision, NULL::double precision),
             (NULL::double precision, NULL::double precision)) AS t(x, y);

-- ============================================================================
-- 16. Regression with GROUP BY
-- ============================================================================

CREATE TABLE stat_groups (grp text, x double precision, y double precision);
INSERT INTO stat_groups VALUES
  ('a', 1, 2), ('a', 2, 4), ('a', 3, 6),
  ('b', 1, 3), ('b', 2, 5), ('b', 3, 7);

-- begin-expected
-- columns: grp, slope, intercept
-- row: a, 2.00, 0.00
-- row: b, 2.00, 1.00
-- end-expected
SELECT grp,
  round(regr_slope(y, x)::numeric, 2) AS slope,
  round(regr_intercept(y, x)::numeric, 2) AS intercept
FROM stat_groups
GROUP BY grp
ORDER BY grp;

-- ============================================================================
-- 17. Constant x values (regr_slope should be NULL)
-- ============================================================================

-- begin-expected
-- columns: slope, r2
-- row: NULL, NULL
-- end-expected
SELECT
  regr_slope(y, x) AS slope,
  regr_r2(y, x) AS r2
FROM (VALUES (1::double precision, 2::double precision),
             (1::double precision, 4::double precision),
             (1::double precision, 6::double precision)) AS t(x, y);

-- ============================================================================
-- 18. var_pop, var_samp, stddev_pop, stddev_samp
-- ============================================================================

-- begin-expected
-- columns: vp, vs, sp, ss
-- row: 2.00, 2.50, 1.41, 1.58
-- end-expected
SELECT
  round(var_pop(x)::numeric, 2) AS vp,
  round(var_samp(x)::numeric, 2) AS vs,
  round(stddev_pop(x)::numeric, 2) AS sp,
  round(stddev_samp(x)::numeric, 2) AS ss
FROM stat_linear;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA stat_test CASCADE;
SET search_path = public;
