DROP SCHEMA IF EXISTS test_1240 CASCADE;
CREATE SCHEMA test_1240;
SET search_path TO test_1240;

CREATE TABLE samples (
    x numeric NOT NULL,
    y numeric NOT NULL
);

INSERT INTO samples VALUES
(1,2),
(2,4),
(3,6),
(4,8);

-- begin-expected
-- columns: corr_value
-- row: 1.000000000000000
-- end-expected
SELECT corr(x, y) AS corr_value
FROM samples;

-- begin-expected
-- columns: slope,intercept,r2
-- row: 2.0000000000000000|0.000000000000000|1.000000000000000
-- end-expected
SELECT regr_slope(y, x) AS slope,
       regr_intercept(y, x) AS intercept,
       regr_r2(y, x) AS r2
FROM samples;

-- begin-expected
-- columns: stddev_pop,var_pop
-- row: 1.118033988749895|1.25
-- end-expected
SELECT stddev_pop(x) AS stddev_pop,
       var_pop(x) AS var_pop
FROM samples;

-- begin-expected
-- columns: median
-- row: 2.5
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) AS median
FROM samples;

-- begin-expected
-- columns: modal_value
-- row: 1
-- end-expected
SELECT mode() WITHIN GROUP (ORDER BY x) AS modal_value
FROM (VALUES (1),(1),(2),(3)) v(x);

