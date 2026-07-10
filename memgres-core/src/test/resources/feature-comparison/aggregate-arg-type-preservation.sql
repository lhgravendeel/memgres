-- A scalar function/expression wrapping aggregate results must compare/operate on their
-- runtime (typed) values, not a re-stringified representation.
--
-- Regression coverage for mtask-8 Group 5: SelectAggregateEvaluator substituted resolved
-- aggregate results as string literals when a non-aggregate function had aggregate arguments,
-- so LEAST(SUM(a), SUM(b)) compared its arguments lexicographically: LEAST('7.0', '10.0')
-- returned '10.0' (bigger) because '1' < '7' character-wise -- silently the wrong (larger) value.

-- setup
CREATE TABLE agg_type_t (grp int, v numeric);
INSERT INTO agg_type_t VALUES (1, 3), (1, 4), (1, 10);

-- stmt 1: LEAST over two aggregate FILTER sums compares numerically (7.0), not lexicographically
-- begin-expected
-- columns: m
-- row: 7.0
-- end-expected
SELECT LEAST(SUM(v) FILTER (WHERE v < 10), SUM(v) FILTER (WHERE v >= 10)) AS m
FROM agg_type_t WHERE grp = 1;

-- stmt 2: GREATEST likewise
-- begin-expected
-- columns: m
-- row: 10.0
-- end-expected
SELECT GREATEST(SUM(v) FILTER (WHERE v < 10), SUM(v) FILTER (WHERE v >= 10)) AS m
FROM agg_type_t WHERE grp = 1;

-- cleanup
DROP TABLE agg_type_t;
