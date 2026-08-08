-- begin-expected
-- columns: a|b|c
-- row: infinity|infinity|infinity
-- end-expected
SELECT (timestamptz 'infinity')::text AS a, (timestamp 'infinity')::text AS b, (date 'infinity')::text AS c;

-- begin-expected
-- columns: a|b
-- row: -infinity|-infinity
-- end-expected
SELECT (timestamptz '-infinity')::text AS a, (date '-infinity')::text AS b;

-- begin-expected
-- columns: a|b|c
-- row: f|f|f
-- end-expected
SELECT isfinite(timestamptz 'infinity') AS a, isfinite(timestamp 'infinity') AS b, isfinite(date 'infinity') AS c;

-- begin-expected
-- columns: a|b
-- row: t|t
-- end-expected
SELECT isfinite(timestamp '2020-01-01') AS a, isfinite(date '2020-01-01') AS b;

-- begin-expected
-- columns: a
-- row: f
-- end-expected
SELECT '9999-12-31 23:59:59'::timestamp = 'infinity'::timestamp AS a;

-- begin-expected
-- columns: a
-- row: f
-- end-expected
SELECT '9999-12-31'::date = 'infinity'::date AS a;

-- begin-expected
-- columns: a
-- row: infinity
-- end-expected
SELECT date 'infinity' + 1 AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT date 'infinity' > date '9999-12-31' AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT date '-infinity' < date '0001-01-01' AS a;

