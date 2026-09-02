-- source: investigation.md
-- finding: 3
-- title: Window frame validation is absent
-- Frames that cannot be ordered (PG: 42P20 each; memgres computes a result):
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ".."
-- end-expected-error
... OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW)
... OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)
... OVER (ORDER BY id ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING)
... OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING)

-- Offset frames without the required ORDER BY (PG: 42P20; memgres computes):
... OVER (RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)             -- needs exactly one ORDER BY col
... OVER (ORDER BY g, v RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)  -- two cols is an error
... OVER (GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)            -- GROUPS needs ORDER BY

-- NULL offset (PG: 22004 frame starting offset must not be null; memgres computes)
... OVER (ORDER BY id ROWS BETWEEN NULL PRECEDING AND CURRENT ROW)

-- DISTINCT inside a window aggregate (PG: 0A000 not implemented; memgres computes a value)
SELECT count(DISTINCT v) OVER (PARTITION BY g) FROM t;
