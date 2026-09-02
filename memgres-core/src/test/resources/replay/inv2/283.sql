-- source: investigation-2026-08.md
-- finding: 283
-- title: PgWireValueFormatter is a second, divergent renderer of PostgreSQL's output syntax rather than a caller of the engine's own text cast: the array branch quotes o
-- begin-expected
-- ok: 0
-- end-expected
SET TIME ZONE 'UTC';
-- begin-expected
-- columns: array:_timestamp
-- row: {"2020-01-02 03:04:05",NULL}
-- rowcount: 1
-- end-expected
SELECT ARRAY['2020-01-02 03:04:05'::timestamp, NULL];
-- begin-expected
-- columns: array:_interval
-- row: {"1 day 02:00:00"}
-- rowcount: 1
-- end-expected
SELECT ARRAY['1 day 2 hours'::interval];
-- begin-expected
-- columns: array:_timestamptz
-- row: {"2020-01-02 03:04:05+00"}
-- rowcount: 1
-- end-expected
SELECT ARRAY['2020-01-02 03:04:05+00'::timestamptz];
-- begin-expected
-- columns: array_agg:_timestamp
-- row: {"2020-01-02 03:04:05"}
-- rowcount: 1
-- end-expected
SELECT array_agg(x) FROM (SELECT '2020-01-02 03:04:05'::timestamp AS x) s;
-- begin-expected
-- ok: 0
-- end-expected
SET TIME ZONE 'Africa/Abidjan';
-- begin-expected
-- columns: timestamptz:timestamptz
-- row: 1899-12-31 23:43:52-00:16:08
-- rowcount: 1
-- end-expected
SELECT '1900-01-01 00:00:00+00'::timestamptz;
-- begin-expected
-- ok: 0
-- end-expected
SET TIME ZONE 'America/Sao_Paulo';
-- begin-expected
-- columns: timestamptz:timestamptz
-- row: 1799-12-31 20:53:32-03:06:28
-- rowcount: 1
-- end-expected
SELECT '1800-01-01 00:00:00+00'::timestamptz;
