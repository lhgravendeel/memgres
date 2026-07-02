-- generate_series with timestamp, timestamptz, and date arguments

-- stmt 1: typed DATE with interval
-- begin-expected
-- columns: gs
-- row: 2026-01-01 00:00:00+00
-- row: 2026-01-02 00:00:00+00
-- row: 2026-01-03 00:00:00+00
-- row: 2026-01-04 00:00:00+00
-- row: 2026-01-05 00:00:00+00
-- end-expected
SELECT gs FROM generate_series(DATE '2026-01-01', DATE '2026-01-05', INTERVAL '1 day') AS gs;

-- stmt 2: typed TIMESTAMP with interval
-- begin-expected
-- columns: gs
-- row: 2026-01-01 00:00:00
-- row: 2026-01-01 01:00:00
-- row: 2026-01-01 02:00:00
-- row: 2026-01-01 03:00:00
-- end-expected
SELECT gs FROM generate_series(TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 03:00:00', INTERVAL '1 hour') AS gs;

-- stmt 3: descending DATE series
-- begin-expected
-- columns: gs
-- row: 2026-01-05 00:00:00+00
-- row: 2026-01-04 00:00:00+00
-- row: 2026-01-03 00:00:00+00
-- row: 2026-01-02 00:00:00+00
-- row: 2026-01-01 00:00:00+00
-- end-expected
SELECT gs FROM generate_series(DATE '2026-01-05', DATE '2026-01-01', INTERVAL '-1 day') AS gs;

-- stmt 4: descending TIMESTAMP series
-- begin-expected
-- columns: gs
-- row: 2026-01-01 03:00:00
-- row: 2026-01-01 02:00:00
-- row: 2026-01-01 01:00:00
-- row: 2026-01-01 00:00:00
-- end-expected
SELECT gs FROM generate_series(TIMESTAMP '2026-01-01 03:00:00', TIMESTAMP '2026-01-01 00:00:00', INTERVAL '-1 hour') AS gs;

-- stmt 5: count of now()-based series (60 minutes + 1 = 61 rows)
-- begin-expected
-- columns: count
-- row: 61
-- end-expected
SELECT count(*) FROM generate_series(now() - interval '1 hour', now(), interval '1 minute') AS gs;

-- stmt 6: integer baseline still works
-- begin-expected
-- columns: generate_series
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- row: 5
-- end-expected
SELECT generate_series(1, 5);
