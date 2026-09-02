-- source: review-2026-08.md
-- finding: Root cause 3: a hard 10000-row cap in generate_series
-- area: Transactions, sessions, cursors and locks
-- title: Root cause 3: a hard 10000-row cap in generate_series
-- begin-expected
-- columns: count:int8
-- row: 18264
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (SELECT generate_series('2000-01-01'::timestamp, '2050-01-01'::timestamp, '1 day'::interval) AS g) t;
-- begin-expected
-- columns: count:int8
-- row: 20000
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (SELECT generate_series(1::numeric, 20000::numeric, 1::numeric) AS g) t;
