-- source: investigation-2026-08.md
-- finding: 100
-- title: interval and time have no aggregate support: sum/avg have no overload and the ordered-set aggregates interpolate through executor.toDouble(), which answers 0 fo
-- begin-expected
-- columns: percentile_cont:text
-- row: 2 days
-- rowcount: 1
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x)::text FROM (VALUES ('1 day'::interval),('3 days'::interval)) t(x);
-- begin-expected
-- columns: sum:text
-- row: 1 day 02:00:00
-- rowcount: 1
-- end-expected
SELECT sum(v)::text FROM (VALUES (interval '1 day'),(interval '2 hours')) x(v);
-- begin-expected
-- columns: avg:text
-- row: 02:00:00
-- rowcount: 1
-- end-expected
SELECT avg(v)::text FROM (VALUES (time '01:00'),(time '03:00')) x(v);
