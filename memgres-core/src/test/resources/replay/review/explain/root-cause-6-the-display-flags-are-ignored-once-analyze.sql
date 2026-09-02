-- source: review-2026-08.md
-- finding: Root cause 6: the display flags are ignored once ANALYZE is on, and the numbers are formatted with the JVM's default locale
-- area: EXPLAIN
-- title: Root cause 6: the display flags are ignored once ANALYZE is on, and the numbers are formatted with the JVM's default locale
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result (actual rows=1.00 loops=1)
-- rowcount: 1
-- end-expected
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result  (cost=0.00..0.01 rows=1 width=4) (actual time=0.000..0.001 rows=1.00 loops=1)
-- row: Planning Time: 0.004 ms
-- row: Execution Time: 0.002 ms
-- rowcount: 3
-- end-expected
EXPLAIN (ANALYZE) SELECT 1;
