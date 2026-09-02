-- source: investigation-2026-08.md
-- finding: 321
-- title: The display flags are ignored once ANALYZE is on (`costs || analyze` lets ANALYZE override COSTS OFF, the Planning/Execution Time lines are unconditional, TIMIN
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result (actual rows=1.00 loops=1)
-- rowcount: 1
-- end-expected
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT 1;
