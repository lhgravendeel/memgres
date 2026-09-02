-- source: review-2026-08.md
-- finding: Root cause 1: EXPLAIN ANALYZE hands the raw statement to the executor with no gate on what may be run
-- area: EXPLAIN
-- title: Root cause 1: EXPLAIN ANALYZE hands the raw statement to the executor with no gate on what may be run
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_x (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_x VALUES (1),(2),(3);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "TRUNCATE"
-- end-expected-error
EXPLAIN (ANALYZE) TRUNCATE zz_x;
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_x;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result (actual time=0.001..0.001 rows=1.00 loops=1)
-- row: Planning Time: 0.006 ms
-- row: Execution Time: 0.006 ms
-- rowcount: 3
-- end-expected
EXPLAIN (ANALYZE, COSTS OFF) DECLARE zz_cur CURSOR FOR SELECT 1;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_cursors WHERE name = 'zz_cur';
