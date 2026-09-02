-- source: review-2026-08.md
-- finding: Root cause 9: the option list is a hand-written token loop, not a DefElem list run through defGetBoolean
-- area: EXPLAIN
-- title: Root cause 9: the option list is a hand-written token loop, not a DefElem list run through defGetBoolean
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result  (cost=0.00..0.01 rows=1 width=4)
-- rowcount: 1
-- end-expected
EXPLAIN (COSTS 1) SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result
-- rowcount: 1
-- end-expected
EXPLAIN (COSTS 0) SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result
-- row:   Output: 1
-- rowcount: 2
-- end-expected
EXPLAIN (VERBOSE 1, COSTS OFF) SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result
-- rowcount: 1
-- end-expected
EXPLAIN (COSTS 'off') SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result  (cost=0.00..0.01 rows=1 width=4)
-- rowcount: 1
-- end-expected
EXPLAIN (VERBOSE 'false') SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result
-- rowcount: 1
-- end-expected
EXPLAIN (COSTS OFF, SERIALIZE NONE) SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result (actual rows=1.00 loops=1)
-- row: Serialization: output=1kB  format=text
-- rowcount: 2
-- end-expected
EXPLAIN (ANALYZE, SERIALIZE 'text', COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
EXPLAIN () SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
EXPLAIN (,) SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
EXPLAIN (COSTS OFF,) SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized EXPLAIN option "COSTS"
-- end-expected-error
EXPLAIN ("COSTS" OFF) SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'costs'"
-- end-expected-error
EXPLAIN ('costs' OFF) SELECT 1;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized value for EXPLAIN option "format": "JSON"
-- end-expected-error
EXPLAIN (COSTS OFF, FORMAT "JSON") SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result  (cost=0.00..0.01 rows=1 width=4) (actual time=0.001..0.001 rows=1.00 loops=1)
-- row: Planning Time: 0.007 ms
-- row: Execution Time: 0.005 ms
-- rowcount: 3
-- end-expected
EXPLAIN ANALYSE SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result  (cost=0.00..0.01 rows=1 width=4) (actual time=0.001..0.001 rows=1.00 loops=1)
-- row:   Output: 1
-- row: Planning Time: 0.005 ms
-- row: Execution Time: 0.004 ms
-- rowcount: 4
-- end-expected
EXPLAIN ANALYSE VERBOSE SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result  (cost=0.00..0.01 rows=1 width=4) (actual time=0.000..0.001 rows=1.00 loops=1)
-- row: Planning Time: 0.003 ms
-- row: Execution Time: 0.002 ms
-- rowcount: 3
-- end-expected
EXPLAIN ANALYSE (SELECT 1);
