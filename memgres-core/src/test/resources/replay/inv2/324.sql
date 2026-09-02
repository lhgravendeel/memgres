-- source: investigation-2026-08.md
-- finding: 324
-- title: The EXPLAIN option list is a hand-written token loop, not a DefElem list run through defGetBoolean/defGetString: it switches on the upper-cased token value rega
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
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "COSTS"
-- end-expected-error
EXPLAIN ANALYZE (COSTS OFF) SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "EXPLAIN"
-- end-expected-error
EXPLAIN EXPLAIN SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "EXPLAIN"
-- end-expected-error
EXPLAIN (COSTS OFF) EXPLAIN (COSTS OFF) SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "EXPLAIN"
-- end-expected-error
CREATE TABLE zz_bad AS EXPLAIN SELECT 1;
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
