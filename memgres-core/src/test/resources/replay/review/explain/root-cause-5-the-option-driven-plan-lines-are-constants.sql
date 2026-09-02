-- source: review-2026-08.md
-- finding: Root cause 5: the option-driven plan lines are constants appended after the whole plan
-- area: EXPLAIN
-- title: Root cause 5: the option-driven plan lines are constants appended after the whole plan
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_rr" does not exist
-- end-expected-error
INSERT INTO zz_rr SELECT g, g FROM generate_series(1,10) g;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_rr" does not exist
-- end-expected-error
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT * FROM zz_rr WHERE id = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_bu" does not exist
-- end-expected-error
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM zz_bu;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_bu" does not exist
-- end-expected-error
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM zz_bu;
-- begin-expected
-- ok: 0
-- end-expected
SET work_mem = '5555kB';
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result
-- row: Settings: work_mem = '5555kB'
-- rowcount: 2
-- end-expected
EXPLAIN (COSTS OFF, SETTINGS) SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result
-- row: Planning:
-- row:   Memory: used=6kB  allocated=8kB
-- rowcount: 3
-- end-expected
EXPLAIN (COSTS OFF, MEMORY) SELECT 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_se" does not exist
-- end-expected-error
EXPLAIN (ANALYZE, SERIALIZE BINARY, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM zz_se;
