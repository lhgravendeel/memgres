-- source: investigation-2026-08.md
-- finding: 320
-- title: The option-driven plan lines are constants appended after the whole plan — appendExplainExtras writes five fixed strings after buildPlanLines has already added 
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
