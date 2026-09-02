-- source: investigation-2026-08.md
-- finding: 325
-- title: Only WAL has a requires-ANALYZE check; no other option pair is validated, so SERIALIZE/TIMING without ANALYZE and ANALYZE together with GENERIC_PLAN are all acc
-- begin-expected-error
-- sqlstate: 22023
-- message-like: EXPLAIN option SERIALIZE requires ANALYZE
-- end-expected-error
EXPLAIN (SERIALIZE) SELECT 1;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: EXPLAIN option SERIALIZE requires ANALYZE
-- end-expected-error
EXPLAIN (COSTS OFF, SERIALIZE) SELECT 1;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: EXPLAIN option TIMING requires ANALYZE
-- end-expected-error
EXPLAIN (COSTS OFF, TIMING ON) SELECT 1;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: EXPLAIN option TIMING requires ANALYZE
-- end-expected-error
EXPLAIN (TIMING) SELECT 1;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: EXPLAIN options ANALYZE and GENERIC_PLAN cannot be used together
-- end-expected-error
EXPLAIN (ANALYZE, GENERIC_PLAN) SELECT 1;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: EXPLAIN options ANALYZE and GENERIC_PLAN cannot be used together
-- end-expected-error
EXPLAIN (GENERIC_PLAN, ANALYZE) SELECT 1;
