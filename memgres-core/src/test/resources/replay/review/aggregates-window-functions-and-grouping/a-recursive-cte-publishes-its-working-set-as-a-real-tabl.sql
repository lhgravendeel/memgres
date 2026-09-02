-- source: review-2026-08.md
-- finding: A recursive CTE publishes its working set as a real table in the shared schema
-- area: Aggregates, window functions and grouping
-- title: A recursive CTE publishes its working set as a real table in the shared schema
-- session A
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_users (n int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_users VALUES (999);
-- session B, concurrently
-- begin-expected
-- columns: count:int8
-- row: 20000
-- rowcount: 1
-- end-expected
WITH RECURSIVE zz_vf_users(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM zz_vf_users WHERE n < 20000)
SELECT count(*) FROM zz_vf_users;
-- session A, in a loop while B runs
-- begin-expected
-- columns: n:int4
-- row: 999
-- rowcount: 1
-- end-expected
SELECT n FROM zz_vf_users;
