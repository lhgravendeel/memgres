-- source: investigation-2026-08.md
-- finding: 108
-- title: WITH scope bookkeeping is ad hoc: the CTE result cache is one flat map keyed by the lowercased bare name whose entries are evicted on push and never restored on
-- begin-expected
-- columns: a:int4 | b:int4 | c:int4
-- row: 1 | 2 | 1
-- rowcount: 1
-- end-expected
WITH x AS (SELECT 1 AS n)
SELECT (SELECT n FROM x) AS a,
       (WITH x AS (SELECT 2 AS n) SELECT n FROM x) AS b,
       (SELECT n FROM x) AS c;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
WITH zz_vf_ghost AS (SELECT 1 AS n) SELECT 1/0 FROM zz_vf_ghost UNION ALL SELECT 1;
-- next statement, same connection:
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ghost" does not exist
-- end-expected-error
SELECT n FROM zz_vf_ghost;
