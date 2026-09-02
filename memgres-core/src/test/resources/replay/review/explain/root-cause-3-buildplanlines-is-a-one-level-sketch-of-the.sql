-- source: review-2026-08.md
-- finding: Root cause 3: buildPlanLines is a one-level sketch of the raw AST, not a plan printer
-- area: EXPLAIN
-- title: Root cause 3: buildPlanLines is a one-level sketch of the raw AST, not a plan printer
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_a" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT count(*) FROM zz_a;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_a" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_a ORDER BY v;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_a" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_a LIMIT 5;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_a" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) UPDATE zz_a SET v = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_a" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_a, zz_b;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Unique
-- row:   ->  Sort
-- row:         Sort Key: (1)
-- row:         ->  Append
-- row:               ->  Result
-- row:               ->  Result
-- rowcount: 6
-- end-expected
EXPLAIN (COSTS OFF) SELECT 1 UNION SELECT 2;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Append
-- row:   ->  Result
-- row:   ->  Result
-- rowcount: 3
-- end-expected
EXPLAIN (COSTS OFF) SELECT 1 UNION ALL SELECT 2;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_u" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT id FROM zz_u INTERSECT SELECT v FROM zz_u;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_u" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) MERGE INTO zz_u t USING zz_u s ON t.id = s.id WHEN MATCHED THEN DO NOTHING;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result
-- rowcount: 1
-- end-expected
EXPLAIN (COSTS OFF) CREATE TABLE zz_ct AS SELECT 1 AS a;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result
-- rowcount: 1
-- end-expected
EXPLAIN (COSTS OFF) SELECT 1 INTO zz_sel;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_f" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_f WHERE v = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_f" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_f ORDER BY v DESC NULLS LAST;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result
-- row:   Output: 2
-- rowcount: 2
-- end-expected
EXPLAIN (COSTS OFF, VERBOSE) SELECT 1+1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_v" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF, VERBOSE) SELECT id AS x, v*2 AS y FROM zz_v;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_v" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF, VERBOSE) SELECT * FROM zz_v;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_n" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_n a;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_Mx" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM "zz_Mx";
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_nv" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_nv;
-- a view over zz_n
-- begin-expected
-- columns: QUERY PLAN:text
-- row: CTE Scan on c
-- row:   CTE c
-- row:     ->  Result
-- rowcount: 3
-- end-expected
EXPLAIN (COSTS OFF) WITH c AS MATERIALIZED (SELECT 1 AS a) SELECT * FROM c;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_nosuchsch.t" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_nosuchsch.t;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result (actual rows=1.00 loops=1)
-- rowcount: 1
-- end-expected
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_ar" does not exist
-- end-expected-error
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) INSERT INTO zz_ar VALUES (1,1);
