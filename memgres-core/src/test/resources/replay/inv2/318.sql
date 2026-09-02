-- source: investigation-2026-08.md
-- finding: 318
-- title: buildPlanLines is a one-level sketch of the raw AST, not a plan printer: it is called once with indent 0 and never recurses (so the `->` prefix is dead code), h
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
-- ok: 0
-- end-expected
PREPARE zz_q (int) AS SELECT $1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: wrong number of parameters for prepared statement "zz_q"
-- end-expected-error
EXPLAIN (COSTS OFF) EXECUTE zz_q;
-- begin-expected-error
-- sqlstate: 26000
-- message-like: prepared statement "zz_absent_prep" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) EXECUTE zz_absent_prep;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
EXPLAIN (COSTS OFF) EXECUTE zz_q();
