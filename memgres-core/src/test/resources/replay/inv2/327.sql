-- source: investigation-2026-08.md
-- finding: 327
-- title: Unrelated singletons in this area
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
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result  (cost=0.00..0.01 rows=1 width=4) (actual time=0.001..0.001 rows=1.00 loops=1)
-- row: Planning Time: 0.007 ms
-- row: Execution Time: 0.004 ms
-- rowcount: 3
-- end-expected
EXPLAIN (ANALYZE) SELECT 1;
-- simple query protocol
-- begin-expected-error
-- sqlstate: 42P02
-- message-like: there is no parameter $1
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT $1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_g" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_g WHERE id = $1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_g" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF, GENERIC_PLAN) SELECT * FROM zz_g WHERE id = $0;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_g" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF, GENERIC_PLAN) SELECT * FROM zz_g WHERE id = $1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_c" does not exist
-- end-expected-error
WITH d AS (DELETE FROM zz_c WHERE id = 3 RETURNING *) SELECT count(*) FROM d;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_c" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) WITH d AS (DELETE FROM zz_c WHERE id = 3 RETURNING *) SELECT * FROM d;
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
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result  (cost=0.00..0.01 rows=1 width=4) (actual time=0.000..0.000 rows=1.00 loops=1)
-- row: Planning Time: 0.002 ms
-- row: Execution Time: 0.002 ms
-- rowcount: 3
-- end-expected
EXPLAIN ANALYSE SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result  (cost=0.00..0.01 rows=1 width=4) (actual time=0.000..0.000 rows=1.00 loops=1)
-- row:   Output: 1
-- row: Planning Time: 0.001 ms
-- row: Execution Time: 0.001 ms
-- rowcount: 4
-- end-expected
EXPLAIN ANALYSE VERBOSE SELECT 1;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result  (cost=0.00..0.01 rows=1 width=4) (actual time=0.000..0.000 rows=1.00 loops=1)
-- row: Planning Time: 0.001 ms
-- row: Execution Time: 0.001 ms
-- rowcount: 3
-- end-expected
EXPLAIN ANALYSE (SELECT 1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_p() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "RETURNING"
-- end-expected-error
CALL zz_p() RETURNING 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "1"
-- end-expected-error
CALL zz_p() 1;
-- begin-expected-error
-- sqlstate: 25001
-- message-like: SET TRANSACTION ISOLATION LEVEL must be called before any query
-- end-expected-error
DO $$ BEGIN SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; END $$;
