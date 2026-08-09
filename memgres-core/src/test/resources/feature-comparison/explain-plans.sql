DROP TABLE IF EXISTS zz_ep CASCADE;

CREATE TABLE zz_ep (id int, v int, t text);

INSERT INTO zz_ep VALUES (1,1,'a'),(2,2,'b'),(3,3,'c');

-- begin-expected
-- columns: QUERY PLAN
-- row: Result
-- end-expected
EXPLAIN (COSTS OFF) SELECT 1;

-- begin-expected
-- columns: QUERY PLAN
-- row: Seq Scan on zz_ep
-- end-expected
EXPLAIN (COSTS OFF) SELECT * FROM zz_ep;

-- begin-expected
-- columns: QUERY PLAN
-- row: Seq Scan on zz_ep
-- row:   Filter: (v = 1)
-- end-expected
EXPLAIN (COSTS OFF) SELECT * FROM zz_ep WHERE v = 1;

-- begin-expected
-- columns: QUERY PLAN
-- row: Sort
-- row:   Sort Key: v
-- row:   ->  Seq Scan on zz_ep
-- end-expected
EXPLAIN (COSTS OFF) SELECT * FROM zz_ep ORDER BY v;

-- begin-expected
-- columns: QUERY PLAN
-- row: Aggregate
-- row:   ->  Seq Scan on zz_ep
-- end-expected
EXPLAIN (COSTS OFF) SELECT count(*) FROM zz_ep;

-- begin-expected
-- columns: QUERY PLAN
-- row: HashAggregate
-- row:   Group Key: v
-- row:   ->  Seq Scan on zz_ep
-- end-expected
EXPLAIN (COSTS OFF) SELECT v, count(*) FROM zz_ep GROUP BY v;

-- begin-expected
-- columns: QUERY PLAN
-- row: HashAggregate
-- row:   Group Key: v
-- row:   Filter: (count(*) > 1)
-- row:   ->  Seq Scan on zz_ep
-- end-expected
EXPLAIN (COSTS OFF) SELECT v, count(*) FROM zz_ep GROUP BY v HAVING count(*) > 1;

-- begin-expected
-- columns: QUERY PLAN
-- row: Limit
-- row:   ->  Seq Scan on zz_ep
-- end-expected
EXPLAIN (COSTS OFF) SELECT * FROM zz_ep LIMIT 2;

-- begin-expected
-- columns: QUERY PLAN
-- row: Limit
-- row:   ->  Sort
-- row:         Sort Key: v
-- row:         ->  Seq Scan on zz_ep
-- end-expected
EXPLAIN (COSTS OFF) SELECT * FROM zz_ep ORDER BY v LIMIT 2;

-- begin-expected
-- columns: QUERY PLAN
-- row: Result
-- row:   One-Time Filter: false
-- end-expected
EXPLAIN (COSTS OFF) SELECT * FROM zz_ep WHERE false;

-- begin-expected
-- columns: QUERY PLAN
-- row: Result
-- row:   One-Time Filter: false
-- end-expected
EXPLAIN (COSTS OFF) SELECT 1 WHERE false;

-- begin-expected
-- columns: QUERY PLAN
-- row: Values Scan on "*VALUES*"
-- end-expected
EXPLAIN (COSTS OFF) VALUES (1),(2);

-- begin-expected
-- columns: QUERY PLAN
-- row: Append
-- row:   ->  Result
-- row:   ->  Result
-- end-expected
EXPLAIN (COSTS OFF) SELECT 1 UNION ALL SELECT 2;

-- begin-expected
-- columns: QUERY PLAN
-- row: Unique
-- row:   ->  Sort
-- row:         Sort Key: (1)
-- row:         ->  Append
-- row:               ->  Result
-- row:               ->  Result
-- end-expected
EXPLAIN (COSTS OFF) SELECT 1 UNION SELECT 2;

-- begin-expected
-- columns: QUERY PLAN
-- row: SetOp Except
-- row:   ->  Result
-- row:   ->  Result
-- end-expected
EXPLAIN (COSTS OFF) SELECT 1 EXCEPT SELECT 2;

-- begin-expected
-- columns: QUERY PLAN
-- row: SetOp Intersect
-- row:   ->  Result
-- row:   ->  Result
-- end-expected
EXPLAIN (COSTS OFF) SELECT 1 INTERSECT SELECT 2;

-- begin-expected
-- columns: QUERY PLAN
-- row: CTE Scan on c
-- row:   CTE c
-- row:     ->  Result
-- end-expected
EXPLAIN (COSTS OFF) WITH c AS MATERIALIZED (SELECT 1 AS a) SELECT * FROM c;

-- begin-expected
-- columns: QUERY PLAN
-- row: Insert on zz_ep
-- row:   ->  Result
-- end-expected
EXPLAIN (COSTS OFF) INSERT INTO zz_ep VALUES (9,9,'z');

-- begin-expected
-- columns: QUERY PLAN
-- row: Update on zz_ep
-- row:   ->  Seq Scan on zz_ep
-- row:         Filter: (id = 2)
-- end-expected
EXPLAIN (COSTS OFF) UPDATE zz_ep SET v = 1 WHERE id = 2;

-- begin-expected
-- columns: QUERY PLAN
-- row: Delete on zz_ep
-- row:   ->  Seq Scan on zz_ep
-- row:         Filter: (id = 3)
-- end-expected
EXPLAIN (COSTS OFF) DELETE FROM zz_ep WHERE id = 3;

-- begin-expected
-- columns: QUERY PLAN
-- row: Result
-- row:   Output: 2
-- end-expected
EXPLAIN (COSTS OFF, VERBOSE) SELECT 1+1;

-- begin-expected
-- columns: QUERY PLAN
-- row: Result  (cost=0.00..0.01 rows=1 width=4)
-- end-expected
EXPLAIN (COSTS 1) SELECT 1;

-- begin-expected
-- columns: QUERY PLAN
-- row: Result
-- end-expected
EXPLAIN (COSTS 0) SELECT 1;

-- begin-expected
-- columns: QUERY PLAN
-- row: Result
-- end-expected
EXPLAIN (COSTS 'off') SELECT 1;

-- begin-expected
-- columns: QUERY PLAN
-- row: Result
-- row:   Output: 1
-- end-expected
EXPLAIN (VERBOSE 1, COSTS OFF) SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: costs requires a Boolean value
-- end-expected-error
EXPLAIN (COSTS yes) SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: costs requires a Boolean value
-- end-expected-error
EXPLAIN (COSTS 2) SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: verbose requires a Boolean value
-- end-expected-error
EXPLAIN (VERBOSE bogus) SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: unrecognized EXPLAIN option "bogusopt"
-- end-expected-error
EXPLAIN (BOGUSOPT) SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: unrecognized EXPLAIN option "COSTS"
-- end-expected-error
EXPLAIN ("COSTS" OFF) SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "'costs'"
-- end-expected-error
EXPLAIN ('costs' OFF) SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: format requires a parameter
-- end-expected-error
EXPLAIN (COSTS OFF, FORMAT) SELECT 1;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: unrecognized value for EXPLAIN option "format": "bogus"
-- end-expected-error
EXPLAIN (COSTS OFF, FORMAT BOGUS) SELECT 1;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: unrecognized value for EXPLAIN option "format": "JSON"
-- end-expected-error
EXPLAIN (COSTS OFF, FORMAT "JSON") SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near ")"
-- end-expected-error
EXPLAIN () SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near ","
-- end-expected-error
EXPLAIN (,) SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near ")"
-- end-expected-error
EXPLAIN (COSTS OFF,) SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "COSTS"
-- end-expected-error
EXPLAIN ANALYZE (COSTS OFF) SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "ANALYZE"
-- end-expected-error
EXPLAIN VERBOSE ANALYZE SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "EXPLAIN"
-- end-expected-error
EXPLAIN EXPLAIN SELECT 1;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: EXPLAIN option SERIALIZE requires ANALYZE
-- end-expected-error
EXPLAIN (SERIALIZE) SELECT 1;

-- begin-expected
-- columns: QUERY PLAN
-- row: Result
-- end-expected
EXPLAIN (COSTS OFF, SERIALIZE NONE) SELECT 1;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: unrecognized value for EXPLAIN option "serialize": "bogus"
-- end-expected-error
EXPLAIN (SERIALIZE bogus) SELECT 1;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: EXPLAIN option TIMING requires ANALYZE
-- end-expected-error
EXPLAIN (TIMING) SELECT 1;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: EXPLAIN option TIMING requires ANALYZE
-- end-expected-error
EXPLAIN (COSTS OFF, TIMING ON) SELECT 1;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: EXPLAIN option WAL requires ANALYZE
-- end-expected-error
EXPLAIN (WAL) SELECT 1;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: EXPLAIN options ANALYZE and GENERIC_PLAN cannot be used together
-- end-expected-error
EXPLAIN (ANALYZE, GENERIC_PLAN) SELECT 1;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: EXPLAIN options ANALYZE and GENERIC_PLAN cannot be used together
-- end-expected-error
EXPLAIN (GENERIC_PLAN, ANALYZE) SELECT 1;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: ERROR: relation "zz_nosuch" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_nosuch;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: ERROR: relation "zz_nosuch" does not exist
-- end-expected-error
EXPLAIN (BOGUSOPT) SELECT * FROM zz_nosuch;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "nosuchcol" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT nosuchcol FROM zz_ep;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "int"
-- end-expected-error
EXPLAIN CREATE TABLE zz_nope (x int);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "DROP"
-- end-expected-error
EXPLAIN DROP TABLE zz_ep;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "SET"
-- end-expected-error
EXPLAIN SET work_mem = '4MB';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "CHECKPOINT"
-- end-expected-error
EXPLAIN CHECKPOINT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "DO"
-- end-expected-error
EXPLAIN DO $$ BEGIN NULL; END $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "GRANT"
-- end-expected-error
EXPLAIN GRANT SELECT ON zz_ep TO PUBLIC;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "COPY"
-- end-expected-error
EXPLAIN COPY zz_ep TO STDOUT;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "VIEW"
-- end-expected-error
EXPLAIN CREATE VIEW zz_v9 AS SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "ALTER"
-- end-expected-error
EXPLAIN ALTER TABLE zz_ep ADD COLUMN q int;

-- begin-expected
-- columns: QUERY PLAN
-- row: Result
-- end-expected
EXPLAIN (COSTS OFF) DECLARE zz_c9 CURSOR FOR SELECT 1;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_cursors WHERE name = 'zz_c9';

-- begin-expected
-- columns: QUERY PLAN
-- row: Result
-- end-expected
EXPLAIN (COSTS OFF) CREATE TABLE zz_ct9 AS SELECT 1;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_tables WHERE tablename = 'zz_ct9';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "RETURNING"
-- end-expected-error
CALL zz_nosuchproc() RETURNING 1;

DROP TABLE zz_ep;

