-- source: review-2026-08.md
-- finding: Root cause 8: the EXPLAIN payload is parsed by the general statement parser with no explainable-statement gate
-- area: EXPLAIN
-- title: Root cause 8: the EXPLAIN payload is parsed by the general statement parser with no explainable-statement gate
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "int"
-- end-expected-error
EXPLAIN CREATE TABLE zz_nope (x int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DROP"
-- end-expected-error
EXPLAIN DROP TABLE zz_nope;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SET"
-- end-expected-error
EXPLAIN SET work_mem = '4MB';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "CHECKPOINT"
-- end-expected-error
EXPLAIN CHECKPOINT;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DO"
-- end-expected-error
EXPLAIN DO $$ BEGIN NULL; END $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "GRANT"
-- end-expected-error
EXPLAIN GRANT SELECT ON pg_class TO PUBLIC;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "COPY"
-- end-expected-error
EXPLAIN COPY pg_class TO STDOUT;
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
-- sqlstate: 42P01
-- message-like: relation "zz_c" does not exist
-- end-expected-error
WITH d AS (DELETE FROM zz_c WHERE id = 3 RETURNING *) SELECT count(*) FROM d;
-- works
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_c" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) WITH d AS (DELETE FROM zz_c WHERE id = 3 RETURNING *) SELECT * FROM d;
