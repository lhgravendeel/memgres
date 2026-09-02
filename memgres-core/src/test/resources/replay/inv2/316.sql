-- source: investigation-2026-08.md
-- finding: 316
-- title: EXPLAIN ANALYZE hands the raw statement to the executor with no gate on what may be run — executeExplain does `if (stmt.analyze()) executor.executeStatement(stm
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_x (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_x VALUES (1),(2),(3);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "TRUNCATE"
-- end-expected-error
EXPLAIN (ANALYZE) TRUNCATE zz_x;
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_x;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result (actual time=0.001..0.001 rows=1.00 loops=1)
-- row: Planning Time: 0.006 ms
-- row: Execution Time: 0.005 ms
-- rowcount: 3
-- end-expected
EXPLAIN (ANALYZE, COSTS OFF) DECLARE zz_cur CURSOR FOR SELECT 1;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_cursors WHERE name = 'zz_cur';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_e" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT nosuchcol FROM zz_e;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT nosuchfunc(1);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "nosuchtype" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT 1::nosuchtype;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT 1/0;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT 'abc'::int;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_e" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT id, count(*) FROM zz_e;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_e" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_e ORDER BY 99;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_e" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_e a WHERE b.id = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_e" does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT * FROM zz_e, zz_e;
