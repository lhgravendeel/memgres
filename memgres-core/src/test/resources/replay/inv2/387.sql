-- source: investigation-2026-08.md
-- finding: 387
-- title: commandTag() maps a small QueryResult.Type enum, so every DROP collapses to DROP TABLE, every ALTER to CREATE TABLE, SHOW/EXPLAIN report SELECT n, FETCH reports
-- begin-expected-error
-- sqlstate: 42704
-- message-like: index "i" does not exist
-- end-expected-error
DROP INDEX i;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ADD COLUMN b int;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t RENAME TO t2;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: view "v" does not exist
-- end-expected-error
DROP VIEW v;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "v" does not exist
-- end-expected-error
ALTER VIEW v RENAME TO w;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: sequence "s" does not exist
-- end-expected-error
DROP SEQUENCE s;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sch" does not exist
-- end-expected-error
DROP SCHEMA sch;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function f() does not exist
-- end-expected-error
DROP FUNCTION f();
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "ty" does not exist
-- end-expected-error
DROP TYPE ty;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "d" does not exist
-- end-expected-error
DROP DOMAIN d;
-- begin-expected
-- columns: work_mem:text
-- row: 4MB
-- rowcount: 1
-- end-expected
SHOW work_mem;
-- begin-expected
-- columns: QUERY PLAN:text
-- row: Result  (cost=0.00..0.01 rows=1 width=4)
-- rowcount: 1
-- end-expected
EXPLAIN SELECT 1;
-- begin-expected
-- ok: 0
-- end-expected
CHECKPOINT;
-- begin-expected
-- ok: 0
-- end-expected
START TRANSACTION;
-- begin-expected
-- ok: 0
-- end-expected
CLOSE ALL;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "cur" does not exist
-- end-expected-error
FETCH 2 FROM cur;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
TRUNCATE t;
