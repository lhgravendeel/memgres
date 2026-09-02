-- source: investigation-2026-08.md
-- finding: 180
-- title: the DROP paths for operators, functions and aggregates walk no dependency graph, stop after the first name in a comma list, and do not parse or diagnose a malfo
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer ###= integer
-- end-expected-error
DROP OPERATOR ###= (int, int), ###! (int, int);
-- begin-expected
-- columns: count:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_operator WHERE oprname IN ('###=','###!');
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_add(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR ###! (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_add);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_add(integer, integer) does not exist
-- end-expected-error
DROP FUNCTION zz_add(int, int);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer ###! integer
-- end-expected-error
SELECT 1 ###! 2;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_sum(integer, integer) does not exist
-- end-expected-error
CREATE AGGREGATE zz_agg (int) (SFUNC = zz_sum, STYPE = int);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_agg(integer) does not exist
-- end-expected-error
DROP FUNCTION zz_agg(int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: missing argument
-- end-expected-error
DROP OPERATOR ###@ (int);
