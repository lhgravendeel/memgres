-- source: review-2026-08.md
-- finding: Root cause 17: the DROP paths for operators, functions and aggregates ignore dependencies, extra names and argument-list syntax
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 17: the DROP paths for operators, functions and aggregates ignore dependencies, extra names and argument-list syntax
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_add(int,int) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT $1+$2 $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR ###! (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_add);
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop function zz_add(integer,integer) because other objects depend on it
-- end-expected-error
DROP FUNCTION zz_add(int, int);
-- begin-expected
-- columns: ?column?:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT 1 ###! 2;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer ###= integer
-- end-expected-error
DROP OPERATOR ###= (int, int), ###! (int, int);
-- begin-expected
-- columns: count:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_operator WHERE oprname IN ('###=','###!');
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
