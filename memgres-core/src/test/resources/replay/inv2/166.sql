-- source: investigation-2026-08.md
-- finding: 166
-- title: the user-operator arm of pg_operator mints the backing function's OID under "func:" where pg_proc and the built-in arm use "proc:", sets oprcom only for a self-
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_eq(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR ###@ (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_eq, RESTRICT=eqsel, JOIN=eqjoinsel, HASHES, MERGES);
-- begin-expected
-- columns: oprcode:text | oprrest:text | oprjoin:text
-- rowcount: 0
-- end-expected
SELECT oprcode::text, oprrest::text, oprjoin::text FROM pg_operator WHERE oprname='###@';
-- begin-expected
-- columns: oprname:name | oprrest:text | oprjoin:text
-- row: = | eqsel | eqjoinsel
-- rowcount: 1
-- end-expected
SELECT oprname, oprrest::text, oprjoin::text FROM pg_operator
 WHERE oprname='=' AND oprleft='int4'::regtype AND oprright='int4'::regtype;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_lt(integer, text) does not exist
-- end-expected-error
CREATE OPERATOR @#< (LEFTARG=integer, RIGHTARG=text, FUNCTION=zz_lt, COMMUTATOR = @#>);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_gt(text, integer) does not exist
-- end-expected-error
CREATE OPERATOR @#> (LEFTARG=text, RIGHTARG=integer, FUNCTION=zz_gt, COMMUTATOR = @#<);
-- begin-expected
-- columns: oprname:name | oprname:name
-- rowcount: 0
-- end-expected
SELECT a.oprname, b.oprname FROM pg_operator a LEFT JOIN pg_operator b ON b.oid=a.oprcom
 WHERE a.oprname IN ('@#<','@#>') ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_eq(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR ###= (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_eq, NEGATOR = ###!);
-- begin-expected
-- columns: count:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_operator WHERE oprname='###!';
-- begin-expected
-- columns: ?column?:bool
-- rowcount: 0
-- end-expected
SELECT oprnegate <> 0 FROM pg_operator WHERE oprname='###=';
