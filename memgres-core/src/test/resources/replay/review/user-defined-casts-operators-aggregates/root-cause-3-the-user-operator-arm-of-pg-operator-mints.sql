-- source: review-2026-08.md
-- finding: Root cause 3: the user-operator arm of pg_operator mints OIDs from a different key than pg_proc and writes zeroes where the built-in arm writes regprocs
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 3: the user-operator arm of pg_operator mints OIDs from a different key than pg_proc and writes zeroes where the built-in arm writes regprocs
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_eq(int,int) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT ($1%10)=($2%10) $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR ###@ (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_eq, RESTRICT=eqsel, JOIN=eqjoinsel, HASHES, MERGES);
-- begin-expected
-- columns: oprcode:text | oprrest:text | oprjoin:text
-- row: zz_eq | eqsel | eqjoinsel
-- rowcount: 1
-- end-expected
SELECT oprcode::text, oprrest::text, oprjoin::text FROM pg_operator WHERE oprname='###@';
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT (SELECT count(*) FROM pg_proc p WHERE p.oid = o.oprcode) FROM pg_operator o WHERE o.oprname='###@';
-- begin-expected
-- columns: oprname:name | oprrest:text | oprjoin:text
-- row: = | eqsel | eqjoinsel
-- rowcount: 1
-- end-expected
SELECT oprname, oprrest::text, oprjoin::text FROM pg_operator
 WHERE oprname='=' AND oprleft='int4'::regtype AND oprright='int4'::regtype;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_lt(integer,text) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT $1::text < $2 $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_gt(text,integer) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT $1 > $2::text $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR @#< (LEFTARG=integer, RIGHTARG=text, FUNCTION=zz_lt, COMMUTATOR = @#>);
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR @#> (LEFTARG=text, RIGHTARG=integer, FUNCTION=zz_gt, COMMUTATOR = @#<);
-- begin-expected
-- columns: oprname:name | oprname:name
-- row: @#< | @#>
-- row: @#> | @#<
-- rowcount: 2
-- end-expected
SELECT a.oprname, b.oprname FROM pg_operator a LEFT JOIN pg_operator b ON b.oid=a.oprcom
 WHERE a.oprname IN ('@#<','@#>') ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR ###= (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_eq, NEGATOR = ###!);
-- begin-expected
-- columns: count:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_operator WHERE oprname='###!';
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT oprnegate <> 0 FROM pg_operator WHERE oprname='###=';
