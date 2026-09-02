-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Unrelated singletons
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'abc' LIKE ANY (ARRAY['x%','a%']);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'abc' NOT LIKE ALL (ARRAY['x%','y%']);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'abc' ~ ANY (ARRAY['^x','^a']);
-- begin-expected
-- columns: length:int4
-- row: 5
-- rowcount: 1
-- end-expected
SELECT length(btrim(E' \tabc\n '));
-- begin-expected
-- columns: length:int4
-- row: 32
-- rowcount: 1
-- end-expected
SELECT length(sha256('abc'::bytea));
-- begin-expected
-- columns: encode:text
-- row: ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
-- rowcount: 1
-- end-expected
SELECT encode(sha256('abc'::bytea),'hex');
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*) FROM regexp_matches(E'a\nb', '.', 'g');
-- begin-expected
-- columns: string_agg:text
-- row: 1+2
-- rowcount: 1
-- end-expected
SELECT string_agg(v::text, d) FROM (VALUES (1,'-'),(2,'+')) t(v,d);
-- begin-expected
-- columns: substring:text
-- row: abc
-- rowcount: 1
-- end-expected
SELECT substring('abc' SIMILAR 'abc' ESCAPE '#');
-- begin-expected
-- columns: substring:text
-- row: abc
-- rowcount: 1
-- end-expected
SELECT substring('abc' from 'a%c' for '#');
-- begin-expected
-- columns: substring:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT substring('abc' from '(x)?b');
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'a' SIMILAR TO '.' ESCAPE '!';
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'a1' SIMILAR TO 'a\d' ESCAPE '!';
-- begin-expected
-- columns: quote_ident:text
-- row: "user"
-- rowcount: 1
-- end-expected
SELECT quote_ident('user');
-- begin-expected
-- columns: quote_ident:text
-- row: "window"
-- rowcount: 1
-- end-expected
SELECT quote_ident('window');
-- begin-expected
-- columns: quote_ident:text
-- row: "placing"
-- rowcount: 1
-- end-expected
SELECT quote_ident('placing');
-- begin-expected
-- columns: format:text
-- row: a b
-- rowcount: 1
-- end-expected
SELECT format('%1$s %s', 'a', 'b');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: format specifies argument 0, but arguments are numbered from 1
-- end-expected-error
SELECT format('%0$s','a');
-- begin-expected
-- columns: unistr:text
-- row: data
-- rowcount: 1
-- end-expected
SELECT unistr('dat\U00000061');
-- begin-expected-error
-- sqlstate: 42601
-- message-like: invalid Unicode escape
-- end-expected-error
SELECT unistr('\wxyz');
-- begin-expected-error
-- sqlstate: 42601
-- message-like: invalid Unicode escape
-- end-expected-error
SELECT unistr('\12');
-- begin-expected
-- columns: upper:text
-- row: STRAßE
-- rowcount: 1
-- end-expected
SELECT upper('straße');
-- begin-expected
-- columns: length:int4
-- row: 6
-- rowcount: 1
-- end-expected
SELECT length(upper('straße'));
-- begin-expected
-- columns: unicode_assigned:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT unicode_assigned(U&'a\0378');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_o1 (id int primary key, a text);
-- begin-expected
-- columns: pg_describe_object:text
-- row: column a of table zz_vf_o1
-- rowcount: 1
-- end-expected
SELECT pg_describe_object('pg_class'::regclass::oid, 'zz_vf_o1'::regclass::oid, 2);
-- begin-expected
-- columns: pg_describe_object:text
-- row: type integer
-- rowcount: 1
-- end-expected
SELECT pg_describe_object('pg_type'::regclass::oid, 'int4'::regtype::oid, 0);
-- begin-expected
-- columns: pg_describe_object:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT pg_describe_object(0,0,0);
-- begin-expected
-- columns: ?column?:tsquery
-- row: !'a'
-- rowcount: 1
-- end-expected
SELECT !!'a'::tsquery;
-- begin-expected
-- columns: tmplname:name
-- row: ispell
-- row: simple
-- row: snowball
-- row: synonym
-- row: thesaurus
-- rowcount: 5
-- end-expected
SELECT tmplname FROM pg_ts_template
 WHERE tmplname IN ('ispell','thesaurus','snowball','simple','synonym') ORDER BY tmplname;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf_grantee;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_g (id int);
-- begin-expected
-- ok: 0
-- end-expected
GRANT ALL ON zz_vf_g TO zz_vf_grantee;
-- begin-expected
-- columns: array_to_string:text
-- row: memgres=arwdDxtm/memgres zz_vf_grantee=arwdDxtm/memgres
-- rowcount: 1
-- end-expected
SELECT array_to_string(relacl,' ') FROM pg_class WHERE relname='zz_vf_g';
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf_r;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf_r2;
-- begin-expected
-- ok: 0
-- end-expected
GRANT zz_vf_r2 TO zz_vf_r WITH ADMIN OPTION;
-- begin-expected
-- ok: 0
-- end-expected
REVOKE ADMIN OPTION FOR zz_vf_r2 FROM zz_vf_r;
-- begin-expected
-- columns: admin_option:text
-- row: false
-- rowcount: 1
-- end-expected
SELECT admin_option::text FROM pg_auth_members m
  JOIN pg_roles a ON a.oid=m.member JOIN pg_roles b ON b.oid=m.roleid
 WHERE a.rolname='zz_vf_r' AND b.rolname='zz_vf_r2';
-- begin-expected-error
-- sqlstate: 08006
-- message-like: The server's client_encoding parameter was changed to LATIN1. The JDBC driver requires client_encoding to be UTF8 for correct operation.
-- end-expected-error
SET client_encoding = 'LATIN1';
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
BEGIN;
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SET DateStyle = 'German, DMY';
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
ROLLBACK;
-- extended protocol: Parse("", ""), Describe('S', ""), Sync
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SELECT overlay('abcdef' placing 'XY' from 2 for -1);
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SELECT overlay('\x010203'::bytea placing '\xff'::bytea from 2 for -1);
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SELECT 'abc' LIKE 'abc' ESCAPE NULL;
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SELECT 'abc' SIMILAR TO 'abc' ESCAPE 'xy';
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SELECT regexp_replace('banana', 'a', 'X', 1, -1);
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SELECT 1 ^@ 1;
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SELECT (date '2020-01-01') ^@ '2020';
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SELECT unicode('a');
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SELECT '12.34'::money * '2.00'::money;
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
SELECT '12.34'::money + 1;
-- begin-expected-error
-- sqlstate: 08003
-- message-like: This connection has been closed.
-- end-expected-error
CREATE OPERATOR #%& (LEFTARG = int, RIGHTARG = int, FUNCTION = zz_vf_nosuchin);
