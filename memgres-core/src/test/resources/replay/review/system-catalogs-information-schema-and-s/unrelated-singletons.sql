-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: System catalogs, information_schema and security
-- title: Unrelated singletons
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_pn PASSWORD NULL;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_roles WHERE rolname='zz_pn';
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_pr;
-- begin-expected
-- ok: 0
-- end-expected
GRANT pg_read_all_data TO zz_pr;
-- begin-expected
-- columns: pg_has_role:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT pg_has_role('zz_pr','pg_read_all_data','MEMBER')::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE USER zz_u;
-- begin-expected
-- columns: rolcanlogin:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT rolcanlogin::text FROM pg_roles WHERE rolname='zz_u';
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_ad;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_ad2;
-- begin-expected
-- ok: 0
-- end-expected
GRANT zz_ad2 TO zz_ad WITH ADMIN OPTION;
-- begin-expected
-- columns: pg_has_role:text | pg_has_role:text
-- row: true | true
-- rowcount: 1
-- end-expected
SELECT pg_has_role('zz_ad','zz_ad2','MEMBER WITH ADMIN OPTION')::text,
       pg_has_role('zz_ad','zz_ad2','USAGE WITH GRANT OPTION')::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_pw PASSWORD 'secret';
-- begin-expected
-- columns: substring:text
-- row: SCRAM-SHA-256
-- rowcount: 1
-- end-expected
SELECT substring(rolpassword from 1 for 13) FROM pg_authid WHERE rolname='zz_pw';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_v1 (id int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_v1 ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- columns: row_security_active:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT row_security_active('zz_v1');
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_polr;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pol (id int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_pol ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_polp ON zz_pol FOR SELECT TO zz_polr, PUBLIC USING (true);
-- begin-expected
-- columns: roles:text
-- row: {public}
-- rowcount: 1
-- end-expected
SELECT roles::text FROM pg_policies WHERE policyname='zz_polp';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_d1 (id int PRIMARY KEY);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_d2 (id int REFERENCES zz_d1(id));
-- begin-expected
-- columns: deptype:char | ?column?:bool
-- row: a | t
-- row: i | t
-- row: n | t
-- rowcount: 3
-- end-expected
SELECT deptype, count(*) > 0 FROM pg_depend WHERE refobjid='zz_d1'::regclass GROUP BY deptype ORDER BY deptype;
-- begin-expected
-- columns: count:int8
-- row: 494
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_get_keywords();
-- begin-expected
-- columns: pg_settings_get_flags:_text
-- row: {EXPLAIN}
-- rowcount: 1
-- end-expected
SELECT pg_settings_get_flags('work_mem');
-- begin-expected
-- columns: pg_char_to_encoding:int4
-- row: 6
-- rowcount: 1
-- end-expected
SELECT pg_char_to_encoding('UTF8');
-- begin-expected
-- columns: pg_relation_filenode:oid
-- row: 1259
-- rowcount: 1
-- end-expected
SELECT pg_relation_filenode('pg_class'::regclass);
-- begin-expected
-- columns: pg_opfamily_is_visible:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT pg_opfamily_is_visible((SELECT oid FROM pg_opfamily LIMIT 1));
-- begin-expected
-- columns: type:text
-- row: table
-- rowcount: 1
-- end-expected
SELECT type FROM pg_identify_object('pg_class'::regclass::oid,'pg_class'::regclass::oid,0);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_fd(a int, b text DEFAULT 'z') RETURNS text LANGUAGE sql AS $$ SELECT b $$;
-- begin-expected
-- columns: pg_get_function_arg_default:text
-- row: 'z'::text
-- rowcount: 1
-- end-expected
SELECT pg_get_function_arg_default('zz_fd(int,text)'::regprocedure, 2);
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_no_such_schema" does not exist
-- end-expected-error
SELECT 'zz_no_such_schema'::regnamespace::text;
-- begin-expected
-- columns: text:text
-- row: character varying
-- rowcount: 1
-- end-expected
SELECT 'varchar(10)'::regtype::text;
-- begin-expected
-- columns: text:text
-- row: +(integer,integer)
-- rowcount: 1
-- end-expected
SELECT '+(int4,int4)'::regoperator::text;
-- begin-expected
-- columns: to_regnamespace:text
-- row: public
-- rowcount: 1
-- end-expected
SELECT to_regnamespace('public')::text;
-- begin-expected
-- columns: pg_column_size:int4 | pg_column_size:int4 | pg_column_size:int4 | pg_column_size:int4
-- row: 4 | 1 | 8 | 2
-- rowcount: 1
-- end-expected
SELECT pg_column_size(1::int), pg_column_size(true), pg_column_size(1::bigint), pg_column_size(1::smallint);
-- memgres side run with -Duser.language=tr -Duser.country=TR
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ID (x int);
-- begin-expected
-- columns: text:text
-- row: zz_id
-- rowcount: 1
-- end-expected
SELECT 'zz_ID'::regclass::text;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_class WHERE relname='zz_id';
