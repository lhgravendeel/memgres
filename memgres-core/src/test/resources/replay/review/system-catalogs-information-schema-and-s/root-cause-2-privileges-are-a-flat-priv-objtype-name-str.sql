-- source: review-2026-08.md
-- finding: Root cause 2: privileges are a flat `PRIV:OBJTYPE:name` string set that checker and catalogs read literally
-- area: System catalogs, information_schema and security
-- title: Root cause 2: privileges are a flat `PRIV:OBJTYPE:name` string set that checker and catalogs read literally
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_y1 (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_yr;
-- begin-expected
-- ok: 0
-- end-expected
GRANT ALL ON zz_y1 TO zz_yr;
-- begin-expected
-- columns: has_table_privilege:text
-- row: false
-- rowcount: 1
-- end-expected
SELECT has_table_privilege('zz_yr','zz_y1','SELECT WITH GRANT OPTION')::text;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_y1" already exists
-- end-expected-error
CREATE TABLE zz_y1 (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_y2 (a int);
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_yr" already exists
-- end-expected-error
CREATE ROLE zz_yr;
-- begin-expected
-- ok: 0
-- end-expected
GRANT ALL ON zz_y1 TO zz_yr;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_y2 TO zz_yr WITH GRANT OPTION;
-- begin-expected
-- columns: table_name:name | privilege_type:varchar | is_grantable:varchar
-- row: zz_y1 | DELETE | NO
-- row: zz_y1 | INSERT | NO
-- row: zz_y1 | REFERENCES | NO
-- row: zz_y1 | SELECT | NO
-- row: zz_y1 | TRIGGER | NO
-- row: zz_y1 | TRUNCATE | NO
-- row: zz_y1 | UPDATE | NO
-- row: zz_y2 | SELECT | YES
-- rowcount: 8
-- end-expected
SELECT table_name, privilege_type, is_grantable FROM information_schema.table_privileges
 WHERE grantee='zz_yr' ORDER BY table_name, privilege_type;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_y1" already exists
-- end-expected-error
CREATE TABLE zz_y1 (a int);
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_yr" already exists
-- end-expected-error
CREATE ROLE zz_yr;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_y1 TO zz_yr WITH GRANT OPTION;
-- begin-expected
-- columns: relacl:text
-- row: {memgres=arwdDxtm/memgres,zz_yr=ar*wdDxtm/memgres}
-- rowcount: 1
-- end-expected
SELECT relacl::text FROM pg_class WHERE relname='zz_y1';
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO zz_yr;
-- begin-expected
-- columns: defaclacl:text
-- row: {zz_yr=r/memgres}
-- rowcount: 1
-- end-expected
SELECT defaclacl::text FROM pg_default_acl WHERE defaclacl::text LIKE '%zz_yr%';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g1 (a int, b text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r2;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT (b) ON zz_g1 TO zz_r2;
-- begin-expected
-- columns: attname:name | attacl:text
-- row: b | {zz_r2=r/memgres}
-- rowcount: 1
-- end-expected
SELECT attname, attacl::text FROM pg_attribute
 WHERE attrelid='zz_g1'::regclass AND attacl IS NOT NULL ORDER BY attname;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_hr;
-- begin-expected
-- columns: has_table_privilege:text | has_table_privilege:text
-- row: true | true
-- rowcount: 1
-- end-expected
SELECT has_table_privilege('zz_hr','pg_class','SELECT')::text,
       has_table_privilege('zz_hr','information_schema.tables','SELECT')::text;
-- begin-expected
-- columns: acldefault:text
-- row: {memgres=arwdDxtm/memgres}
-- rowcount: 1
-- end-expected
SELECT acldefault('r', (SELECT oid FROM pg_roles WHERE rolname=current_user))::text;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: ACL arrays must be one-dimensional
-- end-expected-error
SELECT grantor, grantee, privilege_type FROM aclexplode('{}'::aclitem[]);
