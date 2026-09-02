-- source: investigation-2026-08.md
-- finding: 159
-- title: Privilege checks read only TABLE-keyed grants held by the current role. checkTablePrivilege never consults the COLUMN-keyed grants the GRANT executor writes, no
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_pq;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf_r LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_vf_r;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for sequence zz_vf_pq
-- end-expected-error
SELECT nextval('zz_vf_pq');
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_st (id int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_st VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_vi2 WITH (security_invoker=true) AS SELECT * FROM zz_vf_st;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf_r3 LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_vf_vi2 TO zz_vf_r3;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_vf_r3;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for table zz_vf_st
-- end-expected-error
SELECT id FROM zz_vf_vi2 ORDER BY id;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pt2 (i int);
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_vf_r" already exists
-- end-expected-error
CREATE ROLE zz_vf_r LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf_r2;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_vf_pt2 TO zz_vf_r2;
-- begin-expected
-- ok: 0
-- end-expected
GRANT zz_vf_r2 TO zz_vf_r WITH INHERIT FALSE;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_vf_r;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for table zz_vf_pt2
-- end-expected-error
SELECT count(*)::int FROM zz_vf_pt2;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_psc;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_vf_r2" already exists
-- end-expected-error
CREATE ROLE zz_vf_r2;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_vf_psc GRANT USAGE ON SEQUENCES TO zz_vf_r2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_psc.s1;
-- begin-expected
-- columns: p:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_sequence_privilege('zz_vf_r2','zz_vf_psc.s1','USAGE')::text AS p;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pt (a int, b text);
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_vf_r" already exists
-- end-expected-error
CREATE ROLE zz_vf_r;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_vf_r2" already exists
-- end-expected-error
CREATE ROLE zz_vf_r2;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT (a) ON zz_vf_pt TO zz_vf_r;
-- begin-expected
-- columns: column_name:name | privilege_type:varchar
-- row: a | SELECT
-- rowcount: 1
-- end-expected
SELECT column_name, privilege_type FROM information_schema.column_privileges WHERE table_name='zz_vf_pt' AND grantee='zz_vf_r' ORDER BY 1,2;
-- begin-expected
-- ok: 0
-- end-expected
GRANT zz_vf_r2 TO zz_vf_r WITH ADMIN OPTION;
-- begin-expected
-- columns: role_name:name | is_grantable:varchar
-- row: zz_vf_r2 | YES
-- rowcount: 1
-- end-expected
SELECT role_name, is_grantable FROM information_schema.applicable_roles WHERE grantee='zz_vf_r' ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf_pt" already exists
-- end-expected-error
CREATE TABLE zz_vf_pt (a int, b text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_pt VALUES (1,'x');
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_vf_r" already exists
-- end-expected-error
CREATE ROLE zz_vf_r LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT (a) ON zz_vf_pt TO zz_vf_r;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_vf_r;
-- begin-expected
-- columns: a:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT a FROM zz_vf_pt;
