CREATE ROLE ma_r1 NOLOGIN;
CREATE ROLE ma_r2 NOLOGIN;
CREATE TABLE ma_t (id int, owner text);
ALTER TABLE ma_t ENABLE ROW LEVEL SECURITY;
CREATE POLICY ma_p1 ON ma_t AS RESTRICTIVE FOR SELECT USING (owner = 'x');
-- begin-expected
-- columns: n | polpermissive
-- row: ma_p1 | f
-- end-expected
SELECT polname::text AS n, polpermissive FROM pg_policy WHERE polrelid='ma_t'::regclass;
ALTER POLICY ma_p1 ON ma_t USING (owner = 'y');
-- begin-expected
-- columns: n | polpermissive
-- row: ma_p1 | f
-- end-expected
SELECT polname::text AS n, polpermissive FROM pg_policy WHERE polrelid='ma_t'::regclass;
ALTER POLICY ma_p1 ON ma_t TO ma_r1;
-- begin-expected
-- columns: n | polpermissive
-- row: ma_p1 | f
-- end-expected
SELECT polname::text AS n, polpermissive FROM pg_policy WHERE polrelid='ma_t'::regclass;
ALTER POLICY ma_p1 ON ma_t RENAME TO ma_p2;
-- begin-expected
-- columns: n | polpermissive
-- row: ma_p2 | f
-- end-expected
SELECT polname::text AS n, polpermissive FROM pg_policy WHERE polrelid='ma_t'::regclass;
-- begin-expected
-- columns: permissive
-- row: RESTRICTIVE
-- end-expected
SELECT permissive FROM pg_policies WHERE tablename='ma_t';
GRANT SELECT ON ma_t TO ma_r1;
GRANT ma_r1 TO ma_r2;
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM information_schema.role_table_grants WHERE grantee='ma_r1' AND table_name='ma_t';
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_auth_members m JOIN pg_roles r ON m.roleid=r.oid JOIN pg_roles g ON m.member=g.oid WHERE r.rolname='ma_r1' AND g.rolname='ma_r2';
ALTER ROLE ma_r1 RENAME TO ma_r1b;
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM information_schema.role_table_grants WHERE grantee='ma_r1b' AND table_name='ma_t';
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_auth_members m JOIN pg_roles r ON m.roleid=r.oid JOIN pg_roles g ON m.member=g.oid WHERE r.rolname='ma_r1b' AND g.rolname='ma_r2';
-- begin-expected
-- columns: a
-- row: {OWNER=arwdDxtm/OWNER,ma_r1b=r/OWNER}
-- end-expected
SELECT replace(relacl::text, current_user, 'OWNER') AS a FROM pg_class WHERE relname='ma_t';
DROP TABLE ma_t CASCADE;
DROP OWNED BY ma_r1b;
DROP ROLE ma_r1b, ma_r2;
CREATE ROLE mb_r NOLOGIN;
CREATE TABLE mb_t (id int);
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT replace(relacl::text, current_user, 'OWNER') AS a FROM pg_class WHERE relname='mb_t';
GRANT SELECT ON mb_t TO mb_r;
-- begin-expected
-- columns: a
-- row: {OWNER=arwdDxtm/OWNER,mb_r=r/OWNER}
-- end-expected
SELECT replace(relacl::text, current_user, 'OWNER') AS a FROM pg_class WHERE relname='mb_t';
GRANT INSERT, UPDATE ON mb_t TO mb_r;
-- begin-expected
-- columns: a
-- row: {OWNER=arwdDxtm/OWNER,mb_r=arw/OWNER}
-- end-expected
SELECT replace(relacl::text, current_user, 'OWNER') AS a FROM pg_class WHERE relname='mb_t';
GRANT ALL ON mb_t TO PUBLIC;
-- begin-expected
-- columns: a
-- row: {OWNER=arwdDxtm/OWNER,mb_r=arw/OWNER,=arwdDxtm/OWNER}
-- end-expected
SELECT replace(relacl::text, current_user, 'OWNER') AS a FROM pg_class WHERE relname='mb_t';
REVOKE ALL ON mb_t FROM PUBLIC;
REVOKE ALL ON mb_t FROM mb_r;
-- begin-expected
-- columns: a
-- row: {OWNER=arwdDxtm/OWNER}
-- end-expected
SELECT replace(relacl::text, current_user, 'OWNER') AS a FROM pg_class WHERE relname='mb_t';
CREATE SEQUENCE mb_s;
GRANT USAGE ON SEQUENCE mb_s TO mb_r;
-- begin-expected
-- columns: a
-- row: {OWNER=rwU/OWNER,mb_r=U/OWNER}
-- end-expected
SELECT replace(relacl::text, current_user, 'OWNER') AS a FROM pg_class WHERE relname='mb_s';
CREATE SCHEMA mb_sc;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT replace(nspacl::text, current_user, 'OWNER') AS a FROM pg_namespace WHERE nspname='mb_sc';
GRANT USAGE ON SCHEMA mb_sc TO mb_r;
-- begin-expected
-- columns: a
-- row: {OWNER=UC/OWNER,mb_r=U/OWNER}
-- end-expected
SELECT replace(nspacl::text, current_user, 'OWNER') AS a FROM pg_namespace WHERE nspname='mb_sc';
CREATE TABLE mb_c (a int, b int);
GRANT SELECT (b), INSERT (a), UPDATE (a), REFERENCES (a) ON mb_c TO mb_r;
-- begin-expected
-- columns: n | a
-- row: a | {mb_r=awx/OWNER}
-- row: b | {mb_r=r/OWNER}
-- end-expected
SELECT attname::text AS n, replace(attacl::text, current_user, 'OWNER') AS a FROM pg_attribute WHERE attrelid='mb_c'::regclass AND attnum>0 ORDER BY attnum;
GRANT ALL ON mb_c TO mb_r;
-- begin-expected
-- columns: a
-- row: {OWNER=arwdDxtm/OWNER,mb_r=arwdDxtm/OWNER}
-- end-expected
SELECT replace(relacl::text, current_user, 'OWNER') AS a FROM pg_class WHERE relname='mb_c';
GRANT SELECT ON mb_c TO mb_r WITH GRANT OPTION;
-- begin-expected
-- columns: a
-- row: {OWNER=arwdDxtm/OWNER,mb_r=ar*wdDxtm/OWNER}
-- end-expected
SELECT replace(relacl::text, current_user, 'OWNER') AS a FROM pg_class WHERE relname='mb_c';
-- begin-expected
-- columns: g | p | ig
-- row: OWNER | DELETE | YES
-- row: OWNER | INSERT | YES
-- row: OWNER | REFERENCES | YES
-- row: OWNER | SELECT | YES
-- row: OWNER | TRIGGER | YES
-- row: OWNER | TRUNCATE | YES
-- row: OWNER | UPDATE | YES
-- row: mb_r | DELETE | NO
-- row: mb_r | INSERT | NO
-- row: mb_r | REFERENCES | NO
-- row: mb_r | SELECT | YES
-- row: mb_r | TRIGGER | NO
-- row: mb_r | TRUNCATE | NO
-- row: mb_r | UPDATE | NO
-- end-expected
SELECT replace(grantee::text, current_user, 'OWNER') AS g, privilege_type::text AS p, is_grantable::text AS ig FROM information_schema.role_table_grants WHERE table_name='mb_c' ORDER BY 1,2;
-- begin-expected
-- columns: g | c | p
-- row: OWNER | a | INSERT
-- row: OWNER | a | REFERENCES
-- row: OWNER | a | SELECT
-- row: OWNER | a | UPDATE
-- row: OWNER | b | INSERT
-- row: OWNER | b | REFERENCES
-- row: OWNER | b | SELECT
-- row: OWNER | b | UPDATE
-- row: mb_r | a | INSERT
-- row: mb_r | a | REFERENCES
-- row: mb_r | a | SELECT
-- row: mb_r | a | UPDATE
-- row: mb_r | b | INSERT
-- row: mb_r | b | REFERENCES
-- row: mb_r | b | SELECT
-- row: mb_r | b | SELECT
-- row: mb_r | b | UPDATE
-- end-expected
SELECT replace(grantee::text, current_user, 'OWNER') AS g, column_name::text AS c, privilege_type::text AS p FROM information_schema.column_privileges WHERE table_name='mb_c' ORDER BY 1,2,3;
CREATE FUNCTION mb_f() RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$;
GRANT ALL ON FUNCTION mb_f() TO mb_r;
-- begin-expected
-- columns: a
-- row: {=X/OWNER,OWNER=X/OWNER,mb_r=X/OWNER}
-- end-expected
SELECT replace(proacl::text, current_user, 'OWNER') AS a FROM pg_proc WHERE proname='mb_f';
CREATE TYPE mb_ty AS (x int);
GRANT ALL ON TYPE mb_ty TO mb_r;
-- begin-expected
-- columns: a
-- row: {=U/OWNER,OWNER=U/OWNER,mb_r=U/OWNER}
-- end-expected
SELECT replace(typacl::text, current_user, 'OWNER') AS a FROM pg_type WHERE typname='mb_ty';
CREATE DOMAIN mb_d AS int;
GRANT USAGE ON DOMAIN mb_d TO mb_r;
-- begin-expected
-- columns: a
-- row: {=U/OWNER,OWNER=U/OWNER,mb_r=U/OWNER}
-- end-expected
SELECT replace(typacl::text, current_user, 'OWNER') AS a FROM pg_type WHERE typname='mb_d';
DROP TABLE mb_t, mb_c CASCADE;
DROP SEQUENCE mb_s;
DROP SCHEMA mb_sc;
DROP FUNCTION mb_f();
DROP TYPE mb_ty;
DROP DOMAIN mb_d;
DROP OWNED BY mb_r;
DROP ROLE mb_r;
