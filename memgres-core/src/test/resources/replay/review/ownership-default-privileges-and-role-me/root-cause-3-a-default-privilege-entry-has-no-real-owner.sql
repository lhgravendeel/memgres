-- source: review-2026-08.md
-- finding: Root cause 3: a default-privilege entry has no real owner
-- area: Ownership, default privileges and role membership
-- title: Root cause 3: a default-privilege entry has no real owner
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_own;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_gee;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_ds AUTHORIZATION zz_own;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_own;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_ds GRANT SELECT ON TABLES TO zz_gee;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ds.mine (i int);
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ds.theirs (i int);
-- begin-expected
-- columns: rolname:name
-- row: zz_own
-- rowcount: 1
-- end-expected
SELECT r.rolname FROM pg_default_acl d JOIN pg_roles r ON r.oid=d.defaclrole
  JOIN pg_namespace n ON n.oid=d.defaclnamespace WHERE n.nspname='zz_ds';
-- begin-expected
-- columns: has_table_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_table_privilege('zz_gee','zz_ds.mine','SELECT')::text;
-- begin-expected
-- columns: has_table_privilege:text
-- row: false
-- rowcount: 1
-- end-expected
SELECT has_table_privilege('zz_gee','zz_ds.theirs','SELECT')::text;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_xa;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_xb;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_xg;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_xs;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES FOR ROLE zz_xa IN SCHEMA zz_xs GRANT SELECT ON TABLES TO zz_xg;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES FOR ROLE zz_xb IN SCHEMA zz_xs GRANT SELECT ON TABLES TO zz_xg;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_xs REVOKE SELECT ON TABLES FROM zz_xg;
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_default_acl d JOIN pg_namespace n ON n.oid=d.defaclnamespace
 WHERE n.nspname='zz_xs';
