-- source: review-2026-08.md
-- finding: Root cause 6: pg_default_acl is a statement log, not an ACL catalog
-- area: Ownership, default privileges and role membership
-- title: Root cause 6: pg_default_acl is a statement log, not an ACL catalog
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_s;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT INSERT ON TABLES TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT EXECUTE ON FUNCTIONS TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT EXECUTE ON ROUTINES TO zz_a;
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_default_acl d JOIN pg_namespace n ON n.oid=d.defaclnamespace
 WHERE n.nspname='zz_s';
-- begin-expected
-- columns: attname:name | format_type:text
-- row: defaclacl | aclitem[]
-- row: defaclnamespace | oid
-- row: defaclobjtype | "char"
-- row: defaclrole | oid
-- rowcount: 4
-- end-expected
SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute
 WHERE attrelid='pg_default_acl'::regclass
   AND attname IN ('defaclacl','defaclnamespace','defaclobjtype','defaclrole')
 ORDER BY attname;
