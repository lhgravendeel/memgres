-- source: investigation-2026-08.md
-- finding: 230
-- title: Unrelated singletons in this area
-- begin-expected
-- ok: 0
-- end-expected
CREATE COLLATION zz_coll (LOCALE = 'C');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a text COLLATE zz_coll);
-- begin-expected
-- columns: collation_name:name
-- row: zz_coll
-- rowcount: 1
-- end-expected
SELECT collation_name FROM information_schema.columns WHERE table_name='zz_t' AND column_name='a';
-- begin-expected
-- columns: collname:name
-- row: zz_coll
-- rowcount: 1
-- end-expected
SELECT c.collname FROM pg_attribute at JOIN pg_collation c ON c.oid = at.attcollation WHERE at.attrelid = 'zz_t'::regclass AND at.attname = 'a';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_rb" does not exist
-- end-expected-error
ALTER DEFAULT PRIVILEGES FOR ROLE zz_ra IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_rb;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_rb" does not exist
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s REVOKE SELECT ON TABLES FROM zz_rb;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_default_acl d JOIN pg_namespace n ON n.oid=d.defaclnamespace WHERE n.nspname='zz_s';
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_s" does not exist
-- end-expected-error
CREATE FUNCTION zz_s.f() RETURNS int LANGUAGE sql AS 'SELECT 1';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_ra" does not exist
-- end-expected-error
ALTER FUNCTION zz_s.f() OWNER TO zz_ra;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_ra" does not exist
-- end-expected-error
REASSIGN OWNED BY zz_ra TO zz_rb;
-- begin-expected
-- columns: rolname:name
-- rowcount: 0
-- end-expected
SELECT r.rolname FROM pg_proc p JOIN pg_roles r ON r.oid=p.proowner WHERE p.proname='f';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_w" does not exist
-- end-expected-error
DECLARE zz_f1 CURSOR WITH HOLD FOR SELECT i FROM zz_w FOR UPDATE;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE cannot be applied to VALUES
-- end-expected-error
DECLARE zz_f2 CURSOR FOR VALUES (1) FOR UPDATE;
-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot insert into view "pg_cursors"
-- end-expected-error
INSERT INTO pg_cursors VALUES ('x','y',false,false,false,now());
-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot delete from view "pg_cursors"
-- end-expected-error
DELETE FROM pg_cursors;
-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot update view "pg_cursors"
-- end-expected-error
UPDATE pg_cursors SET name = 'z';
