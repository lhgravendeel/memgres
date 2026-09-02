-- source: investigation-2026-08.md
-- finding: 302
-- title: Unrelated singletons in this area
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_b NOINHERIT;
-- begin-expected
-- ok: 0
-- end-expected
GRANT zz_a TO zz_b;
-- begin-expected
-- columns: pg_has_role:text
-- row: false
-- rowcount: 1
-- end-expected
SELECT pg_has_role('zz_b','zz_a','USAGE')::text;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_s;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT EXECUTE ON FUNCTIONS TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_s.f() RETURNS int LANGUAGE sql AS 'SELECT 1';
-- begin-expected
-- columns: has_function_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_function_privilege('zz_a','zz_s.f()','EXECUTE')::text;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES GRANT USAGE ON SCHEMAS TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_s2;
-- begin-expected
-- columns: has_schema_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_schema_privilege('zz_a','zz_s2','USAGE')::text;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_nosuchrole_x" does not exist
-- end-expected-error
DROP OWNED BY zz_a, zz_nosuchrole_x;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_b" already exists
-- end-expected-error
CREATE ROLE zz_b;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_c;
-- begin-expected
-- ok: 0
-- end-expected
REASSIGN OWNED BY zz_a, zz_b TO zz_c;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_b" already exists
-- end-expected-error
CREATE ROLE zz_b;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "CASCADE"
-- end-expected-error
REASSIGN OWNED BY zz_a TO zz_b CASCADE;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_en AS ENUM ('x','y');
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_d AS int;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TYPE zz_en OWNER TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DOMAIN zz_d OWNER TO zz_a;
-- begin-expected
-- columns: typname:name | ?column?:bool
-- row: zz_d | t
-- row: zz_en | t
-- rowcount: 2
-- end-expected
SELECT typname, typowner = (SELECT oid FROM pg_roles WHERE rolname='zz_a')
  FROM pg_type WHERE typname IN ('zz_en','zz_d') ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_b" already exists
-- end-expected-error
CREATE ROLE zz_b;
-- begin-expected-error
-- sqlstate: 42P06
-- message-like: schema "zz_s" already exists
-- end-expected-error
CREATE SCHEMA zz_s;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s, public GRANT SELECT ON TABLES TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES FOR ROLE zz_a, zz_b GRANT SELECT ON TABLES TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s FOR ROLE zz_a GRANT SELECT ON TABLES TO zz_b;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES FOR ROLE CURRENT_USER IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES FOR ROLE CURRENT_ROLE IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DEFAULT PRIVILEGES FOR ROLE SESSION_USER IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_a;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected-error
-- sqlstate: 42P06
-- message-like: schema "zz_s" already exists
-- end-expected-error
CREATE SCHEMA zz_s;
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: default privileges cannot be set for columns
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT (a) ON TABLES TO zz_a;
