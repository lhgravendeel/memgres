-- source: investigation-2026-08.md
-- finding: 299
-- title: parseAlterDefaultPrivileges matches a fixed skeleton — one FOR ROLE name, then one IN SCHEMA name, an optional TO — and then advances to the semicolon discardin
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_s;
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: invalid privilege type SELECT for function
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON FUNCTIONS TO zz_a;
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: invalid privilege type EXECUTE for relation
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT EXECUTE ON TABLES TO zz_a;
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: invalid privilege type USAGE for relation
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT USAGE ON TABLES TO zz_a;
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: invalid privilege type SELECT for type
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON TYPES TO zz_a;
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: invalid privilege type CREATE for sequence
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT CREATE ON SEQUENCES TO zz_a;
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: cannot use IN SCHEMA clause when using GRANT/REVOKE ON SCHEMAS
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT USAGE ON SCHEMAS TO zz_a;
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: cannot use IN SCHEMA clause when using GRANT/REVOKE ON LARGE OBJECTS
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON LARGE OBJECTS TO zz_a;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_nosuchrole" does not exist
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_nosuchrole;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized privilege type "nosuchpriv"
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT NOSUCHPRIV ON TABLES TO zz_a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "NOSUCHKIND"
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON NOSUCHKIND TO zz_a;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_default_acl d JOIN pg_namespace n ON n.oid=d.defaclnamespace WHERE n.nspname='zz_s';
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
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON TABLES;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "TABLE"
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON TABLE TO zz_a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ADMIN"
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_a WITH ADMIN OPTION;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "GRANTED"
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_a GRANTED BY zz_a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "THIS"
-- end-expected-error
ALTER DEFAULT PRIVILEGES IN SCHEMA zz_s GRANT SELECT ON TABLES TO zz_a THIS IS NOT SQL AT ALL;
