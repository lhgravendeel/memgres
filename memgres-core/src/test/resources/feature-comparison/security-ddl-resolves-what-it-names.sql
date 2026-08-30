CREATE TABLE ig_t (a int);
CREATE ROLE ig_r1 NOLOGIN;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "ig_t" is not a sequence
-- end-expected-error
GRANT SELECT ON SEQUENCE ig_t TO ig_r1;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function ig_nosuch(integer) does not exist
-- end-expected-error
GRANT EXECUTE ON FUNCTION ig_nosuch(int) TO ig_r1;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "ig_nosuchtype" does not exist
-- end-expected-error
GRANT USAGE ON TYPE ig_nosuchtype TO ig_r1;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: language "ig_nosuchlang" does not exist
-- end-expected-error
GRANT USAGE ON LANGUAGE ig_nosuchlang TO ig_r1;
-- begin-expected-error
-- sqlstate: 3D000
-- message-like: database "ig_nosuchdb" does not exist
-- end-expected-error
GRANT CREATE ON DATABASE ig_nosuchdb TO ig_r1;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "ig_nosuchschema" does not exist
-- end-expected-error
GRANT SELECT ON ALL TABLES IN SCHEMA ig_nosuchschema TO ig_r1;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "ig_nosuchschema" does not exist
-- end-expected-error
REVOKE USAGE ON SCHEMA ig_nosuchschema FROM ig_r1;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "ig_nosuchrole" does not exist
-- end-expected-error
GRANT ig_r1 TO ig_nosuchrole;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "ig_nosuchrole" does not exist
-- end-expected-error
GRANT ig_nosuchrole TO ig_r1;
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: role "ig_r1" is a member of role "ig_r1"
-- end-expected-error
GRANT ig_r1 TO ig_r1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ADMIN"
-- end-expected-error
GRANT SELECT ON ig_t TO ig_r1 WITH ADMIN OPTION;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "ig_nosuchrole" does not exist
-- end-expected-error
GRANT SELECT ON ig_t TO ig_nosuchrole;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized privilege type "nosuchpriv"
-- end-expected-error
GRANT NOSUCHPRIV ON ig_t TO ig_r1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ig_nosuchtable" does not exist
-- end-expected-error
GRANT SELECT ON ig_nosuchtable TO ig_r1;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "ig_t" does not exist
-- end-expected-error
GRANT SELECT (nosuchcol) ON ig_t TO ig_r1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized role option "nosuchoption"
-- end-expected-error
CREATE ROLE ig_ro NOLOGIN NOSUCHOPTION;
CREATE ROLE ig_ro NOLOGIN;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
ALTER ROLE ig_ro WITH SUPERUSER NOSUPERUSER;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized role option "nosuchoption"
-- end-expected-error
ALTER ROLE ig_ro WITH NOSUCHOPTION;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type timestamp with time zone: "garbage"
-- end-expected-error
ALTER ROLE ig_ro VALID UNTIL 'garbage';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized configuration parameter "ig_nosuchguc"
-- end-expected-error
ALTER ROLE ig_ro SET ig_nosuchguc = 1;
ALTER ROLE ig_ro SET work_mem = '4MB';
CREATE ROLE ig_dr NOLOGIN;
GRANT SELECT ON ig_t TO ig_dr;
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: role "ig_dr" cannot be dropped because some objects depend on it
-- end-expected-error
DROP ROLE ig_dr;
CREATE TABLE ig_d (i int);
CREATE ROLE ig_dr1 LOGIN;
CREATE ROLE ig_dr2 NOLOGIN;
GRANT SELECT ON ig_d TO ig_dr1 WITH GRANT OPTION;
REVOKE SELECT ON ig_d FROM ig_dr1 RESTRICT;
CREATE TABLE ig_t3 (i int);
CREATE ROLE ig_r4 NOLOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ig_r4;
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM ig_r4;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT has_table_privilege('ig_r4','ig_t3','SELECT')::text AS a;
DROP OWNED BY ig_r4;
DROP OWNED BY ig_r4, ig_dr2;
REASSIGN OWNED BY ig_r4 TO ig_dr2;
REASSIGN OWNED BY ig_r4, ig_dr2 TO ig_r1;
DROP TABLE ig_t, ig_d, ig_t3 CASCADE;
DROP OWNED BY ig_r1;
DROP OWNED BY ig_ro;
DROP OWNED BY ig_dr1;
DROP OWNED BY ig_dr2;
DROP OWNED BY ig_r4;
DROP ROLE IF EXISTS ig_r1, ig_ro, ig_dr1, ig_dr2, ig_r4;
