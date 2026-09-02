-- source: review-2026-08.md
-- finding: Root cause 5: security DDL validates nothing it names
-- area: System catalogs, information_schema and security
-- title: Root cause 5: security DDL validates nothing it names
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g1 (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_gr1;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_g1" is not a sequence
-- end-expected-error
GRANT SELECT ON SEQUENCE zz_g1 TO zz_gr1;
-- PG 42809
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_nosuch(integer) does not exist
-- end-expected-error
GRANT EXECUTE ON FUNCTION zz_nosuch(int) TO zz_gr1;
-- PG 42883
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_nosuchtype" does not exist
-- end-expected-error
GRANT USAGE ON TYPE zz_nosuchtype TO zz_gr1;
-- PG 42704
-- begin-expected-error
-- sqlstate: 42704
-- message-like: language "zz_nosuchlang" does not exist
-- end-expected-error
GRANT USAGE ON LANGUAGE zz_nosuchlang TO zz_gr1;
-- PG 42704
-- begin-expected-error
-- sqlstate: 3D000
-- message-like: database "zz_nosuchdb" does not exist
-- end-expected-error
GRANT CREATE ON DATABASE zz_nosuchdb TO zz_gr1;
-- PG 3D000
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_nosch" does not exist
-- end-expected-error
GRANT SELECT ON ALL TABLES IN SCHEMA zz_nosch TO zz_gr1;
-- PG 3F000
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_nosch" does not exist
-- end-expected-error
REVOKE USAGE ON SCHEMA zz_nosch FROM zz_gr1;
-- PG 3F000
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_nosuchrole" does not exist
-- end-expected-error
GRANT zz_gr1 TO zz_nosuchrole;
-- PG 42704
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_nosuchrole" does not exist
-- end-expected-error
GRANT zz_nosuchrole TO zz_gr1;
-- PG 42704
-- begin-expected-error
-- sqlstate: 0LP01
-- message-like: role "zz_gr1" is a member of role "zz_gr1"
-- end-expected-error
GRANT zz_gr1 TO zz_gr1;
-- PG 0LP01
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ADMIN"
-- end-expected-error
GRANT SELECT ON zz_g1 TO zz_gr1 WITH ADMIN OPTION;
-- PG 42601
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized role option "nosuchoption"
-- end-expected-error
CREATE ROLE zz_ro NOSUCHOPTION;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
ALTER ROLE zz_ro WITH SUPERUSER NOSUPERUSER;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized role option "nosuchoption"
-- end-expected-error
ALTER ROLE zz_ro WITH NOSUCHOPTION;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_ro" does not exist
-- end-expected-error
ALTER ROLE zz_ro VALID UNTIL 'garbage';
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r3;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized configuration parameter "zz_nosuchguc"
-- end-expected-error
ALTER ROLE zz_r3 SET zz_nosuchguc = 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_dt (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_dr;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_dt TO zz_dr;
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: role "zz_dr" cannot be dropped because some objects depend on it
-- end-expected-error
DROP ROLE zz_dr;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_d (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_dr1 LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_dr2;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_d TO zz_dr1 WITH GRANT OPTION;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_dr1;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_d TO zz_dr2;
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: dependent privileges exist
-- end-expected-error
REVOKE SELECT ON zz_d FROM zz_dr1 RESTRICT;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t3 (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r4;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON ALL TABLES IN SCHEMA public TO zz_r4;
-- begin-expected
-- ok: 0
-- end-expected
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM zz_r4;
-- begin-expected
-- columns: has_table_privilege:text
-- row: false
-- rowcount: 1
-- end-expected
SELECT has_table_privilege('zz_r4','zz_t3','SELECT')::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pt (id int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_pt ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_pp ON zz_pt FOR SELECT USING (id = 1);
-- begin-expected-error
-- sqlstate: 42710
-- message-like: policy "zz_pp" for table "zz_pt" already exists
-- end-expected-error
ALTER POLICY zz_pp ON zz_pt RENAME TO zz_pp;
