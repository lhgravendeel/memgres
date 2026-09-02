-- source: investigation-2026-08.md
-- finding: 209
-- title: Role administration validates nothing it names — the same root cause the August report gave as security root cause 5, reached through four grammar paths it did 
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_a;
-- begin-expected
-- columns: pg_has_role:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT pg_has_role(current_user,'zz_a','MEMBER')::text;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a IN ROLE zz_nosuchrole;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_roles WHERE rolname='zz_a';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_b" does not exist
-- end-expected-error
GRANT zz_a TO zz_b;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_c" does not exist
-- end-expected-error
GRANT zz_b TO zz_c;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_c" does not exist
-- end-expected-error
GRANT zz_c TO zz_a;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_c" does not exist
-- end-expected-error
SELECT pg_has_role('zz_a','zz_c','MEMBER')::text;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_b" does not exist
-- end-expected-error
REVOKE zz_nosuchrole FROM zz_b;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_nosuchrole" does not exist
-- end-expected-error
REVOKE zz_a FROM zz_nosuchrole;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "public" does not exist
-- end-expected-error
GRANT zz_a TO PUBLIC;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_b" does not exist
-- end-expected-error
GRANT PUBLIC TO zz_b;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_nosuchrole" does not exist
-- end-expected-error
GRANT zz_a TO zz_b GRANTED BY zz_nosuchrole;
