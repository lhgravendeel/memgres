-- source: review-2026-08.md
-- finding: Root cause 12: role is a GUC string, not a role reference
-- area: PREPARE, EXECUTE, DO and CALL
-- title: Root cause 12: role is a GUC string, not a role reference
-- begin-expected-error
-- sqlstate: 22023
-- message-like: role "test" does not exist
-- end-expected-error
SET ROLE test;
-- begin-expected
-- columns: current_setting:text
-- row: none
-- rowcount: 1
-- end-expected
SELECT current_setting('role');
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: role "test" does not exist
-- end-expected-error
SET SESSION AUTHORIZATION test;
-- begin-expected
-- columns: current_user:name
-- row: memgres
-- rowcount: 1
-- end-expected
SELECT current_user;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
GRANT zz_a TO CURRENT_USER;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE TO zz_a;
-- begin-expected
-- columns: current_user:name
-- row: zz_a
-- rowcount: 1
-- end-expected
SELECT current_user;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE = zz_a;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r2;
-- begin-expected
-- ok: 0
-- end-expected
SET SESSION AUTHORIZATION zz_r2;
-- begin-expected
-- ok: 0
-- end-expected
DISCARD ALL;
-- begin-expected
-- columns: still_set:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT session_user = 'zz_r2' AS still_set;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DEFAULT"
-- end-expected-error
SET ROLE DEFAULT;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: role "zz_nosuchrole" does not exist
-- end-expected-error
SET ROLE zz_nosuchrole;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: role "zz_nosuchrole" does not exist
-- end-expected-error
SET SESSION AUTHORIZATION zz_nosuchrole;
