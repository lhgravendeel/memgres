-- source: investigation-2026-08.md
-- finding: 300
-- title: A role grant is considered complete after its grantee list, so only the literal WITH ADMIN OPTION tail is recognised and everything else after it is neither par
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_b;
-- begin-expected
-- ok: 0
-- end-expected
GRANT zz_a TO zz_b WITH ADMIN FALSE;
-- begin-expected
-- ok: 0
-- end-expected
REVOKE INHERIT OPTION FOR zz_a FROM zz_b;
-- begin-expected
-- ok: 0
-- end-expected
REVOKE SET OPTION FOR zz_a FROM zz_b;
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
-- message-like: unrecognized role option "nosuchoption"
-- end-expected-error
GRANT zz_a TO zz_b WITH NOSUCHOPTION TRUE;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "MAYBE"
-- end-expected-error
GRANT zz_a TO zz_b WITH INHERIT MAYBE;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized role option "grant"
-- end-expected-error
GRANT zz_a TO zz_b WITH GRANT OPTION;
