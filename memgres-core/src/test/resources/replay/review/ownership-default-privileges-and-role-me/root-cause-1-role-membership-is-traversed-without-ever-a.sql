-- source: review-2026-08.md
-- finding: Root cause 1: role membership is traversed without ever asking whether the member inherits
-- area: Ownership, default privileges and role membership
-- title: Root cause 1: role membership is traversed without ever asking whether the member inherits
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
CREATE TABLE zz_t (i int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_t TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
GRANT zz_a TO zz_b;
-- begin-expected
-- columns: has_table_privilege:text
-- row: false
-- rowcount: 1
-- end-expected
SELECT has_table_privilege('zz_b','zz_t','SELECT')::text;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_b;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for table zz_t
-- end-expected-error
SELECT count(*) FROM zz_t;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_b" already exists
-- end-expected-error
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
