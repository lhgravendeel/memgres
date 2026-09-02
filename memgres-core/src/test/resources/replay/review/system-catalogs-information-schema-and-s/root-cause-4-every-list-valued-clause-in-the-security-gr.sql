-- source: review-2026-08.md
-- finding: Root cause 4: every list-valued clause in the security grammar keeps one element and drops the rest
-- area: System catalogs, information_schema and security
-- title: Root cause 4: every list-valued clause in the security grammar keeps one element and drops the rest
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_a (x int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_b (x int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_a, zz_b TO zz_r;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t2 (id int, owner text, n int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r3;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT (id), UPDATE (n) ON zz_t2 TO zz_r3;
-- begin-expected
-- columns: has_column_privilege:text | has_column_privilege:text
-- row: true | false
-- rowcount: 1
-- end-expected
SELECT has_column_privilege('zz_r3','zz_t2','id','SELECT')::text,
       has_column_privilege('zz_r3','zz_t2','n','SELECT')::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_g1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_a1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_c1 IN ROLE zz_g1 ROLE zz_a1 ADMIN zz_a1;
-- begin-expected
-- columns: pg_has_role:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT pg_has_role('zz_a1','zz_c1','MEMBER')::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_l1;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_no_such_role_x" does not exist
-- end-expected-error
DROP ROLE zz_l1, zz_no_such_role_x;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_roles WHERE rolname='zz_l1';
