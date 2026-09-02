-- source: investigation-2026-08.md
-- finding: 187
-- title: Object ownership is recorded but never checked: no DDL path consults Database.setObjectOwner/objectOwners, so any role can ALTER or DROP any table
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_perm (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf_perm_r NOLOGIN;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_vf_perm_r;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: must be owner of table zz_vf_perm
-- end-expected-error
ALTER TABLE zz_vf_perm ADD COLUMN z int;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: must be owner of table zz_vf_perm
-- end-expected-error
DROP TABLE zz_vf_perm;
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM information_schema.tables WHERE table_name = 'zz_vf_perm';
