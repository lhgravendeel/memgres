-- source: review-2026-08.md
-- finding: Root cause 2: object ownership is recorded but never checked
-- area: Transactions, sessions, cursors and locks
-- title: Root cause 2: object ownership is recorded but never checked
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
