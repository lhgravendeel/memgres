-- source: investigation-2026-08.md
-- finding: 165
-- title: ALTER ROLE ... RENAME TO is removeRole+createRole and moves only the attribute map; privileges and memberships are keyed by role name in two other maps and are 
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t1" does not exist
-- end-expected-error
GRANT SELECT ON zz_t1 TO zz_old;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_old" does not exist
-- end-expected-error
GRANT zz_grp TO zz_old;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_old" does not exist
-- end-expected-error
ALTER ROLE zz_old RENAME TO zz_new;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_new" does not exist
-- end-expected-error
SELECT has_table_privilege('zz_new','zz_t1','SELECT')::text, pg_has_role('zz_new','zz_grp','MEMBER')::text;
