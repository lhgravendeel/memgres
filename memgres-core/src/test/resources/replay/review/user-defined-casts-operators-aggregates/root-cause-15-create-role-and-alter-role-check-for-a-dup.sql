-- source: review-2026-08.md
-- finding: Root cause 15: CREATE ROLE and ALTER ROLE check for a duplicate name and nothing else
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 15: CREATE ROLE and ALTER ROLE check for a duplicate name and nothing else
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_no_such_role" does not exist
-- end-expected-error
CREATE ROLE zz_c1 IN ROLE zz_no_such_role;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_no_such_role" does not exist
-- end-expected-error
CREATE SCHEMA zz_s1 AUTHORIZATION zz_no_such_role;
-- begin-expected-error
-- sqlstate: 42939
-- message-like: role name "pg_zz_reserved_x" is reserved
-- end-expected-error
CREATE ROLE pg_zz_reserved_x;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_m1" does not exist
-- end-expected-error
ALTER ROLE zz_m1 RENAME TO pg_zz_reserved_y;
