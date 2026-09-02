-- source: review-2026-08.md
-- finding: Root cause 5: type ownership is never recorded, and composite types are invisible to the ownership path
-- area: Ownership, default privileges and role membership
-- title: Root cause 5: type ownership is never recorded, and composite types are invisible to the ownership path
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_ty AS (a int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TYPE zz_ty OWNER TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TYPE public.zz_ty OWNER TO zz_a;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_en AS ENUM ('x','y');
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_d AS int;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TYPE zz_en OWNER TO zz_a;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DOMAIN zz_d OWNER TO zz_a;
-- begin-expected
-- columns: typname:name | ?column?:bool
-- row: zz_d | t
-- row: zz_en | t
-- rowcount: 2
-- end-expected
SELECT typname, typowner = (SELECT oid FROM pg_roles WHERE rolname='zz_a')
  FROM pg_type WHERE typname IN ('zz_en','zz_d') ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "zz_a" already exists
-- end-expected-error
CREATE ROLE zz_a;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "zz_ty" already exists
-- end-expected-error
CREATE TYPE zz_ty AS (a int);
-- begin-expected
-- columns: has_type_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_type_privilege('zz_a','zz_ty','USAGE')::text;
-- begin-expected
-- columns: has_type_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_type_privilege('zz_a','public.zz_ty','USAGE')::text;
