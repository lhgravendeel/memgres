-- source: investigation-2026-08.md
-- finding: 297
-- title: Type ownership is never recorded, and composite types are invisible to the ownership and type-privilege resolvers: executeAlterType routes only RENAME TO and SE
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
