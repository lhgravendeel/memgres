-- source: investigation-2026-08.md
-- finding: 177
-- title: CREATE CAST hardcodes castfunc to 0, renders the context keyword as 'e', scans only the user-defined cast list for duplicates, and validates only the first argu
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_e AS ENUM ('a','bb');
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_ef(zz_e) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT length($1::text) $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE CAST (zz_e AS int) WITH FUNCTION zz_ef(zz_e) AS ASSIGNMENT;
-- begin-expected
-- columns: castcontext:char | castmethod:char | castfunc:text
-- row: a | f | zz_ef(zz_e)
-- rowcount: 1
-- end-expected
SELECT castcontext, castmethod, castfunc::regprocedure::text FROM pg_cast
 WHERE castsource='zz_e'::regtype AND casttarget='int4'::regtype;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: cast from type integer to type bigint already exists
-- end-expected-error
CREATE CAST (int4 AS int8) WITH INOUT;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_v" does not exist
-- end-expected-error
CREATE CAST (zz_v AS int) WITH FUNCTION zz_f4(zz_v, text);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_v" does not exist
-- end-expected-error
CREATE CAST (zz_v AS int) WITH FUNCTION zz_f5(zz_v, int, int);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_notype" does not exist
-- end-expected-error
DROP CAST (int AS zz_notype);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_notype" does not exist
-- end-expected-error
DROP CAST (zz_notype AS int);
