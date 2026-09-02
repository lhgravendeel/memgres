-- source: investigation-2026-08.md
-- finding: 172
-- title: the privilege-inquiry functions parse relation names by hand (no case folding, indexOf('.') with no quote awareness, sequences keyed on the bare name) and two o
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_sc;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_sc.zz_sq;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_sr" does not exist
-- end-expected-error
GRANT ALL ON ALL SEQUENCES IN SCHEMA zz_sc TO zz_sr;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_sr" does not exist
-- end-expected-error
SELECT has_sequence_privilege('zz_sr','zz_sc.zz_sq','USAGE')::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_ty AS ENUM ('a','b');
-- begin-expected
-- ok: 0
-- end-expected
REVOKE USAGE ON TYPE zz_ty FROM PUBLIC;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "zz_hr" does not exist
-- end-expected-error
SELECT has_type_privilege('zz_hr','zz_ty','USAGE')::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_m1;
-- begin-expected
-- columns: has_database_privilege:text
-- row: false
-- rowcount: 1
-- end-expected
SELECT has_database_privilege('zz_m1','template0','TEMPORARY')::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ht (id int);
-- begin-expected
-- columns: has_table_privilege:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT has_table_privilege('ZZ_HT','SELECT')::text;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_a.dotted" does not exist
-- end-expected-error
SELECT has_table_privilege('"zz_a.dotted"','SELECT')::text;
