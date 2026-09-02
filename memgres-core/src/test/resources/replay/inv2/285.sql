-- source: investigation-2026-08.md
-- finding: 285
-- title: Catalog columns PostgreSQL resolves at DDL time are left at whatever the declaration happened to say, or at a plausible constant: resolveColumnOpclass defaults 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_p (id int PRIMARY KEY);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_c (x int REFERENCES zz_vf2_p);
-- begin-expected
-- columns: contype:char | conkey:text | confkey:text
-- row: f | {1} | {1}
-- rowcount: 1
-- end-expected
SELECT contype, conkey::text, confkey::text FROM pg_constraint
 WHERE conrelid = 'zz_vf2_c'::regclass AND contype = 'f';
