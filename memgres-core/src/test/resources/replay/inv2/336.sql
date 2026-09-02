-- source: investigation-2026-08.md
-- finding: 336
-- title: Column resolution goes through one map keyed on name.toLowerCase(Locale.ROOT), and the lookup key is lower-cased too, so a quoted identifier means exactly what 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_dc (keep int, gone int);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "GONE" of relation "zz_dc" does not exist
-- end-expected-error
ALTER TABLE zz_dc DROP COLUMN "GONE";
-- begin-expected
-- columns: column_name:name
-- row: keep
-- row: gone
-- rowcount: 2
-- end-expected
SELECT column_name FROM information_schema.columns WHERE table_name = 'zz_dc' ORDER BY ordinal_position;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_q2 ("MiXeD" int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_q2 VALUES (1);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "mixed" does not exist
-- end-expected-error
SELECT "mixed" FROM zz_q2;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "mixed" of relation "zz_q2" does not exist
-- end-expected-error
UPDATE zz_q2 SET "mixed" = 9;
