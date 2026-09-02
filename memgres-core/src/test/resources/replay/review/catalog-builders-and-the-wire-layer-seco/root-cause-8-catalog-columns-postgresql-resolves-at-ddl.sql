-- source: review-2026-08.md
-- finding: Root cause 8: catalog columns PostgreSQL resolves at DDL time are left at whatever the declaration happened to say, or at a plausible constant
-- area: Catalog builders and the wire layer, second pass
-- title: Root cause 8: catalog columns PostgreSQL resolves at DDL time are left at whatever the declaration happened to say, or at a plausible constant
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_oc (a bytea, b interval, c inet, d "time", e char(5), f uuid, h money);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_oc_a ON zz_vf2_oc (a);
-- and one per column
-- begin-expected
-- columns: relname:name | opcname:name
-- row: zz_vf2_oc_a | bytea_ops
-- rowcount: 1
-- end-expected
SELECT c.relname, (SELECT opcname FROM pg_opclass o WHERE o.oid = i.indclass[0])
  FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
 WHERE c.relname LIKE 'zz\_vf2\_oc%' ORDER BY 1;
-- begin-expected
-- columns: rngsubopc:text | rngcanonical:text | rngsubdiff:text
-- row: 1978 | int4range_canonical | int4range_subdiff
-- rowcount: 1
-- end-expected
SELECT rngsubopc::text, rngcanonical::text, rngsubdiff::text
  FROM pg_range WHERE rngtypid = 'int4range'::regtype;
-- begin-expected
-- columns: ?column?:bool | ?column?:bool | ?column?:bool
-- row: t | f | t
-- rowcount: 1
-- end-expected
SELECT rngsubopc > 0, rngcanonical::text <> '-', rngsubdiff::text <> '-'
  FROM pg_range WHERE rngtypid = 'tsrange'::regtype;
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
