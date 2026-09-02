-- source: investigation-2026-08.md
-- finding: 292
-- title: Unrelated singletons in this area
-- Parse + Describe 'S'
-- begin-expected-error
-- sqlstate: 42P02
-- message-like: there is no parameter $2000000000
-- end-expected-error
SELECT $2000000000;
-- Parse + Describe 'S'
-- begin-expected-error
-- sqlstate: 42P02
-- message-like: there is no parameter $70000
-- end-expected-error
SELECT $70000;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_d (a text DEFAULT 'a' || 'b', e text DEFAULT upper('q'));
-- begin-expected
-- columns: column_name:name | column_default:varchar
-- row: a | ('a'::text || 'b'::text)
-- row: e | upper('q'::text)
-- rowcount: 2
-- end-expected
SELECT column_name, column_default FROM information_schema.columns
 WHERE table_name = 'zz_vf2_d' ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf2_d" already exists
-- end-expected-error
CREATE TABLE zz_vf2_d (b int DEFAULT '-3'::int, f numeric DEFAULT '1.5'::numeric);
-- begin-expected
-- columns: column_name:name | column_default:varchar
-- row: a | ('a'::text || 'b'::text)
-- row: e | upper('q'::text)
-- rowcount: 2
-- end-expected
SELECT column_name, column_default FROM information_schema.columns
 WHERE table_name = 'zz_vf2_d' ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_df (a text DEFAULT CASE WHEN true THEN 'x' ELSE 'y' END,
                        b text DEFAULT COALESCE(NULL, 'c'),
                        c int  DEFAULT (ARRAY[7,8,9])[2]);
-- begin-expected
-- columns: column_name:name | column_default:varchar
-- row: a | \nCASE\n    WHEN true THEN 'x'::text\n    ELSE 'y'::text\nEND
-- row: b | COALESCE(NULL::text, 'c'::text)
-- row: c | (ARRAY[7, 8, 9])[2]
-- rowcount: 3
-- end-expected
SELECT column_name, column_default FROM information_schema.columns
 WHERE table_name='zz_vf2_df' ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ix (id int, n int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_ix_p ON zz_vf2_ix (id) WHERE n > 5;
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_ix_e ON zz_vf2_ix ((n + 1));
-- begin-expected
-- columns: pg_get_expr:text
-- row: (n > 5)
-- rowcount: 1
-- end-expected
SELECT pg_get_expr(indpred, indrelid) FROM pg_index WHERE indexrelid='zz_vf2_ix_p'::regclass;
-- begin-expected
-- columns: pg_get_expr:text
-- row: (n + 1)
-- rowcount: 1
-- end-expected
SELECT pg_get_expr(indexprs, indrelid) FROM pg_index WHERE indexrelid='zz_vf2_ix_e'::regclass;
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
-- begin-expected-error
-- sqlstate: 42704
-- message-like: replication slot "zz_vf2_noslot" does not exist
-- end-expected-error
SELECT pg_drop_replication_slot('zz_vf2_noslot');
-- begin-expected
-- columns: pg_indexam_has_property:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT pg_indexam_has_property(405,'can_order');
-- begin-expected
-- columns: pg_indexam_has_property:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT pg_indexam_has_property(405,'can_unique');
-- begin-expected
-- columns: pg_indexam_has_property:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT pg_indexam_has_property(403,'can_include');
-- begin-expected
-- columns: pg_indexam_has_property:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT pg_indexam_has_property(783,'can_exclude');
-- begin-expected
-- columns: pg_indexam_has_property:bool
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT pg_indexam_has_property(403,'no_such_property');
-- begin-expected
-- columns: pg_indexam_has_property:bool
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT pg_indexam_has_property(999999,'can_order');
