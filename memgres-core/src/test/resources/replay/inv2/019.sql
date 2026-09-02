-- source: investigation-2026-08.md
-- finding: 19
-- title: CREATE TYPE ... AS RANGE only calls database.addRangeType: the name never enters the type namespace, no companion multirange type is created, the option list is
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_rt1 AS RANGE (SUBTYPE = int4);
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_rt1 AS RANGE (SUBTYPE = int4);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_r2 AS RANGE (SUBTYPE = int4);
-- begin-expected
-- columns: ?column?:bool | rngcanonical:text | rngsubdiff:text | ?column?:bool
-- row: t | - | - | t
-- rowcount: 1
-- end-expected
SELECT rngsubopc > 0, rngcanonical::text, rngsubdiff::text, rngmultitypid > 0 FROM pg_range WHERE rngtypid='zz_vf_r2'::regtype;
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_type WHERE typname LIKE 'zz\_vf\_r2%';
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_vf_d AS numeric(6,2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_c AS (p int, q text);
-- begin-expected
-- columns: typname:name | typarray:text
-- row: zz_vf_c | zz_vf_c[]
-- row: zz_vf_d | zz_vf_d[]
-- rowcount: 2
-- end-expected
SELECT typname, typarray::regtype::text FROM pg_type WHERE typname IN ('zz_vf_c','zz_vf_d') ORDER BY typname;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_fr AS RANGE (subtype = float8);
-- begin-expected
-- columns: zz_vf_fr:zz_vf_fr
-- row: [1.5,2.5)
-- rowcount: 1
-- end-expected
SELECT '[1.5,2.5)'::zz_vf_fr;
-- begin-expected
-- columns: pg_typeof:text
-- row: zz_vf_fr
-- rowcount: 1
-- end-expected
SELECT pg_typeof(zz_vf_fr(1.5,2.5))::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_rt (r zz_vf_fr);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: type attribute "nosuchopt" not recognized
-- end-expected-error
CREATE TYPE zz_vf_cr1 AS RANGE (SUBTYPE = int4, NOSUCHOPT = 1);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: operator class "nosuch_ops" does not exist for access method "btree"
-- end-expected-error
CREATE TYPE zz_vf_cr2 AS RANGE (SUBTYPE = int4, SUBTYPE_OPCLASS = nosuch_ops);
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot specify a canonical function without a pre-created shell type
-- end-expected-error
CREATE TYPE zz_vf_cr3 AS RANGE (SUBTYPE = int4, CANONICAL = missingfn);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: data type json has no default operator class for access method "btree"
-- end-expected-error
CREATE TYPE zz_vf_cr6 AS RANGE (SUBTYPE = json);
