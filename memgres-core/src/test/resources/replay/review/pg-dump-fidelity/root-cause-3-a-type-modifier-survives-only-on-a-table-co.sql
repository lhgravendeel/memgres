-- source: review-2026-08.md
-- finding: Root cause 3: a type modifier survives only on a table column; everywhere else the declaration is recorded as a bare type name
-- area: pg_dump fidelity
-- title: Root cause 3: a type modifier survives only on a table column; everywhere else the declaration is recorded as a bare type name
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf2_ct1 AS (street text, city varchar(40), zip char(6), amt numeric(8,2));
-- begin-expected
-- columns: attname:name | format_type:text
-- row: street | text
-- row: city | character varying(40)
-- row: zip | character(6)
-- row: amt | numeric(8,2)
-- rowcount: 4
-- end-expected
SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
 JOIN pg_class c ON c.oid = a.attrelid JOIN pg_type t ON t.typrelid = c.oid
 WHERE t.typname = 'zz_vf2_ct1' AND a.attnum > 0 ORDER BY a.attnum;
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_vf2_dv1 AS varchar(20);
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_vf2_dn1 AS integer NOT NULL DEFAULT 1 CHECK (VALUE > 0);
-- begin-expected
-- columns: format_type:text
-- row: character varying(20)
-- rowcount: 1
-- end-expected
SELECT format_type(typbasetype, typtypmod) FROM pg_type WHERE typname = 'zz_vf2_dv1';
-- begin-expected
-- columns: typlen:int2 | typbyval:bool | typstorage:char | typoutput:text
-- row: 4 | t | p | int4out
-- rowcount: 1
-- end-expected
SELECT typlen, typbyval, typstorage, typoutput::text FROM pg_type WHERE typname = 'zz_vf2_dn1';
-- begin-expected
-- columns: pg_get_expr:text
-- row: 1
-- rowcount: 1
-- end-expected
SELECT pg_get_expr(typdefaultbin, 0) FROM pg_type WHERE typname = 'zz_vf2_dn1';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_t1 (c_arrn numeric(6,2)[]);
-- begin-expected
-- columns: atttypmod:int4 | format_type:text
-- row: 393222 | numeric(6,2)[]
-- rowcount: 1
-- end-expected
SELECT a.atttypmod, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
 JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'zz_vf2_t1' AND a.attname = 'c_arrn';
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf2_t1" already exists
-- end-expected-error
CREATE TABLE zz_vf2_t1 (c_arr2 text[][]);
-- begin-expected
-- columns: format_type:text | attndims:int2
-- rowcount: 0
-- end-expected
SELECT format_type(a.atttypid, a.atttypmod), a.attndims FROM pg_attribute a
 JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'zz_vf2_t1' AND a.attname = 'c_arr2';
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "zz_vf2_dn1" already exists
-- end-expected-error
CREATE DOMAIN zz_vf2_dn1 AS integer NOT NULL DEFAULT 1 CHECK (VALUE > 0);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf2_t1" already exists
-- end-expected-error
CREATE TABLE zz_vf2_t1 (c_dom zz_vf2_dn1);
-- begin-expected
-- columns: attnotnull:bool
-- rowcount: 0
-- end-expected
SELECT attnotnull FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
 WHERE c.relname = 'zz_vf2_t1' AND a.attname = 'c_dom';
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "zz_vf2_dv1" already exists
-- end-expected-error
CREATE DOMAIN zz_vf2_dv1 AS varchar(20);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf2_rg1 AS RANGE (subtype = integer);
-- begin-expected
-- columns: typname:name
-- row: _zz_vf2_dv1
-- row: _zz_vf2_rg1
-- rowcount: 2
-- end-expected
SELECT typname FROM pg_type WHERE typname IN ('_zz_vf2_dv1','_zz_vf2_rg1') ORDER BY typname;
