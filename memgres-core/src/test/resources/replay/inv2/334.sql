-- source: investigation-2026-08.md
-- finding: 334
-- title: Unrelated singletons in this area
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_d1 (id integer, CONSTRAINT zz_vf2_ck1 CHECK (id > 0 AND id < 100));
-- begin-expected
-- columns: pg_get_expr:text
-- row: ((id > 0) AND (id < 100))
-- rowcount: 1
-- end-expected
SELECT pg_get_expr(conbin, conrelid) FROM pg_constraint WHERE conname = 'zz_vf2_ck1';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_t1 (c_arr2 text[][]);
-- begin-expected
-- columns: format_type:text | attndims:int2
-- row: text[] | 2
-- rowcount: 1
-- end-expected
SELECT format_type(a.atttypid, a.atttypmod), a.attndims FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'zz_vf2_t1' AND a.attname = 'c_arr2';
-- begin-expected
-- ok: 0
-- end-expected
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
SELECT attnotnull FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'zz_vf2_t1' AND a.attname = 'c_dom';
-- begin-expected
-- ok: 0
-- end-expected
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
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf2_t1" already exists
-- end-expected-error
CREATE TABLE zz_vf2_t1 (c_vc varchar(44));
-- begin-expected
-- columns: attstattarget:int2
-- rowcount: 0
-- end-expected
SELECT attstattarget FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'zz_vf2_t1' AND a.attname = 'c_vc';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_q5" does not exist
-- end-expected-error
GRANT SELECT, USAGE ON SEQUENCE zz_vf2_q5 TO PUBLIC;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf2_f5(integer) does not exist
-- end-expected-error
GRANT EXECUTE ON FUNCTION zz_vf2_f5(integer) TO PUBLIC;
-- begin-expected
-- columns: seq_acl:text | func_acl:text
-- row: NULL | NULL
-- rowcount: 1
-- end-expected
SELECT (SELECT array_to_string(relacl,' ') FROM pg_class WHERE relname='zz_vf2_q5') AS seq_acl, (SELECT array_to_string(proacl,' ') FROM pg_proc WHERE proname='zz_vf2_f5') AS func_acl;
-- begin-expected
-- columns: text:text
-- row: 2024-01-01
-- rowcount: 1
-- end-expected
SELECT '2024-01-01'::date::text;
-- begin-expected
-- columns: text:text
-- row: 1
-- rowcount: 1
-- end-expected
SELECT 1::int::text;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_d7 (p point);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_d7 VALUES ('(1e10,-1e-10)');
-- begin-expected
-- columns: p:text
-- row: (10000000000,-1e-10)
-- rowcount: 1
-- end-expected
SELECT p::text FROM zz_vf2_d7;
-- begin-expected
-- columns: option_name:text | option_value:text
-- row: autovacuum_enabled | false
-- row: fillfactor | 70
-- rowcount: 2
-- end-expected
SELECT option_name, option_value FROM pg_options_to_table(ARRAY['fillfactor=70','autovacuum_enabled=false']) ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_t7" does not exist
-- end-expected-error
CREATE STATISTICS zz_vf2_st7 ON a, b FROM zz_vf2_t7;
-- begin-expected
-- columns: pg_get_statisticsobjdef:text
-- rowcount: 0
-- end-expected
SELECT pg_get_statisticsobjdef(oid) FROM pg_statistic_ext WHERE stxname = 'zz_vf2_st7';
