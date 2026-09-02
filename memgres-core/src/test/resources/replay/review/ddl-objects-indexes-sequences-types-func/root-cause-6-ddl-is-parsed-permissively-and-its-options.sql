-- source: review-2026-08.md
-- finding: Root cause 6: DDL is parsed permissively and its options are recorded without validation
-- area: DDL objects: indexes, sequences, types, functions, triggers, rules, views, partitioning and the catalogs that describe them
-- title: Root cause 6: DDL is parsed permissively and its options are recorded without validation
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ct" does not exist
-- end-expected-error
CREATE INDEX zz_vf_i1 ON zz_vf_ct (a) TABLESPACE zz_vf_nosuch_tablespace;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ON"
-- end-expected-error
CREATE INDEX IF NOT EXISTS ON zz_vf_ct (a);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 25001
-- message-like: CREATE INDEX CONCURRENTLY cannot run inside a transaction block
-- end-expected-error
CREATE INDEX CONCURRENTLY zz_vf_ridx_c ON zz_vf_ridx (a);
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_vf_nosuchschema" does not exist
-- end-expected-error
REINDEX SCHEMA zz_vf_nosuchschema;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: can only reindex the currently open database
-- end-expected-error
REINDEX DATABASE zz_vf_nosuchdb;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_mvw" does not exist
-- end-expected-error
REINDEX TABLE zz_vf_mvw;
-- a view
-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE SEQUENCE zz_vf_cq1 START 5 START 6;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE SEQUENCE zz_vf_cq2 CYCLE NO CYCLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_csch;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_csch.cseq;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ct" does not exist
-- end-expected-error
ALTER SEQUENCE zz_vf_csch.cseq OWNED BY zz_vf_ct.a;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "zz_vf_cb1" does not exist
-- end-expected-error
CREATE TYPE zz_vf_cb1 (OUTPUT = int4out);
-- begin-expected-error
-- sqlstate: 42602
-- message-like: invalid enum label "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
-- end-expected-error
CREATE TYPE zz_vf_ce1 AS ENUM ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "RETRN"
-- end-expected-error
CREATE FUNCTION zz_vf_cf8() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETRN 1; END $$;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: missing expression at or near "THEN"
-- end-expected-error
CREATE FUNCTION zz_vf_cf9() RETURNS int LANGUAGE plpgsql AS $$ BEGIN IF THEN END $$;
-- begin-expected-error
-- sqlstate: 42P13
-- message-like: return type mismatch in function declared to return integer
-- end-expected-error
CREATE FUNCTION zz_vf_cf10() RETURNS int LANGUAGE sql AS $$ $$;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf_cadd(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR #%~ (LEFTARG = int, RIGHTARG = int, FUNCTION = zz_vf_cadd, NEGATOR = #%~);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_vf_ce3" does not exist
-- end-expected-error
ALTER TYPE zz_vf_ce3 ADD VALUE 'b';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_vf_ce3" does not exist
-- end-expected-error
SELECT 'b'::zz_vf_ce3;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use "list" partition strategy with more than one column
-- end-expected-error
CREATE TABLE zz_vf_fr1 (a int, b int) PARTITION BY LIST (a, b);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: data type json has no default operator class for access method "btree"
-- end-expected-error
CREATE TABLE zz_vf_fr2 (j json) PARTITION BY RANGE (j);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SELECT"
-- end-expected-error
CREATE TABLE zz_vf_fr3 (i int) PARTITION BY RANGE ((SELECT 1));
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_fr4" does not exist
-- end-expected-error
CREATE TABLE zz_vf_fr4_x PARTITION OF zz_vf_fr4 FOR VALUES IN (MINVALUE);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_flp" does not exist
-- end-expected-error
CREATE TABLE zz_vf_flc () INHERITS (zz_vf_flp);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_fl" does not exist
-- end-expected-error
ALTER TABLE zz_vf_fl ATTACH PARTITION zz_vf_flc FOR VALUES FROM (1) TO (10);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_fi_1" does not exist
-- end-expected-error
ALTER TABLE zz_vf_fi_1 INHERIT zz_vf_flp;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_fi_1" does not exist
-- end-expected-error
ALTER TABLE zz_vf_fi_1 ADD COLUMN extra int;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_fi" does not exist
-- end-expected-error
CREATE TABLE zz_vf_pc () INHERITS (zz_vf_fi);
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_lockseq;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot lock relation "zz_vf_lockseq"
-- end-expected-error
LOCK TABLE zz_vf_lockseq;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: WITH CHECK OPTION not supported on recursive views
-- end-expected-error
CREATE RECURSIVE VIEW zz_vf_v5c (n) AS SELECT 1 UNION ALL SELECT n+1 FROM zz_vf_v5c WHERE n < 5 WITH CHECK OPTION;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_n1" does not exist
-- end-expected-error
CREATE UNIQUE INDEX zz_vf_n1_ix ON zz_vf_n1 ((i+1));
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_n1" does not exist
-- end-expected-error
REFRESH MATERIALIZED VIEW CONCURRENTLY zz_vf_n1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TEMP VIEW zz_vf_v6tv AS SELECT 1 AS x;
-- begin-expected
-- columns: istemp:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT schemaname LIKE 'pg_temp%' AS istemp FROM pg_views WHERE viewname='zz_vf_v6tv';
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_vf_v6s" does not exist
-- end-expected-error
CREATE TEMP VIEW zz_vf_v6s.zz_vf_v6tv2 AS SELECT 1 AS x;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ct" does not exist
-- end-expected-error
CREATE INDEX zz_vf_i5 ON zz_vf_ct (ctid);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ct" does not exist
-- end-expected-error
CREATE INDEX zz_vf_i7 ON zz_vf_ct ((ctid::text));
