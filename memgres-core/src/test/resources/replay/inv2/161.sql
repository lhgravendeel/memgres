-- source: investigation-2026-08.md
-- finding: 161
-- title: CREATE/ALTER DDL is parsed permissively: options are matched, recorded (or discarded) and never validated. Referenced objects are not resolved, mutually exclusi
-- begin-expected
-- ok: 0
-- end-expected
CREATE PROCEDURE zz_vf_cpr() LANGUAGE sql AS $$ SELECT 1 $$;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: zz_vf_cpr() is not a function
-- end-expected-error
DROP FUNCTION zz_vf_cpr();
-- begin-expected
-- columns: n:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_proc WHERE proname='zz_vf_cpr';
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_v6s;
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
-- sqlstate: 42P16
-- message-like: cannot create temporary relation in non-temporary schema
-- end-expected-error
CREATE TEMP VIEW zz_vf_v6s.zz_vf_v6tv2 AS SELECT 1 AS x;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ct (a int);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: tablespace "zz_vf_nosuch_tablespace" does not exist
-- end-expected-error
CREATE INDEX zz_vf_i1 ON zz_vf_ct (a) TABLESPACE zz_vf_nosuch_tablespace;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf_ct" already exists
-- end-expected-error
CREATE TABLE zz_vf_ct (a int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ON"
-- end-expected-error
CREATE INDEX IF NOT EXISTS ON zz_vf_ct (a);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ridx (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf_ridx_i ON zz_vf_ridx (a);
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
-- message-like: relation "zz_vf_nosuchindex" does not exist
-- end-expected-error
REINDEX INDEX zz_vf_nosuchindex;
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_mvw AS SELECT 1 AS x;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_vf_mvw" is not a table or materialized view
-- end-expected-error
REINDEX TABLE zz_vf_mvw;
