-- begin-expected-error
-- sqlstate: 3D000
-- message-like: database "zf_nodb" does not exist
-- end-expected-error
SELECT pg_database_size('zf_nodb');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zf_notable" does not exist
-- end-expected-error
SELECT pg_relation_size('zf_notable');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zf_notable" does not exist
-- end-expected-error
SELECT pg_total_relation_size('zf_notable');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zf_notable" does not exist
-- end-expected-error
SELECT pg_table_size('zf_notable');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zf_notable" does not exist
-- end-expected-error
SELECT pg_indexes_size('zf_notable');
-- begin-expected-error
-- sqlstate: 58P01
-- message-like: could not stat file "pg_tblspc/999999": No such file or directory
-- end-expected-error
SELECT pg_tablespace_location(999999::oid);
CREATE TABLE zf_p (id int PRIMARY KEY);
CREATE TABLE zf_c (x int REFERENCES zf_p);
-- begin-expected
-- columns: t | k | f
-- row: f | {1} | {1}
-- end-expected
SELECT contype::text AS t, conkey::text AS k, confkey::text AS f FROM pg_constraint WHERE conrelid = 'zf_c'::regclass AND contype = 'f';
CREATE INDEX zf_i ON zf_p (id);
-- begin-expected
-- columns: n
-- row: int4_ops
-- end-expected
SELECT o.opcname::text AS n FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid JOIN pg_opclass o ON o.oid = x.indclass[0] WHERE c.relname='zf_i';
CREATE TABLE zf_txt (a text);
CREATE INDEX zf_ti ON zf_txt (a);
-- begin-expected
-- columns: n
-- row: text_ops
-- end-expected
SELECT o.opcname::text AS n FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid JOIN pg_opclass o ON o.oid = x.indclass[0] WHERE c.relname='zf_ti';
DROP TABLE zf_txt, zf_c, zf_p CASCADE;
-- begin-expected
-- columns: pg_relation_size
-- row: NULL
-- end-expected
SELECT pg_relation_size(NULL);
-- begin-expected
-- columns: pg_relation_size
-- row: NULL
-- end-expected
SELECT pg_relation_size(999999::oid);
-- begin-expected
-- columns: pg_database_size
-- row: NULL
-- end-expected
SELECT pg_database_size(NULL);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: database with OID 999999 does not exist
-- end-expected-error
SELECT pg_database_size(999999::oid);
-- begin-expected
-- columns: pg_tablespace_location
-- row: NULL
-- end-expected
SELECT pg_tablespace_location(NULL);
-- begin-expected
-- columns: pg_tablespace_location
-- row: 
-- end-expected
SELECT pg_tablespace_location(1663::oid);
CREATE TABLE zg_t (a int);
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT pg_relation_size('zg_t') >= 0 AS ok;
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT pg_total_relation_size('zg_t') >= 0 AS ok;
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT pg_table_size('zg_t') >= 0 AS ok;
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT pg_indexes_size('zg_t') >= 0 AS ok;
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT pg_database_size(current_database()) > 0 AS ok;
DROP TABLE zg_t;
CREATE TEMP TABLE zh_tmp (a int);
INSERT INTO zh_tmp VALUES (1),(2);
TRUNCATE zh_tmp;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM zh_tmp;
DROP TABLE zh_tmp;
CREATE TABLE zh_p (a int);
CREATE OR REPLACE FUNCTION zh_f() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'boom'; END $$;
-- begin-expected-error
-- sqlstate: P0001
-- message-like: boom
-- end-expected-error
SELECT zh_f();
-- begin-expected-error
-- sqlstate: P0001
-- message-like: do boom
-- end-expected-error
DO $$ BEGIN RAISE EXCEPTION 'do boom'; END $$;
DROP FUNCTION zh_f();
DROP TABLE zh_p;
-- begin-expected
-- columns: o | n
-- row: 1663 | pg_default
-- row: 1664 | pg_global
-- end-expected
SELECT oid::text AS o, spcname::text AS n FROM pg_tablespace ORDER BY oid;
