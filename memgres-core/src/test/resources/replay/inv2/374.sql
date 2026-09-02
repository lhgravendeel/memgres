-- source: investigation-2026-08.md
-- finding: 374
-- title: A DDL statement's side effects are not scoped to what it named: the SERIAL sequence is created before the guarded block and the rollback removes only the table,
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_seqt (id serial, v int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER SEQUENCE zz_vf2_seqt_id_seq OWNED BY NONE;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf2_seqt;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('zz_vf2_seqt_id_seq');
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: multiple primary keys for table "zz_vf2_s" are not allowed
-- end-expected-error
CREATE TABLE zz_vf2_s (id serial PRIMARY KEY, b int PRIMARY KEY);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_s_id_seq" does not exist
-- end-expected-error
SELECT nextval('zz_vf2_s_id_seq');
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_v AS SELECT 1 AS x;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf2_v" already exists
-- end-expected-error
CREATE TABLE zz_vf2_v AS SELECT 2 AS y;
-- begin-expected
-- columns: x:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT * FROM zz_vf2_v;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf2_sq;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf2_sq" already exists
-- end-expected-error
CREATE TABLE zz_vf2_sq AS SELECT 1 AS x;
