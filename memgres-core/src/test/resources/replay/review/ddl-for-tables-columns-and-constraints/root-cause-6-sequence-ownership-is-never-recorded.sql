-- source: review-2026-08.md
-- finding: Root cause 6: sequence ownership is never recorded
-- area: DDL for tables, columns and constraints
-- title: Root cause 6: sequence ownership is never recorded
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (i serial, v text);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_t DROP COLUMN i;
-- begin-expected
-- columns: n:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname='zz_t_i_seq';
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_t_i_seq;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_ds;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int DEFAULT nextval('zz_ds'), b text);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "a" of relation "zz_t" does not exist
-- end-expected-error
SELECT pg_get_serial_sequence('zz_t','a');
