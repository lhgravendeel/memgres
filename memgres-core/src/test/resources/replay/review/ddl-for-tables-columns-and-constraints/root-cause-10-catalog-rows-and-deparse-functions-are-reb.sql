-- source: review-2026-08.md
-- finding: Root cause 10: catalog rows and deparse functions are rebuilt from a partial model
-- area: DDL for tables, columns and constraints
-- title: Root cause 10: catalog rows and deparse functions are rebuilt from a partial model
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
ALTER TABLE zz_t SET (fillfactor = 70);
-- begin-expected
-- columns: reloptions:_text
-- rowcount: 0
-- end-expected
SELECT reloptions FROM pg_class WHERE relname='zz_t';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a int, d text, CONSTRAINT zz_nn NOT NULL d);
-- begin-expected
-- columns: conname:name
-- row: zz_nn
-- rowcount: 1
-- end-expected
SELECT conname FROM pg_constraint WHERE conrelid='zz_t'::regclass AND contype='n';
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int, c int GENERATED ALWAYS AS (a*2) STORED);
-- begin-expected
-- columns: generation_expression:varchar
-- rowcount: 0
-- end-expected
SELECT generation_expression FROM information_schema.columns WHERE table_name='zz_t' AND column_name='c';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_k" does not exist
-- end-expected-error
ALTER TABLE zz_k ADD CONSTRAINT zz_k_ck CHECK (a > 0) NOT VALID;
-- begin-expected
-- columns: pg_get_constraintdef:text
-- rowcount: 0
-- end-expected
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname='zz_k_ck';
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_d AS varchar(10) NOT NULL;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (code zz_d);
-- begin-expected
-- columns: constraint_name:name
-- row: zz_nn
-- row: zz_d_not_null
-- rowcount: 2
-- end-expected
SELECT constraint_name FROM information_schema.check_constraints WHERE constraint_name LIKE 'zz\_%';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c1 (id int PRIMARY KEY);
-- begin-expected
-- columns: conname:name | connoinherit:bool
-- row: zz_c1_pkey | t
-- rowcount: 1
-- end-expected
SELECT conname, connoinherit FROM pg_constraint WHERE conrelid='zz_c1'::regclass AND contype='p';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t5" does not exist
-- end-expected-error
CREATE CONSTRAINT TRIGGER zz_tg5c AFTER INSERT ON zz_t5 DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION zz_f5();
-- begin-expected
-- columns: pg_get_triggerdef:text
-- rowcount: 0
-- end-expected
SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname='zz_tg5c';
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int, b int, c int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_v AS SELECT a FROM zz_t;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_l (LIKE zz_v);
-- begin-expected
-- columns: column_name:name
-- row: a
-- rowcount: 1
-- end-expected
SELECT column_name FROM information_schema.columns WHERE table_name='zz_l' ORDER BY ordinal_position;
