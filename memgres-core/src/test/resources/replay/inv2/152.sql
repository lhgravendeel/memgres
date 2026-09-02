-- source: investigation-2026-08.md
-- finding: 152
-- title: Catalog rows and deparse functions are rebuilt from a model that does not keep everything the DDL said: reloptions, an explicit NOT NULL constraint name, genera
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_t SET (fillfactor = 70);
-- begin-expected
-- columns: reloptions:_text
-- row: {fillfactor=70}
-- rowcount: 1
-- end-expected
SELECT reloptions FROM pg_class WHERE relname='zz_t';
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int, d text, CONSTRAINT zz_nn NOT NULL d);
-- begin-expected
-- columns: conname:name | contype:char
-- rowcount: 0
-- end-expected
SELECT conname, contype FROM pg_constraint WHERE conrelid='zz_t'::regclass AND contype='n' ORDER BY conname;
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
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_k (a int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_k ADD CONSTRAINT zz_k_ck CHECK (a > 0) NOT VALID;
-- begin-expected
-- columns: pg_get_constraintdef:text
-- row: CHECK ((a > 0)) NOT VALID
-- rowcount: 1
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
-- row: zz_k_ck
-- row: zz_d_not_null
-- rowcount: 2
-- end-expected
SELECT constraint_name FROM information_schema.check_constraints WHERE constraint_name LIKE 'zz\_%';
-- begin-expected
-- columns: attnotnull:bool
-- rowcount: 0
-- end-expected
SELECT attnotnull FROM pg_attribute WHERE attrelid='zz_t'::regclass AND attname='code';
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
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c2 (a int, EXCLUDE USING btree (a WITH =));
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT conexclop IS NULL FROM pg_constraint WHERE conrelid='zz_c2'::regclass AND contype='x';
