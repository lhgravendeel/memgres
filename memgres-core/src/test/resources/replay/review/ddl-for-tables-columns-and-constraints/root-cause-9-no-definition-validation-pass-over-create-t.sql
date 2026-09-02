-- source: review-2026-08.md
-- finding: Root cause 9: no definition-validation pass over CREATE TABLE / ALTER TABLE
-- area: DDL for tables, columns and constraints
-- title: Root cause 9: no definition-validation pass over CREATE TABLE / ALTER TABLE
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zz_t (a int CHECK (nosuchcol > 0));
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" named in key does not exist
-- end-expected-error
CREATE TABLE zz_t (a int, UNIQUE (nosuchcol));
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "a" appears twice in primary key constraint
-- end-expected-error
CREATE TABLE zz_t2 (a int, PRIMARY KEY (a, a));
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: access method "gin" does not support exclusion constraints
-- end-expected-error
CREATE TABLE zz_t3 (a int, EXCLUDE USING gin (a WITH =));
-- begin-expected-error
-- sqlstate: 42601
-- message-like: multiple default values specified for column "a" of table "zz_a"
-- end-expected-error
CREATE TABLE zz_a (a int DEFAULT 1 DEFAULT 2);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting NULL/NOT NULL declarations for column "a" of table "zz_b"
-- end-expected-error
CREATE TABLE zz_b (a int NOT NULL NULL);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: multiple default values specified for column "i" of table "zz_c"
-- end-expected-error
CREATE TABLE zz_c (i serial DEFAULT 1);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: both identity and generation expression specified for column "b" of table "zz_d"
-- end-expected-error
CREATE TABLE zz_d (a int, b int GENERATED ALWAYS AS (a) STORED GENERATED ALWAYS AS IDENTITY);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
CREATE TABLE zz_t (a int, b int GENERATED ALWAYS AS ('abc') STORED);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_src (a int, b text);
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "a" specified more than once
-- end-expected-error
CREATE TABLE zz_t (a int, LIKE zz_src);
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use "list" partition strategy with more than one column
-- end-expected-error
CREATE TABLE zz_t (a int, b int) PARTITION BY LIST (a, b);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_p_1" does not exist
-- end-expected-error
ALTER TABLE zz_p_1 ADD COLUMN c int;
-- zz_p_1 is a partition
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot create temporary relation in non-temporary schema
-- end-expected-error
CREATE TEMP TABLE public.zz_tt (a int);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
ALTER TABLE zz_t ADD COLUMN e text COLLATE "nosuch_collation";
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
ALTER TABLE zz_t CLUSTER ON zz_nosuchindex;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
ALTER TABLE zz_t RENAME COLUMN a TO c, ADD COLUMN d int;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
CREATE TABLE zz_u (a int(5));
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a int);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "serial" does not exist
-- end-expected-error
ALTER TABLE zz_t ALTER COLUMN a TYPE serial;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_u (d int UNIQUE DEFERRABLE);
-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot use a deferrable unique constraint for referenced table "zz_u"
-- end-expected-error
CREATE TABLE zz_c (p int REFERENCES zz_u(d));
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_d9 AS int CHECK (VALUE > 0) CHECK (VALUE < 10);
-- begin-expected
-- columns: conname:name
-- row: zz_d9_check
-- row: zz_d9_check1
-- rowcount: 2
-- end-expected
SELECT conname FROM pg_constraint WHERE contypid='zz_d9'::regtype ORDER BY conname;
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zz_d9 violates check constraint "zz_d9_check1"
-- end-expected-error
SELECT 50::zz_d9;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_k1 (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_k1 VALUES (0);
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
ALTER TABLE zz_k1 ADD CONSTRAINT zz_k1_ck CHECK (100 / a > 0);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: data type integer has no default operator class for access method "gist"
-- end-expected-error
CREATE TABLE zz_x6 (a int, EXCLUDE USING gist (a WITH <>));
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_x6" does not exist
-- end-expected-error
INSERT INTO zz_x6 VALUES (1);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_x6" does not exist
-- end-expected-error
INSERT INTO zz_x6 VALUES (2);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t VALUES (NULL);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_t ADD CONSTRAINT zz_nn NOT NULL a NOT VALID;
