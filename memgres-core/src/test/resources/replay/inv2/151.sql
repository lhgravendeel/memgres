-- source: investigation-2026-08.md
-- finding: 151
-- title: There is no definition-validation pass: CREATE TABLE and ALTER TABLE validate by trying to build the object, so every rule that is about the definition rather t
-- begin-expected-error
-- sqlstate: 42710
-- message-like: constraint "k" for domain "zz_d8" already exists
-- end-expected-error
CREATE DOMAIN zz_d8 AS int CONSTRAINT k CHECK (VALUE > 0) CONSTRAINT k CHECK (VALUE < 9);
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
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t VALUES (NULL);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_t ADD CONSTRAINT zz_nn NOT NULL a NOT VALID;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
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
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int, b int GENERATED ALWAYS AS ('abc') STORED);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_u (a int, c int GENERATED ALWAYS AS (a*2) STORED);
-- begin-expected-error
-- sqlstate: 42611
-- message-like: cannot specify USING when altering type of generated column
-- end-expected-error
ALTER TABLE zz_u ALTER COLUMN c TYPE text USING c::text;
