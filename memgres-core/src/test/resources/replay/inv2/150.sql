-- source: investigation-2026-08.md
-- finding: 150
-- title: The DDL grammar predates several standard PostgreSQL forms (SET EXPRESSION AS, per-column SET/RESET, INCLUDE, column-level NOT NULL ... NO INHERIT, typed tables
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a int, c int GENERATED ALWAYS AS (a+1) STORED);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_t ALTER COLUMN c SET EXPRESSION AS (a*10);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_t ALTER COLUMN a SET (n_distinct = 5);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int, b int, UNIQUE (a) INCLUDE (b));
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int NOT NULL NO INHERIT);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_ct AS (x int, y text);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t OF zz_ct;
