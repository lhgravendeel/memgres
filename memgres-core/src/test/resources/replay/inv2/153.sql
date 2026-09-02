-- source: investigation-2026-08.md
-- finding: 153
-- title: SQLSTATE is inferred by pattern-matching the message text, defaulting to the class code 42000, and object-kind is not distinguished from object-absence at the l
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_rc (id int);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "zz_nosuch" for table "zz_rc" does not exist
-- end-expected-error
ALTER TABLE zz_rc RENAME CONSTRAINT zz_nosuch TO x;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a int);
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "zz_t"
-- end-expected-error
SELECT nextval('zz_t');
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "bogustype" does not exist
-- end-expected-error
ALTER SEQUENCE zz_s AS bogustype;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_dt (id int, c int CHECK (c > 0), n int NOT NULL);
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zz_dt" violates check constraint "zz_dt_c_check"
-- end-expected-error
INSERT INTO zz_dt VALUES (2, -1, 9);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (i int, v text);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "i" does not exist
-- end-expected-error
CREATE VIEW zz_v AS SELECT i, v FROM zz_t;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "v" of relation "zz_t" does not exist
-- end-expected-error
ALTER TABLE zz_t DROP COLUMN v;
