-- source: investigation-2026-08.md
-- finding: 249
-- title: A domain's constraints stop at a composite-type boundary, and a composite type is not a resolvable base type: the composite constructor coerces fields to the ba
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_d AS int CHECK (VALUE > 0);
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_dt AS text NOT NULL CHECK (VALUE = lower(VALUE));
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_c AS (a zz_d, b zz_dt);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (id int, c zz_c);
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zz_d violates check constraint "zz_d_check"
-- end-expected-error
INSERT INTO zz_t VALUES (2, ROW(-1, 'ok')::zz_c);
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zz_dt violates check constraint "zz_dt_check"
-- end-expected-error
INSERT INTO zz_t VALUES (3, ROW(5, 'NOTLOWER')::zz_c);
-- begin-expected-error
-- sqlstate: 23502
-- message-like: domain zz_dt does not allow null values
-- end-expected-error
INSERT INTO zz_t VALUES (4, ROW(5, NULL)::zz_c);
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zz_d violates check constraint "zz_d_check"
-- end-expected-error
SELECT ARRAY[ROW(5,'a')::zz_c, ROW(-1,'b')::zz_c];
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "zz_c" already exists
-- end-expected-error
CREATE TYPE zz_c AS (a int, b text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_dc AS zz_c CHECK ((VALUE).a > 3);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (id int, cs zz_c[]);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c" is of type zz_c but expression is of type zz_c[]
-- end-expected-error
INSERT INTO zz_t VALUES (1, ARRAY[ROW(1,'x')::zz_c]);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "cs" does not exist
-- end-expected-error
SELECT id, (cs[1]).a, (cs[1]).b FROM zz_t;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "cs" does not exist
-- end-expected-error
SELECT id, u.a, u.b FROM zz_t, unnest(cs) AS u;
