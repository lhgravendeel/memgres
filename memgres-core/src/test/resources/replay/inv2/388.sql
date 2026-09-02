-- source: investigation-2026-08.md
-- finding: 388
-- title: enrichErrorPosition guesses a Position by extracting the first double-quoted name from the message text and doing lowerSql.indexOf(name). It therefore lands one
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT 'abc', 'abc'::int;
-- PG 15, memgres 9
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT 'abc'::int;
-- PG 8,  memgres 9
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT $$abc$$::int;
-- PG 8,  memgres 10
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_nosuch" does not exist
-- end-expected-error
SELECT 'zz_vf_nosuch'::text, * FROM zz_vf_nosuch;
-- PG 37, memgres 9
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_dup (id int);
-- (twice) PG no P, memgres P=14
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: table "zz_vf_nosuchtable" does not exist
-- end-expected-error
DROP TABLE zz_vf_nosuchtable;
-- PG no P, memgres P=12
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf_nosuchfunc(integer) does not exist
-- end-expected-error
SELECT zz_vf_nosuchfunc(1);
-- PG 8,  memgres none
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_gb" does not exist
-- end-expected-error
SELECT id FROM zz_vf_gb GROUP BY qq;
-- PG 8,  memgres none
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ms" does not exist
-- end-expected-error
INSERT INTO zz_vf_ms VALUES (1);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch_zz_vf" does not exist
-- end-expected-error
SELECT nosuch_zz_vf;
-- PG 41, memgres 8;
