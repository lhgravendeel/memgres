-- source: review-2026-08.md
-- finding: Root cause 9: the error Position is guessed by string-searching the message text
-- area: Wire protocol and error-report fidelity
-- title: Root cause 9: the error Position is guessed by string-searching the message text
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT 'abc', 'abc'::int;
-- PG 15  memgres 9
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT 'abc'::int;
-- PG 8   memgres 9
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT $$abc$$::int;
-- PG 8   memgres 10
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT e'abc'::int;
-- PG 8   memgres 10
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT U&'abc'::int;
-- PG 8   memgres 11
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_nosuch" does not exist
-- end-expected-error
SELECT 'zz_vf_nosuch'::text, * FROM zz_vf_nosuch;
-- PG 37  memgres 9
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_dup (id int);
-- run twice         -- PG none memgres 14
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "a" specified more than once
-- end-expected-error
CREATE TABLE zz_vf_dupcol (a int, a int);
-- PG none memgres 4
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: table "zz_vf_nosuchtable" does not exist
-- end-expected-error
DROP TABLE zz_vf_nosuchtable;
-- PG none memgres 12
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "nosuchunit" not recognized for type timestamp with time zone
-- end-expected-error
SELECT date_trunc('nosuchunit', now());
-- PG none memgres 20
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf_nosuchfunc(integer) does not exist
-- end-expected-error
SELECT zz_vf_nosuchfunc(1);
-- PG 8   memgres none
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_gb" does not exist
-- end-expected-error
SELECT id FROM zz_vf_gb GROUP BY qq;
-- PG 8   memgres none
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
-- PG 41  memgres 8
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_tm" does not exist
-- end-expected-error
INSERT INTO zz_vf_tm (id) VALUES (1, 2);
-- PG 38  memgres 1;
