-- source: review-2026-08.md
-- finding: Root cause 2: storage is an I/O conversion, not an assignment cast, and the typmod pass is incomplete
-- area: Types, casts and coercion
-- title: Root cause 2: storage is an I/O conversion, not an assignment cast, and the typmod pass is incomplete
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_as (i int, d date, b bool);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "i" is of type integer but expression is of type text
-- end-expected-error
INSERT INTO zz_as (i) SELECT '1'::text;
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "i" is of type integer but expression is of type boolean
-- end-expected-error
INSERT INTO zz_as (i) VALUES (true);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "d" is of type date but expression is of type integer
-- end-expected-error
INSERT INTO zz_as (d) VALUES (20200101);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type boolean but expression is of type numeric
-- end-expected-error
INSERT INTO zz_as (b) VALUES (0.5);
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "i" is of type integer but expression is of type boolean
-- end-expected-error
UPDATE zz_as SET i = true;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_brace (id int, d date, i int, u uuid);
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type date: "{foo}"
-- end-expected-error
INSERT INTO zz_brace (id, d) VALUES (1, '{foo}');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "{1,2}"
-- end-expected-error
INSERT INTO zz_brace (id, i) VALUES (2, '{1,2}');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type uuid: "{zzz}"
-- end-expected-error
INSERT INTO zz_brace (id, u) VALUES (4, '{zzz}');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_num (n numeric(5));
-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
INSERT INTO zz_num VALUES (123456789);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_num VALUES (1.7);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_nn (n numeric(5,2));
-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
INSERT INTO zz_nn VALUES ('Infinity');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_tp (ts timestamp(0), tm time(0));
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_tp VALUES ('2024-01-01 01:02:03.987', '01:02:03.987');
-- begin-expected
-- columns: ts:timestamp | tm:time
-- row: 2024-01-01 01:02:04 | 01:02:04
-- rowcount: 1
-- end-expected
SELECT ts, tm FROM zz_tp;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_b (a bit(4), b varbit(4));
-- begin-expected-error
-- sqlstate: 22026
-- message-like: bit string length 3 does not match type bit(4)
-- end-expected-error
INSERT INTO zz_b VALUES ('101', '101');
-- begin-expected-error
-- sqlstate: 22026
-- message-like: bit string length 5 does not match type bit(4)
-- end-expected-error
INSERT INTO zz_b VALUES ('10101', '10101');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_cc (c char(3), v varchar(3));
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_cc VALUES ('ab   ', 'ab   ');
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_dn AS numeric(5,2);
-- begin-expected
-- columns: zz_dn:numeric
-- row: 1.23
-- rowcount: 1
-- end-expected
SELECT 1.234::zz_dn;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
SELECT 12345.6::zz_dn;
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_db AS bit(4);
-- begin-expected
-- columns: zz_db:bit
-- row: 1010
-- rowcount: 1
-- end-expected
SELECT '101'::zz_db;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_lsn (l pg_lsn);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type pg_lsn: "garbage"
-- end-expected-error
INSERT INTO zz_lsn VALUES ('garbage');
-- begin-expected
-- columns: pg_lsn:pg_lsn
-- row: 0/16B374D
-- rowcount: 1
-- end-expected
SELECT '0/16b374d'::pg_lsn;
-- begin-expected
-- columns: length:int4
-- row: 63
-- rowcount: 1
-- end-expected
SELECT length(repeat('a',100)::name);
