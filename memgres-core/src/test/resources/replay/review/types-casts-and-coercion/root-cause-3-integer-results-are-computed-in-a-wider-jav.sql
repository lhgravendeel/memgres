-- source: review-2026-08.md
-- finding: Root cause 3: integer results are computed in a wider Java type and narrowed only if they happen to fit
-- area: Types, casts and coercion
-- title: Root cause 3: integer results are computed in a wider Java type and narrowed only if they happen to fit
-- begin-expected
-- columns: ?column?:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT 5::int2 / 2::int4;
-- begin-expected
-- columns: pg_typeof:regtype
-- row: bigint
-- rowcount: 1
-- end-expected
SELECT pg_typeof(1::int2 + 1::int8);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT 2147483647::int4 + 1::int2;
-- begin-expected
-- columns: ?column?:int4
-- row: -2147483648
-- rowcount: 1
-- end-expected
SELECT 1::int4 << 31;
-- begin-expected
-- columns: ?column?:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT 1 << 32;
-- begin-expected
-- columns: ?column?:int4
-- row: 4
-- rowcount: 1
-- end-expected
SELECT 1024 >> 40;
-- begin-expected
-- columns: ?column?:int2
-- row: -32768
-- rowcount: 1
-- end-expected
SELECT 1::int2 << 15;
-- begin-expected
-- columns: pg_typeof:regtype
-- row: integer
-- rowcount: 1
-- end-expected
SELECT pg_typeof(-2147483648);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT (-2147483648) - 1;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT (-2147483648) * (-1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s START 9223372036854775807;
-- begin-expected
-- columns: nextval:int8
-- row: 9223372036854775807
-- rowcount: 1
-- end-expected
SELECT nextval('zz_s');
-- begin-expected-error
-- sqlstate: 2200H
-- message-like: nextval: reached maximum value of sequence "zz_s" (9223372036854775807)
-- end-expected-error
SELECT nextval('zz_s');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_id (a int GENERATED ALWAYS AS IDENTITY, b text);
-- begin-expected
-- columns: max_value:int8
-- row: 2147483647
-- rowcount: 1
-- end-expected
SELECT max_value FROM pg_sequences WHERE sequencename='zz_id_a_seq';
-- begin-expected
-- columns: setval:int8
-- row: 2147483647
-- rowcount: 1
-- end-expected
SELECT setval('zz_id_a_seq', 2147483647);
-- begin-expected-error
-- sqlstate: 2200H
-- message-like: nextval: reached maximum value of sequence "zz_id_a_seq" (2147483647)
-- end-expected-error
INSERT INTO zz_id (b) VALUES ('x');
-- begin-expected
-- columns: a:serial
-- rowcount: 0
-- end-expected
SELECT a FROM zz_id;
-- begin-expected
-- columns: v:int8
-- row: 9999999999
-- rowcount: 1
-- end-expected
SELECT * FROM xmltable('/r/x' PASSING '<r><x><v>9999999999</v></x></r>' COLUMNS v bigint PATH 'v');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_sk (a int);
-- begin-expected
-- ok: 5
-- end-expected
INSERT INTO zz_sk VALUES (1),(2),(3),(4),(5);
-- begin-expected
-- columns: a:int4
-- row: 3
-- row: 4
-- row: 5
-- rowcount: 3
-- end-expected
SELECT a FROM zz_sk ORDER BY a OFFSET 2 FOR UPDATE SKIP LOCKED;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_g VALUES (1);
-- begin-expected-error
-- sqlstate: 54023
-- message-like: GROUPING must have fewer than 32 arguments
-- end-expected-error
SELECT grouping(a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a) FROM zz_g GROUP BY a;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT information_schema._pg_char_octet_length(1043, 1000000000);
