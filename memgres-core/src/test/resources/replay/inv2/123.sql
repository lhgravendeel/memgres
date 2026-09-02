-- source: investigation-2026-08.md
-- finding: 123
-- title: The dependency from a sequence to its owning column is not modelled: no schema check on OWNED BY, no drop propagation, and setval's third argument is not strict
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s2;
-- begin-expected
-- columns: setval:int8
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT setval('zz_s2', 400, NULL);
-- begin-expected
-- columns: last_value:int8 | is_called:bool
-- row: 1 | f
-- rowcount: 1
-- end-expected
SELECT last_value, is_called FROM zz_s2;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('zz_s2');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a serial, b text);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_t DROP COLUMN a;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_class WHERE relname='zz_t_a_seq';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t2 (a int GENERATED ALWAYS AS IDENTITY, b text);
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop sequence zz_t2_a_seq because column a of table zz_t2 requires it
-- end-expected-error
DROP SEQUENCE zz_t2_a_seq CASCADE;
-- begin-expected
-- columns: a:serial
-- row: 1
-- rowcount: 1
-- end-expected
INSERT INTO zz_t2 (b) VALUES ('x') RETURNING a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_o;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_o.t (a int);
-- begin-expected-error
-- sqlstate: 55000
-- message-like: sequence must be in same schema as table it is linked to
-- end-expected-error
CREATE SEQUENCE zz_os2 OWNED BY zz_o.t.a;
-- begin-expected
-- columns: pg_get_serial_sequence:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT pg_get_serial_sequence('zz_o.t','a');
