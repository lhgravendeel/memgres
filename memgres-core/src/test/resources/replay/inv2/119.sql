-- source: investigation-2026-08.md
-- finding: 119
-- title: DDL is applied straight onto the live structure with no undo entry, so a rolled-back transaction leaves the change in place — unlike CREATE TYPE/SEQUENCE/INDEX,
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t1 (id int, n int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_tf1() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.n := 99; RETURN NEW; END $$;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_tg1 BEFORE INSERT ON zz_t1 FOR EACH ROW EXECUTE FUNCTION zz_tf1();
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t1 VALUES (1, 5);
-- begin-expected
-- columns: id:int4 | n:int4
-- row: 1 | 5
-- rowcount: 1
-- end-expected
SELECT id, n FROM zz_t1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_e1 AS ENUM ('a','b');
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TYPE zz_e1 ADD VALUE 'c';
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: enumlabel:name
-- row: a
-- row: b
-- rowcount: 2
-- end-expected
SELECT enumlabel FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='zz_e1' ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TYPE zz_e1 RENAME VALUE 'a' TO 'zzz';
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: enumlabel:name
-- row: a
-- row: b
-- rowcount: 2
-- end-expected
SELECT enumlabel FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='zz_e1' ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('zz_s');
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
ALTER SEQUENCE zz_s RESTART WITH 900;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: nextval:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT nextval('zz_s');
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
ALTER SEQUENCE zz_s INCREMENT BY 7 MAXVALUE 5000 CYCLE;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: increment_by:int8 | max_value:int8 | cycle:bool
-- row: 1 | 9223372036854775807 | f
-- rowcount: 1
-- end-expected
SELECT increment_by, max_value, cycle FROM pg_sequences WHERE sequencename='zz_s';
