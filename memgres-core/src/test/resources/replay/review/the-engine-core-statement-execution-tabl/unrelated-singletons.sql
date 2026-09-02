-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: The engine core: statement execution, tables, windows, joins and deparsing
-- title: Unrelated singletons
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_g1 (n int, a text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_g2 (n numeric, b text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_g1 VALUES (3,'L');
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_g2 VALUES (3.00,'R');
-- begin-expected
-- columns: n:numeric | a:text | b:text
-- row: 3.00 | L | R
-- rowcount: 1
-- end-expected
SELECT n,a,b FROM zz_vf2_g1 JOIN zz_vf2_g2 USING (n);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf2_ef() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zz_vf2_elog VALUES (tg_tag); END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE EVENT TRIGGER zz_vf2_ev ON ddl_command_start EXECUTE FUNCTION zz_vf2_ef();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_elog" does not exist
-- end-expected-error
CREATE TABLE zz_vf2_et1 (i int);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_elog" does not exist
-- end-expected-error
ALTER TABLE zz_vf2_et1 ADD COLUMN j int;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_elog" does not exist
-- end-expected-error
CREATE INDEX zz_vf2_eix ON zz_vf2_et1 (i);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_elog" does not exist
-- end-expected-error
CREATE VIEW zz_vf2_ev1 AS SELECT 1 AS x;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_elog" does not exist
-- end-expected-error
DROP TABLE zz_vf2_et1 CASCADE;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_elog" does not exist
-- end-expected-error
SELECT tag FROM zz_vf2_elog ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_elog" does not exist
-- end-expected-error
CREATE TABLE zz_vf2_pkt (a int PRIMARY KEY);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_elog" does not exist
-- end-expected-error
CREATE TABLE zz_vf2_fk (a int REFERENCES zz_vf2_pkt(a), FOREIGN KEY (a) REFERENCES zz_vf2_pkt(a));
-- begin-expected
-- columns: conname:name
-- rowcount: 0
-- end-expected
SELECT conname FROM pg_constraint
 WHERE conrelid = (SELECT oid FROM pg_class WHERE relname='zz_vf2_fk') AND contype='f' ORDER BY conname;
-- session A
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_elog" does not exist
-- end-expected-error
CREATE TABLE zz_vf2_cc (id int);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_cc" does not exist
-- end-expected-error
INSERT INTO zz_vf2_cc VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_elog" does not exist
-- end-expected-error
ALTER TABLE zz_vf2_cc ADD COLUMN newc int;
-- left uncommitted
-- session B, concurrently
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_cc" does not exist
-- end-expected-error
UPDATE zz_vf2_cc SET newc = 7;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT id, NTILE(2) OVER (ORDER BY id) FROM t;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT id, FIRST_VALUE(v) OVER (ORDER BY id) FROM t;
