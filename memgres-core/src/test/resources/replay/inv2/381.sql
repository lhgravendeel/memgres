-- source: investigation-2026-08.md
-- finding: 381
-- title: Unrelated singletons in this area
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
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf2_ef() does not exist
-- end-expected-error
CREATE EVENT TRIGGER zz_vf2_ev ON ddl_command_start EXECUTE FUNCTION zz_vf2_ef();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_et1 (i int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf2_et1 ADD COLUMN j int;
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_eix ON zz_vf2_et1 (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_ev1 AS SELECT 1 AS x;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf2_et1 CASCADE;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_elog" does not exist
-- end-expected-error
SELECT tag FROM zz_vf2_elog ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pkt (a int PRIMARY KEY);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_fk (a int REFERENCES zz_vf2_pkt(a), FOREIGN KEY (a) REFERENCES zz_vf2_pkt(a));
-- begin-expected
-- columns: conname:name
-- row: zz_vf2_fk_a_fkey
-- row: zz_vf2_fk_a_fkey1
-- rowcount: 2
-- end-expected
SELECT conname FROM pg_constraint WHERE conrelid=(SELECT oid FROM pg_class WHERE relname='zz_vf2_fk') AND contype='f' ORDER BY conname;
-- session A: BEGIN; ALTER TABLE zz_vf2_cc ADD COLUMN newc int;  (uncommitted)
-- session B: UPDATE zz_vf2_cc SET newc = 7;
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
