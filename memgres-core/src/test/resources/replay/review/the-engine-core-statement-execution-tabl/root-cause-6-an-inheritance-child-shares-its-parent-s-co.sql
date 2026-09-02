-- source: review-2026-08.md
-- finding: Root cause 6: an inheritance child shares its parent's `Column` objects and is read back by position
-- area: The engine core: statement execution, tables, windows, joins and deparsing
-- title: Root cause 6: an inheritance child shares its parent's `Column` objects and is read back by position
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_sp (a text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_sc () INHERITS (zz_vf2_sp);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf2_sc ALTER COLUMN a SET STORAGE PLAIN;
-- begin-expected
-- columns: st:text
-- row: x
-- rowcount: 1
-- end-expected
SELECT at.attstorage::text AS st FROM pg_attribute at JOIN pg_class c ON c.oid = at.attrelid
 WHERE c.relname = 'zz_vf2_sp' AND at.attname = 'a';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ip1 (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ip2 (b text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ic () INHERITS (zz_vf2_ip1, zz_vf2_ip2);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_ic VALUES (7, 'hello');
-- begin-expected
-- columns: b:text
-- row: hello
-- rowcount: 1
-- end-expected
SELECT b FROM zz_vf2_ip2 ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pu (a int, b int) PARTITION BY LIST (a);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pu1 (b int, a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_pu1 VALUES (1, 5);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_pu1 VALUES (1, 6);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf2_pu1 ADD CHECK (a IN (5,6));
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf2_pu ATTACH PARTITION zz_vf2_pu1 FOR VALUES IN (5,6);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf2_pu ADD CONSTRAINT zz_vf2_puu UNIQUE (a);
