-- source: investigation-2026-08.md
-- finding: 332
-- title: pg_class and pg_attribute rows are hand-written literal arrays with constants where pg_dump expects facts: relam 2, reloftype 0, reltoastrelid 0, attstattarget 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pt (id integer NOT NULL, ts date NOT NULL) PARTITION BY RANGE (ts);
-- begin-expected
-- columns: relam:oid | relkind:char
-- row: 0 | p
-- rowcount: 1
-- end-expected
SELECT relam, relkind FROM pg_class WHERE relname = 'zz_vf2_pt';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_t7 (a integer, b integer, c text);
-- begin-expected
-- columns: has_toast:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT reltoastrelid <> 0 AS has_toast FROM pg_class WHERE relname = 'zz_vf2_t7';
