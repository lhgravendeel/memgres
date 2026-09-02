-- source: review-2026-08.md
-- finding: Root cause 5: pg_class and pg_attribute rows are literal arrays with constants where pg_dump expects facts
-- area: pg_dump fidelity
-- title: Root cause 5: pg_class and pg_attribute rows are literal arrays with constants where pg_dump expects facts
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
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_t1 (c_vc varchar(44));
-- begin-expected
-- columns: attstattarget:int2
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT attstattarget FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
 WHERE c.relname = 'zz_vf2_t1' AND a.attname = 'c_vc';
