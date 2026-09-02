-- source: investigation-2026-08.md
-- finding: 158
-- title: Names are stored and looked up as bare, case-folded strings in flat maps. The parser idiom `String x = readIdentifier(); if (match(DOT)) x = readIdentifier();` 
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_vs;
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_v AS SELECT 1 AS a;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_vs.zz_vf_v AS SELECT 2 AS a;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: a:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT a FROM public.zz_vf_v;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_fs2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ft (id int, tag text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_gf() RETURNS trigger AS $$ BEGIN NEW.tag := 'public'; RETURN NEW; END; $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_fs2.zz_vf_gf() RETURNS trigger AS $$ BEGIN NEW.tag := 'schema'; RETURN NEW; END; $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_trg2 BEFORE INSERT ON zz_vf_ft FOR EACH ROW EXECUTE FUNCTION zz_vf_fs2.zz_vf_gf();
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_ft VALUES (1, 'orig');
-- begin-expected
-- columns: id:int4 | tag:text
-- row: 1 | schema
-- rowcount: 1
-- end-expected
SELECT id, tag FROM zz_vf_ft ORDER BY id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_ds;
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_vf_dm AS int;
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_vf_ds.zz_vf_dm AS int;
-- begin-expected
-- ok: 0
-- end-expected
ALTER DOMAIN zz_vf_ds.zz_vf_dm SET NOT NULL;
-- begin-expected
-- columns: r:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT NULL::zz_vf_dm IS NULL AS r;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_s1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_t3 (x int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_s1.zz_vf_t3 (x int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_f() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_tg2 AFTER INSERT ON zz_vf_t3 FOR EACH ROW EXECUTE FUNCTION zz_vf_f();
-- begin-expected
-- columns: schemaname:name | hastriggers:bool
-- row: public | t
-- row: zz_vf_s1 | f
-- rowcount: 2
-- end-expected
SELECT schemaname, hastriggers FROM pg_tables WHERE tablename='zz_vf_t3' ORDER BY schemaname;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ca (id int primary key, v text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_cb (id int primary key, v text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_trgfn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_t1 BEFORE INSERT ON zz_vf_ca FOR EACH ROW EXECUTE FUNCTION zz_vf_trgfn();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_t1 BEFORE INSERT ON zz_vf_cb FOR EACH ROW EXECUTE FUNCTION zz_vf_trgfn();
-- begin-expected
-- columns: trigger_name:name | event_object_table:name
-- row: zz_vf_t1 | zz_vf_ca
-- row: zz_vf_t1 | zz_vf_cb
-- rowcount: 2
-- end-expected
SELECT trigger_name, event_object_table FROM information_schema.triggers WHERE trigger_name='zz_vf_t1' ORDER BY event_object_table;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_cv1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_cv2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_cv1.b (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_cv2.b (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_cv1.zz_vf_vw AS SELECT id AS one FROM zz_vf_cv1.b;
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_cv2.zz_vf_vw AS SELECT id AS two FROM zz_vf_cv2.b;
-- begin-expected
-- columns: d:text
-- row:  SELECT id AS two\n   FROM zz_vf_cv2.b;
-- rowcount: 1
-- end-expected
SELECT pg_get_viewdef('zz_vf_cv2.zz_vf_vw'::regclass, true) AS d;
