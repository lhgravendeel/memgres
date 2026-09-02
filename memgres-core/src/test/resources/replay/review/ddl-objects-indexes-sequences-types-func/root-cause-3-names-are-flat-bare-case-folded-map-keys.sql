-- source: review-2026-08.md
-- finding: Root cause 3: names are flat, bare, case-folded map keys
-- area: DDL objects: indexes, sequences, types, functions, triggers, rules, views, partitioning and the catalogs that describe them
-- title: Root cause 3: names are flat, bare, case-folded map keys
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
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_vf_ts" does not exist
-- end-expected-error
DROP TRIGGER zz_vf_trg3 ON zz_vf_ts.zz_vf_tt;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_tsch;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_ce2 AS ENUM ('a');
-- begin-expected
-- ok: 0
-- end-expected
ALTER TYPE zz_vf_ce2 SET SCHEMA zz_vf_tsch;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_vf_ce2" does not exist
-- end-expected-error
SELECT 'a'::zz_vf_ce2;
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
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf_f() does not exist
-- end-expected-error
CREATE TRIGGER zz_vf_tg2 AFTER INSERT ON zz_vf_t3 FOR EACH ROW EXECUTE FUNCTION zz_vf_f();
-- begin-expected
-- columns: schemaname:name | hastriggers:bool
-- row: public | f
-- row: zz_vf_s1 | f
-- rowcount: 2
-- end-expected
SELECT schemaname, hastriggers FROM pg_tables WHERE tablename='zz_vf_t3' ORDER BY schemaname;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_ca" does not exist
-- end-expected-error
CREATE TRIGGER zz_vf_t1 BEFORE INSERT ON zz_vf_ca FOR EACH ROW EXECUTE FUNCTION zz_vf_trgfn();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_cb" does not exist
-- end-expected-error
CREATE TRIGGER zz_vf_t1 BEFORE INSERT ON zz_vf_cb FOR EACH ROW EXECUTE FUNCTION zz_vf_trgfn();
-- begin-expected
-- columns: trigger_name:name | event_object_table:name
-- rowcount: 0
-- end-expected
SELECT trigger_name, event_object_table FROM information_schema.triggers
 WHERE trigger_name='zz_vf_t1' ORDER BY event_object_table;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_cv1.b" does not exist
-- end-expected-error
CREATE VIEW zz_vf_cv1.zz_vf_vw AS SELECT id AS one FROM zz_vf_cv1.b;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_cv2.b" does not exist
-- end-expected-error
CREATE VIEW zz_vf_cv2.zz_vf_vw AS SELECT id AS two FROM zz_vf_cv2.b;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_cv2.zz_vf_vw" does not exist
-- end-expected-error
SELECT pg_get_viewdef('zz_vf_cv2.zz_vf_vw'::regclass, true) AS d;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_rs3;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_rs3.zz_vf_rt3 (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE RULE zz_vf_r3 AS ON DELETE TO zz_vf_rs3.zz_vf_rt3 DO INSTEAD NOTHING;
-- begin-expected
-- columns: schemaname:name | tablename:name
-- row: zz_vf_rs3 | zz_vf_rt3
-- rowcount: 1
-- end-expected
SELECT schemaname, tablename FROM pg_rules WHERE rulename='zz_vf_r3';
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_sa;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf_sb;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_sa.s START 1 CACHE 10;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_sb.s START 1 CACHE 10;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('zz_vf_sa.s');
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('zz_vf_sb.s');
-- begin-expected
-- columns: nextval:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT nextval('zz_vf_sb.s');
-- begin-expected
-- columns: nextval:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT nextval('zz_vf_sa.s');
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_cs START 1 CACHE 10;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('zz_vf_cs');
-- begin-expected
-- ok: 0
-- end-expected
DROP SEQUENCE zz_vf_cs;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_cs START 1 CACHE 10;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('zz_vf_cs');
-- begin-expected
-- columns: nextval:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT nextval('zz_vf_cs');
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_vf_q START 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE "ZZ_VF_Q" START 1;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('"ZZ_VF_Q"');
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE "ZzVfSeqQ";
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zzvfseqq" does not exist
-- end-expected-error
SELECT nextval('ZzVfSeqQ');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_r1" does not exist
-- end-expected-error
CREATE RULE zz_vf_r1_b AS ON INSERT TO zz_vf_r1 DO ALSO INSERT INTO zz_vf_r1log(m) VALUES ('b');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_r1" does not exist
-- end-expected-error
CREATE RULE zz_vf_r1_a AS ON INSERT TO zz_vf_r1 DO ALSO INSERT INTO zz_vf_r1log(m) VALUES ('a');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_r1" does not exist
-- end-expected-error
INSERT INTO zz_vf_r1 VALUES (1);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_r1log" does not exist
-- end-expected-error
SELECT string_agg(m, ',' ORDER BY seq) AS fired FROM zz_vf_r1log;
