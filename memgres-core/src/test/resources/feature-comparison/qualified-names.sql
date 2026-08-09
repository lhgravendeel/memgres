CREATE SCHEMA zz_qn_vs;

CREATE VIEW zz_qn_v AS SELECT 1 AS a;

BEGIN;

CREATE VIEW zz_qn_vs.zz_qn_v AS SELECT 2 AS a;

ROLLBACK;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT a FROM public.zz_qn_v;

CREATE SCHEMA zz_qn_n1;

CREATE TYPE zz_qn_n1.zz_qn_e AS ENUM ('a');

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: type "zz_qn_e" does not exist
-- end-expected-error
SELECT 'a'::zz_qn_e;

-- begin-expected
-- columns: zz_qn_e
-- row: a
-- end-expected
SELECT 'a'::zz_qn_n1.zz_qn_e;

CREATE SCHEMA zz_qn_qv;

CREATE TABLE zz_qn_qv.t (a int);

-- begin-expected
-- columns: a
-- row: f
-- end-expected
SELECT pg_table_is_visible('zz_qn_qv.t'::regclass) AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT pg_table_is_visible('zz_qn_v'::regclass) AS a;

CREATE TABLE zz_qn_shadow (src text);

INSERT INTO zz_qn_shadow VALUES ('permanent');

CREATE TEMP TABLE zz_qn_shadow (src text);

INSERT INTO zz_qn_shadow VALUES ('temp');

-- begin-expected
-- columns: src
-- row: temp
-- end-expected
SELECT src FROM zz_qn_shadow;

SET search_path = public, pg_temp;

-- begin-expected
-- columns: src
-- row: permanent
-- end-expected
SELECT src FROM zz_qn_shadow;

RESET search_path;

CREATE SCHEMA zz_qn_c1;

CREATE SCHEMA zz_qn_c2;

CREATE TABLE zz_qn_c1.b (id int);

CREATE TABLE zz_qn_c2.b (id int);

CREATE VIEW zz_qn_c1.zz_qn_vw AS SELECT id AS one FROM zz_qn_c1.b;

CREATE VIEW zz_qn_c2.zz_qn_vw AS SELECT id AS two FROM zz_qn_c2.b;

-- begin-expected
-- columns: d
-- row:  SELECT id AS two
   FROM zz_qn_c2.b;
-- end-expected
-- begin-expected
-- columns: d
-- row: SELECT id AS two    FROM zz_qn_c2.b;
-- end-expected
SELECT trim(replace(pg_get_viewdef('zz_qn_c2.zz_qn_vw'::regclass), chr(10), ' ')) AS d;

CREATE SCHEMA zz_qn_ds;

CREATE DOMAIN zz_qn_dm AS int;

CREATE DOMAIN zz_qn_ds.zz_qn_dm AS int;

ALTER DOMAIN zz_qn_ds.zz_qn_dm SET NOT NULL;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT NULL::zz_qn_dm IS NULL AS r;

CREATE SCHEMA zz_qn_s1;

CREATE TABLE zz_qn_tt (id int);

CREATE TABLE zz_qn_s1.zz_qn_tt (id int);

CREATE FUNCTION zz_qn_tgf() RETURNS trigger AS 'BEGIN RETURN NEW; END' LANGUAGE plpgsql;

CREATE TRIGGER zz_qn_tr BEFORE INSERT ON zz_qn_tt FOR EACH ROW EXECUTE FUNCTION zz_qn_tgf();

-- begin-expected
-- columns: schemaname|hastriggers
-- row: public|t
-- row: zz_qn_s1|f
-- end-expected
SELECT schemaname, hastriggers FROM pg_tables WHERE tablename = 'zz_qn_tt' ORDER BY schemaname;

CREATE TABLE zz_qn_ca (id int primary key);

CREATE TABLE zz_qn_cb (id int primary key);

CREATE TRIGGER zz_qn_t1 BEFORE INSERT ON zz_qn_ca FOR EACH ROW EXECUTE FUNCTION zz_qn_tgf();

CREATE TRIGGER zz_qn_t1 BEFORE INSERT ON zz_qn_cb FOR EACH ROW EXECUTE FUNCTION zz_qn_tgf();

-- begin-expected
-- columns: trigger_name|event_object_table
-- row: zz_qn_t1|zz_qn_ca
-- row: zz_qn_t1|zz_qn_cb
-- end-expected
SELECT trigger_name, event_object_table FROM information_schema.triggers WHERE trigger_name = 'zz_qn_t1' ORDER BY event_object_table;

CREATE SCHEMA zz_qn_fs;

CREATE TABLE zz_qn_ft (id int, tag text);

CREATE FUNCTION zz_qn_gf() RETURNS trigger AS $$ BEGIN NEW.tag := 'public'; RETURN NEW; END; $$ LANGUAGE plpgsql;

CREATE FUNCTION zz_qn_fs.zz_qn_gf() RETURNS trigger AS $$ BEGIN NEW.tag := 'schema'; RETURN NEW; END; $$ LANGUAGE plpgsql;

CREATE TRIGGER zz_qn_trg2 BEFORE INSERT ON zz_qn_ft FOR EACH ROW EXECUTE FUNCTION zz_qn_fs.zz_qn_gf();

INSERT INTO zz_qn_ft VALUES (1, 'orig');

-- begin-expected
-- columns: id|tag
-- row: 1|schema
-- end-expected
SELECT id, tag FROM zz_qn_ft ORDER BY id;

CREATE SCHEMA zz_qn_qs;

CREATE SCHEMA zz_qn_qs2;

CREATE FUNCTION zz_qn_qs.who() RETURNS text LANGUAGE sql AS $$ SELECT 'one'::text $$;

CREATE FUNCTION zz_qn_qs2.who() RETURNS text LANGUAGE sql AS $$ SELECT 'two'::text $$;

-- begin-expected-error
-- sqlstate: 42723
-- message-like: ERROR: function who() already exists in schema "zz_qn_qs2"
-- end-expected-error
ALTER FUNCTION zz_qn_qs.who() SET SCHEMA zz_qn_qs2;

-- begin-expected
-- columns: who
-- row: two
-- end-expected
SELECT zz_qn_qs2.who();

-- begin-expected
-- columns: who
-- row: one
-- end-expected
SELECT zz_qn_qs.who();

