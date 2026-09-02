-- source: investigation-2026-08.md
-- finding: 141
-- title: Unrelated singletons in this area
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_r5 (i int primary key, v text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_r5v AS SELECT i, v FROM zz_vf_r5;
-- begin-expected
-- ok: 0
-- end-expected
CREATE RULE zz_vf_r5_r AS ON INSERT TO zz_vf_r5v DO INSTEAD INSERT INTO zz_vf_r5 VALUES (NEW.i, NEW.v);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot perform INSERT RETURNING on relation "zz_vf_r5v"
-- end-expected-error
INSERT INTO zz_vf_r5v VALUES (1,'a') RETURNING i;
-- begin-expected
-- columns: i:int4 | v:text
-- rowcount: 0
-- end-expected
SELECT i, v FROM zz_vf_r5 ORDER BY i;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_r6 (i int primary key, v text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_r6log (m text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE RULE zz_vf_r6_h AS ON INSERT TO zz_vf_r6 DO ALSO INSERT INTO zz_vf_r6log VALUES ('i');
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: INSERT with ON CONFLICT clause cannot be used with table that has INSERT or UPDATE rules
-- end-expected-error
INSERT INTO zz_vf_r6 VALUES (1,'a') ON CONFLICT (i) DO NOTHING;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot execute MERGE on relation "zz_vf_r6"
-- end-expected-error
MERGE INTO zz_vf_r6 t USING (SELECT 1 AS i) s ON t.i=s.i WHEN NOT MATCHED THEN DO NOTHING;
-- begin-expected
-- ok: 0
-- end-expected
CREATE DOMAIN zz_vf_wd AS int DEFAULT 7;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_wt (a int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_wt VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_wt ADD COLUMN b zz_vf_wd;
-- begin-expected
-- columns: a:int4 | b:int4
-- row: 1 | 7
-- row: 2 | 7
-- rowcount: 2
-- end-expected
SELECT a, b FROM zz_vf_wt ORDER BY a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_v3t (i int, n int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_v3v AS SELECT i, n*2 AS dn FROM zz_vf_v3t;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot insert into column "dn" of view "zz_vf_v3v"
-- end-expected-error
INSERT INTO zz_vf_v3v VALUES (1, 4);
-- begin-expected
-- columns: i:int4 | n:int4
-- rowcount: 0
-- end-expected
SELECT i, n FROM zz_vf_v3t;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_wb (a int, b int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_wlog (m text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf_w1 AS SELECT a, b FROM zz_vf_wb;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_wf() RETURNS trigger AS $$ BEGIN INSERT INTO zz_vf_wlog VALUES (TG_OP || ':' || NEW.a); RETURN NEW; END; $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_wtg BEFORE INSERT ON zz_vf_wb FOR EACH ROW EXECUTE FUNCTION zz_vf_wf();
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_wb VALUES (1, 2);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_w1 VALUES (3, 4);
-- begin-expected
-- columns: m:text
-- row: INSERT:1
-- row: INSERT:3
-- rowcount: 2
-- end-expected
SELECT m FROM zz_vf_wlog ORDER BY m;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_fta (shared int, a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ftb (shared int NOT NULL, b int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ftc () INHERITS (zz_vf_fta, zz_vf_ftb);
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "shared" of relation "zz_vf_ftc" violates not-null constraint
-- end-expected-error
INSERT INTO zz_vf_ftc (a) VALUES (1);
-- begin-expected
-- columns: is_nullable:varchar
-- row: NO
-- rowcount: 1
-- end-expected
SELECT is_nullable FROM information_schema.columns WHERE table_name='zz_vf_ftc' AND column_name='shared';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_t8 (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_l8 (m text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_f8() RETURNS trigger AS $$ BEGIN INSERT INTO zz_vf_l8 VALUES (TG_NAME); RETURN NULL; END; $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_tg8 AFTER INSERT ON zz_vf_t8 FOR EACH ROW EXECUTE FUNCTION zz_vf_f8();
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf_t8 ENABLE ALWAYS TRIGGER zz_vf_tg8;
-- begin-expected
-- ok: 0
-- end-expected
SET session_replication_role = 'replica';
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_t8 VALUES (1);
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_l8;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_t11 (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_f11() RETURNS trigger AS $$ BEGIN RETURN OLD; END; $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_tg11 BEFORE INSERT ON zz_vf_t11 FOR EACH ROW EXECUTE FUNCTION zz_vf_f11();
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_t11 VALUES (1);
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_t11;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_o1(id int, tag text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_o1f() RETURNS trigger AS $$
begin
  if OLD IS NULL then NEW.tag := 'oldnull'; else NEW.tag := 'oldnotnull'; end if;
  return NEW;
end $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf_o1t BEFORE INSERT ON zz_vf_o1 FOR EACH ROW EXECUTE FUNCTION zz_vf_o1f();
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_o1 VALUES (1, 'x');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_q9 (a int);
-- begin-expected
-- columns: r:int4
-- row: 1
-- rowcount: 1
-- end-expected
INSERT INTO zz_vf_q9 VALUES (1) RETURNING a AS r;
