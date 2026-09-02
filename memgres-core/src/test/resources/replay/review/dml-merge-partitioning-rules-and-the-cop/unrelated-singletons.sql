-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: DML, MERGE, partitioning, rules and the COPY/extended-protocol surface
-- title: Unrelated singletons
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
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_m1 (id int, s text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_m1 VALUES (1,'a');
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "oid" does not exist
-- end-expected-error
UPDATE zz_vf_m1 SET s = 'x' WHERE oid = 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_colmsg (id int);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "zz_vf_colmsg" does not exist
-- end-expected-error
ALTER TABLE zz_vf_colmsg ALTER COLUMN nosuchcol SET NOT NULL;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "ctid" of relation "zz_vf_colmsg" does not exist
-- end-expected-error
INSERT INTO zz_vf_colmsg (ctid) VALUES ('(0,1)');
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" named in key does not exist
-- end-expected-error
CREATE TABLE zz_vf_keymsg (a int, PRIMARY KEY (nosuchcol));
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ret (id int);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
INSERT INTO zz_vf_ret VALUES (1) RETURNING nosuchcol;
