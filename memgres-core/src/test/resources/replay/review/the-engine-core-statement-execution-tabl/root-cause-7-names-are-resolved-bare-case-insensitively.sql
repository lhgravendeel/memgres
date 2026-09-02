-- source: review-2026-08.md
-- finding: Root cause 7: names are resolved bare, case-insensitively, or across every schema
-- area: The engine core: statement execution, tables, windows, joins and deparsing
-- title: Root cause 7: names are resolved bare, case-insensitively, or across every schema
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_uqa (a int, CONSTRAINT zz_vf2_lowuq UNIQUE (a));
-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "ZZ_VF2_LOWUQ" of relation "zz_vf2_uqa" does not exist
-- end-expected-error
ALTER TABLE zz_vf2_uqa DROP CONSTRAINT "ZZ_VF2_LOWUQ";
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zz_vf2_lowuq"
-- end-expected-error
INSERT INTO zz_vf2_uqa VALUES (1), (1);
-- begin-expected
-- columns: c:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*)::int AS c FROM zz_vf2_uqa;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf2_s3;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_s3.zz_vf2_tgt (a int PRIMARY KEY);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_s3.zz_vf2_tgt VALUES (1);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_tgt" does not exist
-- end-expected-error
CREATE TABLE zz_vf2_ch (b int REFERENCES zz_vf2_tgt(a));
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_ch" does not exist
-- end-expected-error
INSERT INTO zz_vf2_ch VALUES (9);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_log (m text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf2_s4;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_s4.zz_vf2_tt (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_tt (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf2_trgf() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN INSERT INTO zz_vf2_log VALUES ('fired'); RETURN NULL; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_vf2_trg AFTER TRUNCATE ON zz_vf2_s4.zz_vf2_tt FOR EACH STATEMENT EXECUTE FUNCTION zz_vf2_trgf();
-- begin-expected
-- ok: 0
-- end-expected
TRUNCATE zz_vf2_tt;
-- begin-expected
-- columns: c:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) AS c FROM zz_vf2_log;
