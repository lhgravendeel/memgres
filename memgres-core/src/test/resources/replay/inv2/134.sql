-- source: investigation-2026-08.md
-- finding: 134
-- title: Generated-column expressions and rule action bodies are produced by regex substitution of runtime values into SQL text, so values are re-parsed as SQL, column n
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_g1 (a text, b text, g text GENERATED ALWAYS AS (a || b) STORED);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_g1 (a, b) VALUES ('b', 'x');
-- begin-expected
-- columns: a:text | b:text | g:text
-- row: b | x | bx
-- rowcount: 1
-- end-expected
SELECT a, b, g FROM zz_vf_g1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_g3 (a text, b text, g text GENERATED ALWAYS AS (a || '-b-') STORED);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_g3 (a, b) VALUES ('x', 'ZZ');
-- begin-expected
-- columns: a:text | b:text | g:text
-- row: x | ZZ | x-b-
-- rowcount: 1
-- end-expected
SELECT a, b, g FROM zz_vf_g3;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_g2 (a text, g text GENERATED ALWAYS AS (a || '!') STORED);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_g2 (a) VALUES ('back\slash');
-- begin-expected
-- columns: a:text | g:text
-- row: back\slash | back\slash!
-- rowcount: 1
-- end-expected
SELECT a, g FROM zz_vf_g2;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_g2 (a) VALUES ('p$1q');
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_g2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_rt (id int, identifier text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_rlog (what text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE RULE zz_vf_r1 AS ON INSERT TO zz_vf_rt DO ALSO INSERT INTO zz_vf_rlog VALUES (NEW.identifier);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_rt (id, identifier) VALUES (7, 'hello');
-- begin-expected
-- columns: id:int4 | identifier:text
-- row: 7 | hello
-- rowcount: 1
-- end-expected
SELECT id, identifier FROM zz_vf_rt;
