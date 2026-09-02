-- source: review-2026-08.md
-- finding: Generated columns and rule bodies are built by substituting values into SQL text
-- area: DML, MERGE, partitioning, rules and the COPY/extended-protocol surface
-- title: Generated columns and rule bodies are built by substituting values into SQL text
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
CREATE RULE zz_vf_r1 AS ON INSERT TO zz_vf_rt
  DO ALSO INSERT INTO zz_vf_rlog VALUES (NEW.identifier);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_rt (id, identifier) VALUES (7, 'hello');
