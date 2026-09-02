-- source: investigation-2026-08.md
-- finding: 371
-- title: Join and window keys are built from Java toString(), so one value with two spellings becomes two keys. matchesUsingKeys ends in Objects.equals || toString().equ
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_l (n numeric, a text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_r (n numeric, b text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_l VALUES (1.0,'L');
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_r VALUES (1.00,'R');
-- begin-expected
-- columns: n:numeric | a:text | b:text
-- row: 1.0 | L | R
-- rowcount: 1
-- end-expected
SELECT n,a,b FROM zz_vf2_l JOIN zz_vf2_r USING (n);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_f1 (n numeric, a text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_f2 (n numeric, b text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_f1 VALUES (2.50,'L');
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_f2 VALUES (2.5,'R');
-- begin-expected
-- columns: n:numeric | a:text | b:text
-- row: 2.50 | L | R
-- rowcount: 1
-- end-expected
SELECT n,a,b FROM zz_vf2_f1 FULL JOIN zz_vf2_f2 USING (n) ORDER BY 1,2,3;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_hs (id int, n numeric);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_hb (id int, n numeric);
-- begin-expected
-- ok: 40
-- end-expected
INSERT INTO zz_vf2_hs SELECT g, 1.0 FROM generate_series(1,40) g;
-- begin-expected
-- ok: 40
-- end-expected
INSERT INTO zz_vf2_hb SELECT g, 1.00 FROM generate_series(1,40) g;
-- begin-expected
-- columns: count:int8
-- row: 1600
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf2_hs a JOIN zz_vf2_hb b ON a.n = b.n;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pn (id int, n numeric);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_pn VALUES (1,1.0),(2,1.00),(3,2.0);
-- begin-expected
-- columns: id:int4 | n:numeric | c:int8
-- row: 1 | 1.0 | 2
-- row: 2 | 1.00 | 2
-- row: 3 | 2.0 | 1
-- rowcount: 3
-- end-expected
SELECT id, n, count(*) OVER (PARTITION BY n) AS c FROM zz_vf2_pn ORDER BY id;
