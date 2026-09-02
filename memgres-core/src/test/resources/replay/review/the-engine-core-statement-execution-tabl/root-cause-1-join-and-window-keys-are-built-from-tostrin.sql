-- source: review-2026-08.md
-- finding: Root cause 1: join and window keys are built from `toString()`, so one value with two spellings becomes two keys
-- area: The engine core: statement execution, tables, windows, joins and deparsing
-- title: Root cause 1: join and window keys are built from `toString()`, so one value with two spellings becomes two keys
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
-- columns: n:numeric | a:text | b:text
-- row: 1.0 | L | R
-- rowcount: 1
-- end-expected
SELECT n,a,b FROM zz_vf2_l NATURAL JOIN zz_vf2_r;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_c1 (c char(3), a text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_c2 (c char(6), b text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_c1 VALUES ('a','L');
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_c2 VALUES ('a','R');
-- begin-expected
-- columns: c:bpchar | a:text | b:text
-- row: a   | L | R
-- rowcount: 1
-- end-expected
SELECT c,a,b FROM zz_vf2_c1 JOIN zz_vf2_c2 USING (c);
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
-- columns: n:numeric | a:text | b:text
-- row: 2.50 | L | R
-- rowcount: 1
-- end-expected
SELECT n,a,b FROM zz_vf2_f1 LEFT JOIN zz_vf2_f2 USING (n) ORDER BY 1,2,3;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_hs (id int, n numeric);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_hb (id int, n numeric);
-- begin-expected
-- ok: 4
-- end-expected
INSERT INTO zz_vf2_hs SELECT g, 1.0 FROM generate_series(1,4) g;
-- begin-expected
-- ok: 4
-- end-expected
INSERT INTO zz_vf2_hb SELECT g, 1.00 FROM generate_series(1,4) g;
-- begin-expected
-- columns: c:int8
-- row: 16
-- rowcount: 1
-- end-expected
SELECT count(*) AS c FROM zz_vf2_hs a JOIN zz_vf2_hb b ON a.n = b.n;
-- 16 pairs, nested loop
-- begin-expected
-- ok: 36
-- end-expected
INSERT INTO zz_vf2_hs SELECT g, 1.0 FROM generate_series(5,40) g;
-- begin-expected
-- ok: 36
-- end-expected
INSERT INTO zz_vf2_hb SELECT g, 1.00 FROM generate_series(5,40) g;
-- begin-expected
-- columns: c:int8
-- row: 1600
-- rowcount: 1
-- end-expected
SELECT count(*) AS c FROM zz_vf2_hs a JOIN zz_vf2_hb b ON a.n = b.n;
-- 1600 pairs, hash path
-- begin-expected
-- columns: total:int8 | matched:int8
-- row: 1600 | 1600
-- rowcount: 1
-- end-expected
SELECT count(*) AS total, count(b.id) AS matched FROM zz_vf2_hs a LEFT JOIN zz_vf2_hb b ON a.n = b.n;
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
-- begin-expected
-- columns: id:int4 | n:numeric | r:int8
-- row: 1 | 1.0 | 1
-- row: 2 | 1.00 | 2
-- row: 3 | 2.0 | 1
-- rowcount: 3
-- end-expected
SELECT id, n, row_number() OVER (PARTITION BY n ORDER BY id) AS r FROM zz_vf2_pn ORDER BY id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pk (id int, a text, b text);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf2_pk VALUES (1,'p','q'||chr(1)||'r'), (2,'p'||chr(1)||'q','r');
-- begin-expected
-- columns: id:int4 | c:int8
-- row: 1 | 1
-- row: 2 | 1
-- rowcount: 2
-- end-expected
SELECT id, count(*) OVER (PARTITION BY a, b) AS c FROM zz_vf2_pk ORDER BY id;
