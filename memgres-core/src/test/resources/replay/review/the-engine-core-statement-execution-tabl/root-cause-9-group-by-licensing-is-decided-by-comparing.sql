-- source: review-2026-08.md
-- finding: Root cause 9: GROUP BY licensing is decided by comparing canonicalised expression *text*
-- area: The engine core: statement execution, tables, windows, joins and deparsing
-- title: Root cause 9: GROUP BY licensing is decided by comparing canonicalised expression *text*
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_g (id int primary key, s text, a int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf2_g VALUES (1,'x',1),(2,'y',3);
-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "zz_vf2_g.s" must appear in the GROUP BY clause or be used in an aggregate function
-- end-expected-error
SELECT s || 'A' AS r FROM zz_vf2_g GROUP BY s || 'a' ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "zz_vf2_g.s" must appear in the GROUP BY clause or be used in an aggregate function
-- end-expected-error
SELECT concat(s,'B') AS r FROM zz_vf2_g GROUP BY concat(s,'b') ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "zz_vf2_g.a" must appear in the GROUP BY clause or be used in an aggregate function
-- end-expected-error
SELECT CASE WHEN a > 0 THEN 'YES' ELSE 'no' END AS r FROM zz_vf2_g
  GROUP BY CASE WHEN a > 0 THEN 'yes' ELSE 'NO' END ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_q (id int primary key, a int, b int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf2_q VALUES (1,1,2),(2,3,4);
-- begin-expected
-- columns: a:int4 | b:int4
-- row: 1 | 2
-- row: 3 | 4
-- rowcount: 2
-- end-expected
SELECT a, b FROM zz_vf2_q GROUP BY (a, b) ORDER BY a;
-- both accept; correct
-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "zz_vf2_q.a" must appear in the GROUP BY clause or be used in an aggregate function
-- end-expected-error
SELECT a, b FROM zz_vf2_q GROUP BY ROW(a, b) ORDER BY a;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_gd (id int primary key, d double precision);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf2_gd VALUES (1,1.5),(2,2.5);
-- begin-expected
-- columns: d:float8
-- row: 1.5
-- row: 2.5
-- rowcount: 2
-- end-expected
SELECT d FROM zz_vf2_gd GROUP BY d::float ORDER BY 1;
-- begin-expected
-- columns: d:float8
-- row: 1.5
-- row: 2.5
-- rowcount: 2
-- end-expected
SELECT d FROM zz_vf2_gd GROUP BY d::float8 ORDER BY 1;
-- both accept, for contrast;
