-- source: investigation-2026-08.md
-- finding: 379
-- title: GROUP BY licensing compares canonicalised expression text: canon lowercases every literal leaf, addResolved expands any isRow() ArrayExpr into its members (so R
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
SELECT s || 'A' FROM zz_vf2_g GROUP BY s || 'a' ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_q (id int primary key, a int, b int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf2_q VALUES (1,1,2),(2,3,4);
-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "zz_vf2_q.a" must appear in the GROUP BY clause or be used in an aggregate function
-- end-expected-error
SELECT a, b FROM zz_vf2_q GROUP BY ROW(a, b) ORDER BY a;
