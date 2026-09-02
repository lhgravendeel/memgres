-- source: review-2026-08.md
-- finding: An aggregate is bound to the query it is written in, not to the level its arguments come from
-- area: Aggregates, window functions and grouping
-- title: An aggregate is bound to the query it is written in, not to the level its arguments come from
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_a (id int, g int, v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_a VALUES (1,1,10),(2,1,20),(3,2,30);
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT (SELECT count(a.v)) FROM zz_vf_a a;
-- begin-expected
-- columns: g:int4 | sum:int8
-- row: 1 | 30
-- row: 2 | 30
-- rowcount: 2
-- end-expected
SELECT a.g, (SELECT sum(a.v)) FROM zz_vf_a a GROUP BY a.g ORDER BY a.g;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf_a" already exists
-- end-expected-error
CREATE TABLE zz_vf_a (id int, g int, v int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_b (id int, v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_a VALUES (1,1,10),(2,1,20),(3,2,30);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_b VALUES (1,100),(2,200);
-- begin-expected-error
-- sqlstate: 21000
-- message-like: more than one row returned by a subquery used as an expression
-- end-expected-error
SELECT (SELECT max(a.id) FROM zz_vf_b) FROM zz_vf_a a ORDER BY 1;
-- begin-expected
-- columns: sum:int8
-- row: 120
-- rowcount: 1
-- end-expected
SELECT (SELECT sum(a.v) FROM zz_vf_b b WHERE b.id = 1) FROM zz_vf_a a ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf_a" already exists
-- end-expected-error
CREATE TABLE zz_vf_a (id int, g int, v int);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf_b" already exists
-- end-expected-error
CREATE TABLE zz_vf_b (id int, v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_a VALUES (1,1,10),(2,1,20),(3,2,30);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_b VALUES (1,100),(2,200);
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT (SELECT count(*) FROM zz_vf_a b WHERE b.v = max(a.v)) FROM zz_vf_a a;
-- begin-expected
-- columns: g:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT a.g FROM zz_vf_a a GROUP BY a.g
 HAVING (SELECT count(*) FROM zz_vf_b b WHERE b.id = min(a.id)) > 0 ORDER BY a.g;
