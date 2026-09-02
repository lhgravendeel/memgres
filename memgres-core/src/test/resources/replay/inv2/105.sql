-- source: investigation-2026-08.md
-- finding: 105
-- title: containsAggregate is a hand-written instanceof chain over expression node types and is missing several containers — BetweenExpr, IsBooleanExpr, ArrayExpr, RowEl
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_t (g int, v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_t VALUES (1,10),(1,20),(2,30);
-- begin-expected
-- columns: max:int4
-- row: 30
-- rowcount: 1
-- end-expected
SELECT max(v) FROM zz_vf_t HAVING max(v) BETWEEN 1 AND 100;
-- begin-expected
-- columns: g:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT g FROM zz_vf_t GROUP BY g HAVING max(v) BETWEEN 1 AND 100 ORDER BY g;
-- begin-expected
-- columns: g:int4 | max:int4
-- row: 1 | 20
-- row: 2 | 30
-- rowcount: 2
-- end-expected
SELECT g, max(v) FROM zz_vf_t GROUP BY g HAVING count(*) BETWEEN 1 AND 5 ORDER BY g;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_t2 (g int, v int, s text);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_t2 VALUES (1,10,'a'),(1,20,'b'),(2,30,'c');
-- begin-expected
-- columns: array:_int4
-- row: {30}
-- rowcount: 1
-- end-expected
SELECT ARRAY[max(v)] FROM zz_vf_t2;
-- begin-expected
-- columns: row:record
-- row: (30,10)
-- rowcount: 1
-- end-expected
SELECT ROW(max(v), min(v)) FROM zz_vf_t2;
-- begin-expected
-- columns: max:text
-- row: c
-- rowcount: 1
-- end-expected
SELECT max(s) COLLATE "C" FROM zz_vf_t2;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT max(v) BETWEEN 1 AND 100 FROM zz_vf_t2;
-- begin-expected
-- columns: g:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT g FROM zz_vf_t2 GROUP BY g HAVING (max(v) > 5) IS TRUE ORDER BY g;
