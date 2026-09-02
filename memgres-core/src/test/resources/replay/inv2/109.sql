-- source: investigation-2026-08.md
-- finding: 109
-- title: A set-operation arm that carries WITH is rebuilt with the 10-argument SelectStmt convenience constructor, which passes null for distinctOn, windowDefs, grouping
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_s (g text, v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_s VALUES ('a',1),('a',2),('b',3);
-- begin-expected
-- columns: g:text | sum:int8
-- row: NULL | 6
-- row: a | 3
-- row: b | 3
-- row: z | 9
-- rowcount: 4
-- end-expected
WITH c AS (SELECT g, v FROM zz_vf_s)
SELECT g, sum(v) FROM c GROUP BY GROUPING SETS ((g), ())
UNION ALL SELECT 'z', 9;
-- begin-expected
-- columns: g:text | rank:int8
-- row: a | 1
-- row: a | 2
-- row: b | 3
-- row: z | 9
-- rowcount: 4
-- end-expected
WITH c AS (SELECT g, v FROM zz_vf_s)
SELECT g, rank() OVER w FROM c WINDOW w AS (ORDER BY v)
UNION ALL SELECT 'z', 9;
