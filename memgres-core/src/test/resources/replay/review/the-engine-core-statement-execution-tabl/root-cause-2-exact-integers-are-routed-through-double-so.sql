-- source: review-2026-08.md
-- finding: Root cause 2: exact integers are routed through `double`, so bigint values past 2^53 collapse onto each other
-- area: The engine core: statement execution, tables, windows, joins and deparsing
-- title: Root cause 2: exact integers are routed through `double`, so bigint values past 2^53 collapse onto each other
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pt (id bigint) PARTITION BY RANGE (id);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pt1 PARTITION OF zz_vf2_pt FOR VALUES FROM (MINVALUE) TO (9007199254740993);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pt2 PARTITION OF zz_vf2_pt FOR VALUES FROM (9007199254740993) TO (MAXVALUE);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_pt VALUES (9007199254740992);
-- begin-expected
-- columns: c:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) AS c FROM zz_vf2_pt1;
-- begin-expected
-- columns: c:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) AS c FROM zz_vf2_pt2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_b (id int, v bigint);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_b VALUES (1,9007199254740992),(2,9007199254740993),(3,9007199254740995);
-- begin-expected
-- columns: id:int4 | v:int8 | c:int8
-- row: 1 | 9007199254740992 | 1
-- row: 2 | 9007199254740993 | 2
-- row: 3 | 9007199254740995 | 1
-- rowcount: 3
-- end-expected
SELECT id, v, count(*) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS c FROM zz_vf2_b ORDER BY id;
-- begin-expected
-- columns: id:int4 | v:int8 | s:numeric
-- row: 1 | 9007199254740992 | 18014398509481985
-- row: 2 | 9007199254740993 | 9007199254740993
-- row: 3 | 9007199254740995 | 9007199254740995
-- rowcount: 3
-- end-expected
SELECT id, v, sum(v) OVER (ORDER BY v RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS s FROM zz_vf2_b ORDER BY id;
