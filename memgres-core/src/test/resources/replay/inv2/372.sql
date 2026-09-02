-- source: investigation-2026-08.md
-- finding: 372
-- title: Exact integers are routed through double: comparePartitionBound reduces both operands with doubleValue(), and computeRangeBoundary computes the frame boundary i
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
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf2_pt1;
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
