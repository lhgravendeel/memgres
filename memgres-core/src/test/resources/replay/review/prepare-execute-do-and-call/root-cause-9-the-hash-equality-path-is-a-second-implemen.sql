-- source: review-2026-08.md
-- finding: Root cause 9: the hash/equality path is a second implementation that disagrees with the comparison path
-- area: PREPARE, EXECUTE, DO and CALL
-- title: Root cause 9: the hash/equality path is a second implementation that disagrees with the comparison path
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (SELECT 1 AS x UNION SELECT 1.0) t;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (SELECT 1::int AS x INTERSECT SELECT 1.0) t;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (SELECT 1::int AS x EXCEPT SELECT 1.0) t;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_iv (id int, iv interval);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_iv VALUES (1, interval '1 mon'), (2, interval '30 days'), (3, interval '720 hours');
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (SELECT iv FROM zz_iv GROUP BY iv) g;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(DISTINCT iv) FROM zz_iv;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT interval '1 mon' = interval '30 days';
