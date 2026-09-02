-- source: review-2026-08.md
-- finding: FETCH FIRST 0 ROWS WITH TIES has no zero guard
-- area: Aggregates, window functions and grouping
-- title: FETCH FIRST 0 ROWS WITH TIES has no zero guard
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_z (id int, v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_z VALUES (1,10),(2,20),(3,30);
-- begin-expected
-- columns: id:int4
-- rowcount: 0
-- end-expected
SELECT id FROM zz_vf_z ORDER BY id FETCH FIRST 0 ROWS WITH TIES;
-- begin-expected
-- columns: id:int4 | count:int8
-- rowcount: 0
-- end-expected
SELECT id, count(*) FROM zz_vf_z GROUP BY id ORDER BY id FETCH FIRST 0 ROWS WITH TIES;
-- begin-expected
-- columns: a:int4
-- rowcount: 0
-- end-expected
SELECT 1 AS a ORDER BY a FETCH FIRST 0 ROWS WITH TIES;
-- begin-expected
-- columns: id:int4 | r:int8
-- rowcount: 0
-- end-expected
SELECT id, row_number() OVER (ORDER BY id) AS r FROM zz_vf_z ORDER BY id FETCH FIRST 0 ROWS WITH TIES;
-- begin-expected
-- columns: s:int4
-- rowcount: 0
-- end-expected
SELECT generate_series(1, id) AS s FROM zz_vf_z ORDER BY s FETCH FIRST 0 ROWS WITH TIES;
