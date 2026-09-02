-- source: review-2026-08.md
-- finding: Root cause 2: Describe treats any statement beginning with EXPLAIN as a safe SELECT and appends `LIMIT 0`
-- area: EXPLAIN
-- title: Root cause 2: Describe treats any statement beginning with EXPLAIN as a safe SELECT and appends `LIMIT 0`
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_d1 (id int, v int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_d2 (id int, v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_d2 VALUES (1,1),(2,2),(3,3);
-- JDBC: conn.prepareStatement(
--   "EXPLAIN (ANALYZE, COSTS OFF) INSERT INTO zz_d1 SELECT id, v FROM zz_d2 LIMIT 2"
-- ).getMetaData()   -- no execute() at all
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_d1;
-- JDBC: conn.prepareStatement("EXPLAIN (COSTS OFF) UPDATE zz_p SET v = 1").getMetaData();
