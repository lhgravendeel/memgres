-- source: investigation.md
-- finding: 86
-- title: Recursive CTEs silently truncate at 100 000 iterations ⚠️ high
-- begin-expected
-- columns: count:int8
-- row: 150000
-- rowcount: 1
-- end-expected
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 150000)
SELECT count(*) FROM r;
--   PG: 150000 | mg: 100001;
