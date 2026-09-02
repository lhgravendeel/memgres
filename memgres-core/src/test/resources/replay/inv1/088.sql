-- source: investigation.md
-- finding: 88
-- title: `generate_series` over a wide integer range does not stream ⚠️
-- begin-expected
-- columns: count:int8
-- row: 5000000
-- rowcount: 1
-- end-expected
SELECT count(*) FROM generate_series(1, 5000000);
--   PG: 5000000, immediately | mg: no result in 30 seconds;
