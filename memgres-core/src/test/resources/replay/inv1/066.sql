-- source: investigation.md
-- finding: 66
-- title: `generate_series` silently truncates at 10 000 rows ⚠️ high
-- begin-expected
-- columns: count:int8
-- row: 20001
-- rowcount: 1
-- end-expected
SELECT count(*) FROM generate_series(
  timestamp '2020-01-01', timestamp '2020-01-01' + interval '20000 hours', interval '1 hour');
--   PG: 20001 | mg: 10000

-- begin-expected
-- columns: count:int8
-- row: 20001
-- rowcount: 1
-- end-expected
SELECT count(*) FROM generate_series(
  date '2020-01-01', date '2020-01-01' + 20000, interval '1 day');
--   PG: 20001 | mg: 10000;
