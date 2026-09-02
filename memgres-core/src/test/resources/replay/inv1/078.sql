-- source: investigation.md
-- finding: 78
-- title: `extract()` from `time` and `timetz` is completely broken ⚠️ (7 cases)
-- begin-expected
-- columns: extract:numeric
-- row: 36000.000000
-- rowcount: 1
-- end-expected
SELECT extract(epoch from time '10:00:00');
--   PG: 36000.000000 | mg: 22007 invalid input syntax for type timestamp: "10:00"
-- begin-expected
-- columns: extract:numeric
-- row: 10
-- rowcount: 1
-- end-expected
SELECT extract(hour from time '10:20:30');
-- PG: 10 | mg: 22007
-- begin-expected
-- columns: extract:numeric
-- row: 30.500000
-- rowcount: 1
-- end-expected
SELECT extract(second from time '10:20:30.5');
-- PG: 30.5 | mg: 22007
-- begin-expected
-- columns: extract:numeric
-- row: 30500000
-- rowcount: 1
-- end-expected
SELECT extract(microseconds from time '10:20:30.5');
-- PG: 30500000 | mg: 22007
-- begin-expected
-- columns: extract:numeric
-- row: 30500.000
-- rowcount: 1
-- end-expected
SELECT extract(milliseconds from time '10:20:30.5');
-- PG: 30500.000 | mg: 22007
-- begin-expected
-- columns: extract:numeric
-- row: 28800.000000
-- rowcount: 1
-- end-expected
SELECT extract(epoch from timetz '10:00:00+02');
-- PG: 28800 | mg: 22007
-- begin-expected
-- columns: extract:numeric
-- row: 2
-- rowcount: 1
-- end-expected
SELECT extract(timezone_hour from timetz '10:00:00+02');
-- PG: 2 | mg: 22007;
