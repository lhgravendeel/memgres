-- source: review-2026-08.md
-- finding: Root cause 3: each IntervalStyle has its own hand-written writer and none of them round-trips
-- area: String, date/time, geometric, range and interval implementations
-- title: Root cause 3: each IntervalStyle has its own hand-written writer and none of them round-trips
-- begin-expected
-- ok: 0
-- end-expected
SET intervalstyle = 'iso_8601';
-- begin-expected
-- columns: interval:interval
-- row: PT-0.5S
-- rowcount: 1
-- end-expected
SELECT interval '-0.5 seconds';
-- begin-expected
-- columns: interval:interval
-- row: PT-1H-0.5S
-- rowcount: 1
-- end-expected
SELECT interval '-1 hour -0.5 seconds';
-- begin-expected
-- columns: interval:interval
-- row: P-1DT-0.25S
-- rowcount: 1
-- end-expected
SELECT interval '-1 day -0.25 seconds';
-- begin-expected
-- ok: 0
-- end-expected
SET intervalstyle = 'sql_standard';
-- begin-expected
-- columns: interval:interval
-- row: -0-1
-- rowcount: 1
-- end-expected
SELECT interval '-1 month';
