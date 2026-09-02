-- source: investigation.md
-- finding: 41
-- title: The `to_char` / `to_timestamp` template engine is largely non-functional ⚠️ high
-- unrunnable: the report wrote this reproducer abbreviated
SELECT to_timestamp(' 2000    JUN', 'YYYY MON');
-- PG: 2000-06-01 | mg: 22007 invalid input syntax
SELECT to_timestamp('2000 - JUN', 'YYYY-MON');
-- PG: 2000-06-01 | mg: 22007
SELECT to_timestamp('2000JUN', 'YYYY///MON');
-- PG: 2000-06-01 | mg: 22007
SELECT to_timestamp('2000/JUN', 'YYYY MON');
-- PG: 2000-06-01 | mg: 22007
SELECT to_timestamp('2000/JUN', 'FXYYYY MON');
-- PG: 2000-06-01 | mg: 22007
SELECT to_timestamp('12.3', 'SS.MS');
-- PG: …00:00:12.3  (300ms, not 3ms) | mg: 22007
SELECT to_timestamp('12.003', 'SS.MS');
-- PG: …00:00:12.003                 | mg: 22007
SELECT to_timestamp('15:12:02.020.001230', 'HH24:MI:SS.MS.US');
--   PG: 15:12:02.02123 | mg: 22007
SELECT to_date('95', 'YY');
-- PG: 1995-01-01 | mg: 22007
SELECT to_date('095', 'YYY');
-- PG: 1995-01-01 | mg: 22007
SELECT to_date('5', 'Y');
-- PG: 2005-01-01 | mg: 22007
SELECT to_date('2020XX06XX06', 'YYYY"XX"MM"XX"DD');
-- PG: 2020-06-06 | mg: 22007
SELECT to_date('2020ab06cd06', 'YYYY"XX"MM"XX"DD');
-- PG: 2020-06-06 | mg: 22007
SELECT to_timestamp('2000y6m1d', 'yyyytMMtDDt');
-- PG: 2000-06-01 | mg: 22007
SELECT to_timestamp('2000y6m1d', 'yyyy"y"MM"m"DD"d"');
-- PG: 2000-06-01 | mg: 22007
SELECT to_date('20000-1130', 'YYYY-MMDD');
-- PG: 20000-11-30 | mg: 22007
SELECT to_date('20000Nov30', 'YYYYMonDD');
--   PG: 20000-11-30 | mg: XX000 Internal error: Unknown pattern letter: o
SELECT to_date('20 2020 06 06', 'CC YYYY MM DD');
--   PG: 2020-06-06  | mg: XX000 Internal error: Unknown pattern letter: C
SELECT to_date('21 95 06 06', 'CC YY MM DD');
--   PG: 2095-06-06  | mg: XX000 Internal error: Unknown pattern letter: C
SELECT to_char(1.5, '99.9V99');
--   PG: 42601 cannot use "V" and decimal point together | mg: '  1.5000';;
