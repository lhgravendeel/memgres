-- source: investigation.md
-- finding: 45
-- title: `date_bin` and interval units (7 cases, datetime)
-- unrunnable: the report wrote this reproducer abbreviated
SELECT date_bin(interval '-2 hours', …);
-- PG: 22008 stride must be greater than zero | mg: a value
SELECT date_bin(interval '1 mon 1 day', …);
--   PG: 0A000 timestamps cannot be binned into intervals containing months or years | mg: a value
SELECT extract(century from interval '2001 years');
-- PG: 20  | mg: 22023 unit not recognized
SELECT extract(millennium from interval '2001 years');
-- PG: 2   | mg: 22023
SELECT extract(decade from interval '2001 years');
-- PG: 200 | mg: 22023
SELECT extract(week from interval '13 days 24 hours');
-- PG: 1   | mg: 22023
SELECT interval 'P0001-02-03T04:05:06';
-- ISO 8601 form; PG: 1 year 2 mons… | mg: 22007
SELECT INTERVAL '1.234567 seconds' SECOND(3);
-- PG: 00:00:01.235 | mg: 42601
SELECT timestamptz '2001-02-16 20:38:40-05' AT LOCAL;
-- PG: works | mg: 42601 syntax error at "AT";;
