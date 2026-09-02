-- source: investigation-2026-08.md
-- finding: 94
-- title: AT TIME ZONE and timezone() stringify their zone operand and hand it to java.time.ZoneId.of, which rejects POSIX abbreviations, numeric offsets and intervals, t
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT timezone(NULL, '2020-01-01 00:00:00'::timestamp) IS NULL;
-- begin-expected
-- ok: 0
-- end-expected
SET TimeZone = 'America/New_York';
-- begin-expected
-- columns: extract:numeric
-- row: 1636266600.000000
-- rowcount: 1
-- end-expected
SELECT extract(epoch from timestamp '2021-11-07 01:30:00' AT TIME ZONE 'America/New_York');
-- begin-expected
-- columns: timezone:text
-- row: 2001-02-17 06:38:40
-- rowcount: 1
-- end-expected
SELECT (timestamptz '2001-02-16 20:38:40-05' AT TIME ZONE interval '05:00')::text;
-- begin-expected
-- columns: timezone:text
-- row: 2020-01-01 12:00:00-05
-- rowcount: 1
-- end-expected
SELECT (timestamp '2020-01-01 12:00' AT TIME ZONE 'EST')::text;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT (timestamp '2020-01-01 12:00' AT TIME ZONE NULL) IS NULL;
