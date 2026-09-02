-- source: review-2026-08.md
-- finding: Root cause 8: AT TIME ZONE hands a stringified operand to ZoneId.of
-- area: Dates, times, intervals — and the DML/MERGE/COPY findings filed with them
-- title: Root cause 8: AT TIME ZONE hands a stringified operand to ZoneId.of
-- begin-expected
-- columns: timezone:text
-- row: 2001-02-17 06:38:40
-- rowcount: 1
-- end-expected
SELECT (timestamptz '2001-02-16 20:38:40-05' AT TIME ZONE interval '05:00')::text;
-- begin-expected
-- columns: timezone:text
-- row: 2020-01-01 17:00:00+00
-- rowcount: 1
-- end-expected
SELECT (timestamp '2020-01-01 12:00' AT TIME ZONE 'EST')::text;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT (timestamp '2020-01-01 12:00' AT TIME ZONE NULL) IS NULL;
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
