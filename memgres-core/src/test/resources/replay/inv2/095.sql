-- source: investigation-2026-08.md
-- finding: 95
-- title: Interval arithmetic on a timestamptz adds calendar fields to the fixed-offset value instead of re-resolving the offset in the session zone, so a day is always e
-- begin-expected
-- ok: 0
-- end-expected
SET TimeZone = 'America/New_York';
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT timestamptz '2021-03-13 12:00:00-05' + interval '1 day' = timestamptz '2021-03-14 12:00:00-04';
-- begin-expected
-- ok: 0
-- end-expected
SET TimeZone = 'America/New_York';
-- begin-expected
-- columns: count:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM generate_series(timestamptz '2021-03-13 00:00-05', timestamptz '2021-03-15 00:00-04', interval '1 day', 'America/New_York') g;
-- begin-expected
-- columns: date_add:text
-- row: 2021-10-31 19:00:00-04
-- rowcount: 1
-- end-expected
SELECT date_add(timestamptz '2021-10-31 00:00:00+02', interval '1 day', 'Europe/Warsaw')::text;
