-- source: investigation-2026-08.md
-- finding: 93
-- title: timestamptz and timetz are written with Java's own toString rather than PostgreSQL's output routine, and the microsecond field is formatted with HH:mm:ss.SSSSSS
-- begin-expected
-- columns: text:text
-- row: 2001-02-16 20:38:41
-- rowcount: 1
-- end-expected
SELECT (timestamp '2001-02-16 20:38:40.9999999')::text;
-- begin-expected
-- ok: 0
-- end-expected
SET TimeZone = 'America/New_York';
-- begin-expected
-- columns: concat:text
-- row: x2001-02-16 15:38:40-05
-- rowcount: 1
-- end-expected
SELECT concat('x', timestamptz '2001-02-16 20:38:40+00');
-- begin-expected
-- columns: text:text
-- row: 04:05:06-08
-- rowcount: 1
-- end-expected
SELECT (timetz '04:05:06-8')::text;
-- begin-expected
-- columns: text:text
-- row: 10:20:30.5+02
-- rowcount: 1
-- end-expected
SELECT '10:20:30.5+02'::timetz::text;
