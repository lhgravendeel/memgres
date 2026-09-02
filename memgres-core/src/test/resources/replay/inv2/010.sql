-- source: investigation-2026-08.md
-- finding: 10
-- title: The infinity sentinels for date/timestamp are ordinary representable values (9999-12-31 23:59:59 and 4714-01-01) rather than a distinct state, so infinity round
-- begin-expected
-- columns: text:text
-- row: infinity
-- rowcount: 1
-- end-expected
SELECT (timestamptz 'infinity')::text;
-- begin-expected
-- columns: isfinite:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT isfinite(timestamptz 'infinity');
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '9999-12-31 23:59:59'::timestamp = 'infinity'::timestamp;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '9999-12-31'::date = 'infinity'::date;
-- begin-expected
-- columns: ?column?:date
-- row: infinity
-- rowcount: 1
-- end-expected
SELECT date 'infinity' + 1;
