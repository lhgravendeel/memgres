-- source: investigation-2026-08.md
-- finding: 96
-- title: The to_char/to_timestamp/to_date template engine routes intervals and times through the timestamp formatter over a 1970 epoch base, and its input side has no zo
-- begin-expected
-- columns: to_timestamp:text
-- row: 2001-02-17 04:38:40.12+00
-- rowcount: 1
-- end-expected
SELECT to_timestamp(982384720.12)::text;
-- begin-expected
-- columns: to_char:text
-- row: 400
-- rowcount: 1
-- end-expected
SELECT to_char(interval '400 days', 'DDD');
-- begin-expected
-- columns: to_char:text
-- row: 0000-00-00
-- rowcount: 1
-- end-expected
SELECT to_char(time '20:38:40', 'YYYY-MM-DD');
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid format specification for an interval value
-- end-expected-error
SELECT to_char(time '20:38:40', 'DDD DAY DY D W WW Q J');
-- begin-expected
-- columns: to_char:text
-- row: +00 00 +0
-- rowcount: 1
-- end-expected
SELECT to_char(timestamptz '2001-02-16 20:38:40+00', 'FMTZH FMTZM FMOF');
-- begin-expected
-- columns: to_char:text
-- row: -1 -02:00:00
-- rowcount: 1
-- end-expected
SELECT to_char(interval '-1 day -2 hours', 'DD HH24:MI:SS');
-- begin-expected
-- columns: to_timestamp:text
-- row: 0001-01-01 00:00:12+00 BC
-- rowcount: 1
-- end-expected
SELECT to_timestamp('12', 'SS')::text;
