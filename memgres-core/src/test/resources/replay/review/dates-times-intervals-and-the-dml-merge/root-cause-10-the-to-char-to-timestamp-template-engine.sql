-- source: review-2026-08.md
-- finding: Root cause 10: the to_char / to_timestamp template engine
-- area: Dates, times, intervals — and the DML/MERGE/COPY findings filed with them
-- title: Root cause 10: the to_char / to_timestamp template engine
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid format specification for an interval value
-- end-expected-error
SELECT to_char(interval '1 year 2 mons', 'YY Y MON Month');
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
-- columns: to_timestamp:text
-- row: 2020-06-06 06:30:00+00
-- rowcount: 1
-- end-expected
SELECT to_timestamp('2020-06-06 12:00:00 +05:30', 'YYYY-MM-DD HH24:MI:SS TZH:TZM')::text;
-- begin-expected
-- columns: to_date:text
-- row: 2006-10-19
-- rowcount: 1
-- end-expected
SELECT to_date('2006-42-4', 'IYYY-IW-ID')::text;
