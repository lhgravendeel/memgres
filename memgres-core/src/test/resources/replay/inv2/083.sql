-- source: investigation-2026-08.md
-- finding: 83
-- title: The format-template engine reads fields without walking the template: to_number strips non-digits and parses whatever is left, DDD is added with plusDays instea
-- begin-expected
-- columns: to_number:numeric
-- row: 123
-- rowcount: 1
-- end-expected
SELECT to_number('12345', '999');
-- begin-expected
-- columns: to_number:numeric
-- row: 12
-- rowcount: 1
-- end-expected
SELECT to_number('12345', '99');
-- begin-expected
-- ok: 0
-- end-expected
SET TIME ZONE 'Europe/Amsterdam';
-- begin-expected
-- columns: to_char:text
-- row: CEST
-- rowcount: 1
-- end-expected
SELECT to_char(TIMESTAMPTZ '2020-06-15 12:00:00+00', 'TZ');
-- begin-expected
-- columns: to_char:text
-- row: CET
-- rowcount: 1
-- end-expected
SELECT to_char(TIMESTAMPTZ '2020-01-15 12:00:00+00', 'TZ');
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range: "2020 400"
-- end-expected-error
SELECT to_date('2020 400', 'YYYY DDD');
-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range: "2021 366"
-- end-expected-error
SELECT to_date('2021 366', 'YYYY DDD');
