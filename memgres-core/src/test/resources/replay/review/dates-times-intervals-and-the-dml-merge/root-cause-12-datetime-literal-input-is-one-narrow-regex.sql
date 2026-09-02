-- source: review-2026-08.md
-- finding: Root cause 12: datetime literal input is one narrow regex
-- area: Dates, times, intervals — and the DML/MERGE/COPY findings filed with them
-- title: Root cause 12: datetime literal input is one narrow regex
-- begin-expected
-- columns: date:date
-- row: 1999-01-08
-- rowcount: 1
-- end-expected
SELECT date '08-Jan-99';
-- begin-expected
-- columns: date:date
-- row: 1999-01-08
-- rowcount: 1
-- end-expected
SELECT date '990108';
-- begin-expected
-- columns: date:date
-- row: 1999-01-08
-- rowcount: 1
-- end-expected
SELECT date '1999.008';
-- begin-expected
-- columns: date:date
-- row: 0099-01-08 BC
-- rowcount: 1
-- end-expected
SELECT date 'January 8, 99 BC';
-- begin-expected
-- columns: time:time
-- row: 16:05:00
-- rowcount: 1
-- end-expected
SELECT time '04:05 PM';
-- begin-expected
-- columns: time:time
-- row: 04:05:06
-- rowcount: 1
-- end-expected
SELECT time '040506';
-- begin-expected
-- columns: text:text
-- row: 04:05:06-08
-- rowcount: 1
-- end-expected
SELECT (timetz '04:05:06 PST')::text;
-- begin-expected
-- columns: timestamptz:timestamptz
-- row: 1999-01-08 12:05:06+00
-- rowcount: 1
-- end-expected
SELECT timestamptz '1999-01-08 04:05:06 -8:00';
-- begin-expected
-- columns: timestamp:timestamp
-- row: 1999-01-08 04:05:06
-- rowcount: 1
-- end-expected
SELECT timestamp 'January 8 04:05:06 1999 PST';
-- begin-expected
-- columns: timestamp:timestamp
-- row: 1999-01-08 04:05:06
-- rowcount: 1
-- end-expected
SELECT timestamp '19990108T040506';
