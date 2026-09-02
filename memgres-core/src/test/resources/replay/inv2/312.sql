-- source: investigation-2026-08.md
-- finding: 312
-- title: SET and SHOW have no value grammar: parseSet concatenates every token after = or TO up to the semicolon, so quoting, commas and keyword-ness are gone before any
-- begin-expected
-- ok: 0
-- end-expected
SET TIME ZONE 7;
-- begin-expected
-- columns: TimeZone:text
-- row: <+07>-07
-- rowcount: 1
-- end-expected
SHOW TimeZone;
-- begin-expected
-- ok: 0
-- end-expected
SET TIME ZONE -7;
-- begin-expected
-- columns: TimeZone:text
-- row: <-07>+07
-- rowcount: 1
-- end-expected
SHOW TimeZone;
-- begin-expected
-- ok: 0
-- end-expected
SET TIME ZONE INTERVAL '1' HOUR;
-- begin-expected
-- columns: TimeZone:text
-- row: <+01>-01
-- rowcount: 1
-- end-expected
SHOW TimeZone;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "TimeZone": "bogus/zone"
-- end-expected-error
SET TIME ZONE 'bogus/zone';
