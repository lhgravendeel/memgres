-- source: review-2026-08.md
-- finding: Raw Java exceptions escape as XX000
-- area: Parser and identifier resolution
-- title: Raw Java exceptions escape as XX000
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
SELECT NULLIF(1);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
SELECT NULLIF(1, 2, 3);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
SELECT COALESCE();
-- begin-expected-error
-- sqlstate: 42601
-- message-like: parameter number too large at or near "$99999999999999"
-- end-expected-error
SELECT $99999999999999;
