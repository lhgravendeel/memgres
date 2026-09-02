-- source: investigation-2026-08.md
-- finding: 341
-- title: Values taken from SQL text are used without a bounds or range check, so a raw Java exception escapes to the client as XX000: NULLIF/COALESCE are parsed as ordin
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
