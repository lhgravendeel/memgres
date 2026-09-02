-- source: review-2026-08.md
-- finding: Root cause 13: .size() is applied before the strict test
-- area: The JSON implementation, second pass
-- title: Root cause 13: .size() is applied before the strict test
-- begin-expected-error
-- sqlstate: 22039
-- message-like: jsonpath item method .size() can only be applied to an array
-- end-expected-error
SELECT jsonb_path_query('{"a":1}', 'strict $.size()');
-- begin-expected-error
-- sqlstate: 22039
-- message-like: jsonpath item method .size() can only be applied to an array
-- end-expected-error
SELECT jsonb_path_query('1', 'strict $.size()');
