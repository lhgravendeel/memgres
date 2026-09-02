-- source: investigation-2026-08.md
-- finding: 54
-- title: The item-method dispatcher applies .type() and .size() ahead of the strict/lax unwrap test and neither arm consults the strict flag.
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
