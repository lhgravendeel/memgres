-- source: review-2026-08.md
-- finding: Root cause 4: a jsonb number's exponent drives the allocation instead of a range check
-- area: The JSON implementation, second pass
-- title: Root cause 4: a jsonb number's exponent drives the allocation instead of a range check
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value overflows numeric format
-- end-expected-error
SELECT length('1e200000'::jsonb::text);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value overflows numeric format
-- end-expected-error
SELECT length('1e1000000'::jsonb::text);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value overflows numeric format
-- end-expected-error
SELECT length('1e2000000'::jsonb::text);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value overflows numeric format
-- end-expected-error
SELECT length('1e-200000'::jsonb::text);
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value overflows numeric format
-- end-expected-error
SELECT jsonb_set('{"a":1}'::jsonb, '{a}', '1e2000000')::text = '1';
